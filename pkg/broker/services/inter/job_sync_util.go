// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inter

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/parallel"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type jobSync struct {
	issuerCode string
	session    *application.Session
	tableInfo  *graph.EnginesInfo
}

func newJobSync(session *application.Session, tableInfo *graph.EnginesInfo) *jobSync {
	issuerCode := session.ExecuteInfo.Issuer.GetCode()
	return &jobSync{issuerCode, session, tableInfo}
}

func (js *jobSync) syncWithAll() (syncTriggered bool, err error) {
	retCh := make(chan bool, 1)
	// update local storage from issuer
	go func() {
		askInfoTriggered := false
		askInfoTriggered, err = js.syncWithIssuer()
		retCh <- askInfoTriggered
	}()
	selfCode := js.session.GetSelfPartyCode()
	// update local storage from other parties
	otherParties := sliceutil.Subtraction(js.session.ExecuteInfo.DataParties, []string{js.issuerCode, selfCode})
	askInfoTriggers, syncWithPartiesErr := parallel.ParallelRun(otherParties, func(p string) (bool, error) {
		return js.syncWithOtherParty(p)
	})
	// waiting for all goroutine finish
	askInfoTriggers = append(askInfoTriggers, <-retCh)
	if err = errors.Join(syncWithPartiesErr, err); err != nil {
		return false, err
	}
	return slices.Contains(askInfoTriggers, true), nil
}

func (js *jobSync) syncWithIssuer() (askInfoTriggered bool, err error) {
	if !slices.Contains(js.session.ExecuteInfo.DataParties, js.issuerCode) {
		return false, nil
	}
	var reqChecksumCompareRes pb.ChecksumCompareResult
	reqChecksumCompareRes, err = js.session.ExecuteInfo.Checksums.CompareChecksumFor(js.issuerCode)
	if err != nil {
		logrus.Warningf("CompareChecksumFor: %s", err)
		return
	}
	askInfoTriggered, err = common.AskInfoByChecksumResult(js.session, reqChecksumCompareRes, js.tableInfo.GetTablesByParty(js.issuerCode), js.issuerCode)
	if err != nil {
		logrus.Warningf("AskInfoByChecksumResult: %s", err)
		return
	}
	return
}

func (js *jobSync) syncWithOtherParty(targetCode string) (askInfoTriggered bool, err error) {
	response, err := js.exchangeJobInfo(targetCode)
	if err != nil {
		return
	}
	session := js.session
	err = session.SaveRemoteChecksum(targetCode, response.ExpectedServerChecksum)
	if err != nil {
		return
	}
	if response.Status.Code == int32(pb.Code_DATA_INCONSISTENCY) {
		askInfoTriggered, err = common.AskInfoByChecksumResult(session, response.ServerChecksumResult, js.tableInfo.GetTablesByParty(targetCode), targetCode)
	}
	session.SaveEndpoint(targetCode, response.Endpoint)
	return
}

func (js *jobSync) exchangeJobInfo(targetParty string) (*pb.ExchangeJobInfoResponse, error) {
	session := js.session
	executionInfo := session.ExecuteInfo
	selfCode := session.GetSelfPartyCode()
	req := &pb.ExchangeJobInfoRequest{
		ProjectId: executionInfo.ProjectID,
		JobId:     executionInfo.JobID,
		ClientId:  &pb.PartyId{Code: selfCode},
	}
	if slices.Contains(executionInfo.DataParties, targetParty) {
		serverChecksum, err := session.GetLocalChecksum(targetParty)
		if err != nil {
			return nil, fmt.Errorf("ExchangeJobInfo: %s", err)
		}
		req.ServerChecksum = &pb.Checksum{
			TableSchema: serverChecksum.TableSchema,
			Ccl:         serverChecksum.CCL,
		}
		logrus.Infof("exchange job info with party %s with request %s", targetParty, req.String())
	}

	url, err := session.App.PartyMgr.GetBrokerUrlByParty(targetParty)
	if err != nil {
		return nil, fmt.Errorf("ExchangeJobInfoStub: %v", err)
	}
	response := &pb.ExchangeJobInfoResponse{}
	// retry to make sure that peer broker has created session
	for i := 0; i < session.App.Conf.ExchangeJobInfoRetryTimes; i++ {
		err = executionInfo.InterStub.ExchangeJobInfo(url, req, response)
		if err != nil {
			return nil, fmt.Errorf("ExchangeJobInfoStub: %v", err)
		}
		if response.GetStatus().GetCode() == int32(pb.Code_SESSION_NOT_FOUND) {
			if i < session.App.Conf.ExchangeJobInfoRetryTimes-1 {
				time.Sleep(session.App.Conf.ExchangeJobInfoRetryInterval)
			}
			continue
		}
		if response.GetStatus().GetCode() == 0 {
			return response, nil
		}
		break
	}
	if response.Status == nil {
		return nil, fmt.Errorf("err response from party %s; response %+v", targetParty, response)
	}
	if response.Status.Code == int32(pb.Code_DATA_INCONSISTENCY) {
		return response, nil
	}
	return nil, fmt.Errorf("failed to exchange job info with %s return error %+v", targetParty, response.Status)
}

// get checksum from parties except self and issuer
func (js *jobSync) getChecksumFromOtherParties(issuerParty string) error {
	session := js.session
	for _, p := range session.ExecuteInfo.DataParties {
		if p == issuerParty || p == session.GetSelfPartyCode() {
			continue
		}
		response, err := js.exchangeJobInfo(p)
		if err != nil {
			return err
		}
		err = session.SaveRemoteChecksum(p, response.ExpectedServerChecksum)
		if err != nil {
			return err
		}
	}
	return nil
}
