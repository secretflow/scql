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
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/executor"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
)

// only issuer can distribute query to other party
func (svc *grpcInterSvc) DistributeQuery(ctx context.Context, req *pb.DistributeQueryRequest) (res *pb.DistributeQueryResponse, err error) {
	// 1. check request
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" || req.GetEngineEndpoint() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "DistributeQuery: illegal request: missing information")
	}
	if req.ClientProtocol != application.Version {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("DistributeQuery: expected client version %d equals to version %d", req.ClientProtocol, application.Version))
	}

	defer func() {
		if err != nil {
			err = status.New(pb.Code_INTERNAL, err.Error())
		}
	}()
	app := svc.app
	// 2. create session
	session, exist := app.GetSession(req.JobId)
	if exist {
		errStr := fmt.Sprintf("DistributeQuery: duplicated job id %s", req.JobId)
		logrus.Warning(errStr)
		return nil, fmt.Errorf(errStr)
	}
	err = common.CheckMemberExistInProject(app.MetaMgr, req.GetProjectId(), req.GetClientId().GetCode())
	if err != nil {
		return nil, err
	}
	info := &application.ExecutionInfo{
		ProjectID:    req.GetProjectId(),
		JobID:        req.GetJobId(),
		Query:        req.GetQuery(),
		Issuer:       req.GetClientId(),
		EngineClient: app.EngineClient,
	}

	session, err = application.NewSession(ctx, info, app, req.GetIsAsync())
	if err != nil {
		return
	}
	app.AddSession(req.JobId, session)
	logrus.Infof("create session %s with query %s in project %s from %s", req.JobId, req.Query, req.ProjectId, req.GetClientId().GetCode())
	// 3.1 parse query
	r := executor.NewQueryRunner(session)
	executionInfo := session.ExecuteInfo
	// get source tables to get participants and info schema
	usedTables, err := core.GetSourceTables(executionInfo.Query)
	if err != nil {
		return
	}
	logrus.Infof("get source tables %+v in project %s from storage", usedTables, req.GetProjectId())
	dataParties, workParties, err := r.Prepare(usedTables)
	if err != nil {
		return
	}
	executionInfo.WorkParties = workParties
	executionInfo.DataParties = dataParties
	logrus.Infof("work parties: %+v; data parties: %+v", workParties, dataParties)
	err = executionInfo.CheckProjectConf()
	if err != nil {
		return
	}
	//  update session info
	if slices.Contains(dataParties, req.GetClientId().GetCode()) {
		err = session.SaveRemoteChecksum(req.GetClientId().GetCode(), req.ClientChecksum)
		if err != nil {
			return
		}
	}
	session.SaveEndpoint(req.GetClientId().GetCode(), req.EngineEndpoint)

	// 3.2 sync info and get endpoints from other party
	localChecksums, err := r.CreateChecksum()
	if err != nil {
		return
	}
	for code, checksum := range localChecksums {
		session.SaveLocalChecksum(code, checksum)
	}
	// ask info from issuer
	chLen := 1
	// ask info from other parties
	selfCode := session.GetSelfPartyCode()
	for _, p := range executionInfo.DataParties {
		if p != req.GetClientId().GetCode() && p != selfCode {
			chLen += 1
		}
	}
	type retInfo struct {
		err          error
		prepareAgain bool
	}
	retCh := make(chan retInfo, chLen)
	// update local storage from issuer
	go func() {
		var err error
		prepareAgain := false
		defer func() {
			retCh <- retInfo{err, prepareAgain}
		}()
		if slices.Contains(dataParties, req.GetClientId().GetCode()) {
			var reqChecksumCompareRes pb.ChecksumCompareResult
			reqChecksumCompareRes, err = executionInfo.Checksums.CompareChecksumFor(req.GetClientId().GetCode())
			if err != nil {
				logrus.Warningf("CompareChecksumFor: %s", err)
				return
			}
			prepareAgain, err = common.AskInfoByChecksumResult(session, reqChecksumCompareRes, r.GetEnginesInfo().GetTablesByParty(req.GetClientId().GetCode()), req.GetClientId().GetCode())
			if err != nil {
				logrus.Warningf("AskInfoByChecksumResult: %s", err)
				return
			}
			return
		}
	}()
	// update local storage from other parties
	for _, p := range executionInfo.DataParties {
		if p == req.GetClientId().GetCode() || p == selfCode {
			continue
		}
		go func(p string) {
			prepareAgain, err := checkInfoFromOtherParty(session, r, p)
			retCh <- retInfo{err: err, prepareAgain: prepareAgain}
		}(p)
	}
	// run sql
	go func() {
		// check error
		var err error
		for i := 0; i < chLen; i++ {
			ret := <-retCh
			if ret.err != nil {
				err = errors.Join(err, ret.err)
				continue
			}
			err = errors.Join(err, ret.err)
			if ret.prepareAgain {
				r.SetPrepareAgain()
			}
		}
		if err != nil {
			logrus.Errorf("runsql err: %s", err)
			return
		}
		err = r.Execute(usedTables)
		if err != nil {
			logrus.Errorf("runsql err: %s", err)
			return
		}
		logrus.Info("runsql succeed")
	}()
	selfEndpoint, err := session.GetEndpoint(selfCode)
	if err != nil {
		return nil, err
	}
	response := &pb.DistributeQueryResponse{
		Status:               &pb.Status{Code: int32(pb.Code_OK)},
		ServerProtocol:       application.Version,
		ServerChecksumResult: pb.ChecksumCompareResult_EQUAL,
		EngineEndpoint:       selfEndpoint}
	if slices.Contains(executionInfo.DataParties, selfCode) {
		checksum, err := session.GetSelfChecksum()
		if err != nil {
			return nil, err
		}
		equalResult := checksum.CompareWith(application.NewChecksumFromProto(req.ServerChecksum))
		if equalResult != pb.ChecksumCompareResult_EQUAL {
			response.Status.Code = int32(pb.Code_DATA_INCONSISTENCY)
		}
		response.ServerChecksumResult = equalResult
		response.ExpectedServerChecksum = &pb.Checksum{TableSchema: checksum.TableSchema, Ccl: checksum.CCL}
	}

	return response, nil
}

func checkInfoFromOtherParty(session *application.Session, r *executor.QueryRunner, targetCode string) (prepareAgain bool, err error) {
	response, err := r.ExchangeJobInfo(targetCode)
	if err != nil {
		return
	}
	if response.Status.Code == int32(pb.Code_DATA_INCONSISTENCY) {
		err = session.SaveRemoteChecksum(targetCode, response.ExpectedServerChecksum)
		if err != nil {
			return
		}
		prepareAgain, err = common.AskInfoByChecksumResult(session, response.ServerChecksumResult, r.GetEnginesInfo().GetTablesByParty(targetCode), targetCode)
	}
	session.SaveEndpoint(targetCode, response.Endpoint)
	return
}

func (svc *grpcInterSvc) CancelQuery(context.Context, *pb.CancelQueryRequest) (*pb.CancelQueryResponse, error) {
	return nil, errors.New("method CancelQuery not implemented")
}
