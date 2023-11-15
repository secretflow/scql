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

package intra

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/executor"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func (svc *grpcIntraSvc) DoQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" {
		return nil, errors.New("request for DoQuery is illegal")
	}

	app := svc.app
	jobID, err := application.GenerateJobID()
	if err != nil {
		return nil, fmt.Errorf("DoQuery: %v", err)
	}
	info := &application.ExecutionInfo{
		ProjectID: req.GetProjectId(),
		JobID:     jobID,
		Query:     req.GetQuery(),
		Issuer: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		EngineClient: app.EngineClient,
	}
	session, err := application.NewSession(ctx, info, app, false)
	if err != nil {
		return nil, err
	}
	app.AddSession(jobID, session)
	logrus.Infof("create session %s with query '%s' in project %s", jobID, req.Query, req.ProjectId)

	err = svc.runQuery(session)
	return session.GetResultSafely(), err
}

func (svc *grpcIntraSvc) SubmitQuery(ctx context.Context, req *pb.QueryRequest) (resp *pb.SubmitResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" {
		return nil, errors.New("request for SubmitQuery is illegal: empty project or query")
	}

	jobID, err := application.GenerateJobID()
	if err != nil {
		return nil, fmt.Errorf("SubmitQuery: %v", err)
	}
	app := svc.app
	info := &application.ExecutionInfo{
		ProjectID: req.GetProjectId(),
		JobID:     jobID,
		Query:     req.GetQuery(),
		Issuer: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		EngineClient: app.EngineClient,
	}
	session, err := application.NewSession(ctx, info, app, true /* async mode */)
	if err != nil {
		return nil, err
	}
	app.AddSession(jobID, session)
	logrus.Infof("create session %s with query '%s' in project %s", jobID, req.Query, req.ProjectId)

	err = svc.runQuery(session)
	if err != nil {
		return nil, fmt.Errorf("SubmitQuery: %v", err)
	}

	return &pb.SubmitResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("submit query job %v succeed", jobID),
		},
		JobId: jobID}, nil

}

func (svc *grpcIntraSvc) FetchResult(c context.Context, req *pb.FetchResultRequest) (resp *pb.QueryResponse, err error) {
	if req == nil || req.GetJobId() == "" {
		return nil, errors.New("request for FetchResult is illegal: empty job id")
	}
	session, ok := svc.app.GetSession(req.GetJobId())
	if !ok {
		return &pb.QueryResponse{
			Status: &pb.Status{
				Code:    int32(pb.Code_NOT_FOUND),
				Message: fmt.Sprintf("no existing session for job: %v", req.GetJobId()),
			},
		}, nil
	}
	result := session.GetResultSafely()
	if result == nil {
		return &pb.QueryResponse{
			Status: &pb.Status{
				Code:    int32(pb.Code_NOT_READY),
				Message: "result not ready, please retry later",
			},
		}, nil
	}
	return result, nil
}

type DistributeRet struct {
	party        string
	endpoint     string
	err          error
	prepareAgain bool
}

func DistributeQueryToOtherParty(session *application.Session, enginesInfo *translator.EnginesInfo, p string) (ret DistributeRet) {
	ret.party = p
	url, err := session.PartyMgr.GetBrokerUrlByParty(p)
	if err != nil {
		ret.err = err
		return
	}
	executionInfo := session.ExecuteInfo
	selfCode := session.PartyMgr.GetSelfInfo().Code
	selfEndpoint, err := session.GetEndpoint(selfCode)
	if err != nil {
		ret.err = err
		return
	}
	// distribute queries to other participants
	distributeReq := &pb.DistributeQueryRequest{
		ClientProtocol: application.Version,
		ProjectId:      executionInfo.ProjectID,
		JobId:          executionInfo.JobID,
		Query:          executionInfo.Query,
		ClientId:       &pb.PartyId{Code: selfCode},
		EngineEndpoint: selfEndpoint,
		IsAsync:        session.AsyncMode,
	}
	if slices.Contains(executionInfo.DataParties, selfCode) {
		selfInfoChecksum, err := session.GetSelfChecksum()
		if err != nil {
			ret.err = err
			return
		}
		distributeReq.ClientChecksum = &pb.Checksum{TableSchema: selfInfoChecksum.TableSchema, Ccl: selfInfoChecksum.CCL}
	}
	if slices.Contains(executionInfo.DataParties, p) {
		// get checksum of table/ccl of party p
		checksum, err := session.GetLocalChecksum(p)
		if err != nil {
			ret.err = err
			return
		}
		distributeReq.ServerChecksum = &pb.Checksum{TableSchema: checksum.TableSchema, Ccl: checksum.CCL}
	}

	response := &pb.DistributeQueryResponse{}
	// distribute query
	err = executionInfo.InterStub.DistributeQuery(url, distributeReq, response)
	if err != nil {
		ret.err = err
		return
	}
	if response == nil || response.Status == nil {
		ret.err = fmt.Errorf("failed to parse response: %+v", response)
		return
	}
	if response.ServerProtocol != application.Version {
		ret.err = fmt.Errorf("failed to check protocol: self is %s, party %s is %s", application.Version, p, response.ServerProtocol)
		return
	}
	// check error code to avoid panic
	if response.Status.Code != 0 && response.Status.Code != int32(pb.Code_CHECKSUM_CHECK_FAILED) {
		ret.err = fmt.Errorf("distribute query err: %+v", response.Status)
		return
	}
	ret.endpoint = response.GetEngineEndpoint()
	if response.Status.Code == 0 {
		return
	}
	if slices.Contains(executionInfo.DataParties, p) {
		err = session.SaveRemoteChecksum(p, response.ExpectedServerChecksum)
		if err != nil {
			ret.err = err
			return
		}
	}
	if response.Status.Code == int32(pb.Code_CHECKSUM_CHECK_FAILED) {
		ret.prepareAgain = true
		_, err = common.AskInfoByChecksumResult(session, response.ServerChecksumResult, enginesInfo.GetTablesByParty(p), p)
		if err != nil {
			logrus.Warningf("runsql err: %s", err)
			return
		}
	}
	return
}

func (svc *grpcIntraSvc) runQuery(session *application.Session) error {
	app := svc.app
	executionInfo := session.ExecuteInfo
	r := executor.NewQueryRunner(session)
	usedTables, err := core.GetSourceTables(session.ExecuteInfo.Query)
	if err != nil {
		return fmt.Errorf("runQuery: %v", err)
	}

	// prepare info, tableSchema, CCL...
	dataParties, workParties, err := r.Prepare(usedTables)
	if err != nil {
		return fmt.Errorf("runQuery Prepare: %v", err)
	}
	executionInfo.WorkParties = workParties
	executionInfo.DataParties = dataParties
	logrus.Infof("work parties: %+v; data parties: %+v", workParties, dataParties)
	err = executionInfo.CheckProjectConf()
	if err != nil {
		return fmt.Errorf("runQuery CheckProjectConf: %v", err)
	}

	// sync info and get endpoints from other party
	localChecksums, err := r.CreateChecksum()
	if err != nil {
		return fmt.Errorf("runQuery CreateChecksum: %v", err)
	}
	for code, checksum := range localChecksums {
		session.SaveLocalChecksum(code, checksum)
	}
	// if alice submit a two-party query which data comes from bob and carol, it must run by a three-party protocol
	retCh := make(chan DistributeRet, len(session.ExecuteInfo.WorkParties))
	distributePartyNum := 0
	for _, p := range session.ExecuteInfo.WorkParties {
		if p == app.Conf.PartyCode {
			continue
		}
		distributePartyNum++
		go func(p string) {
			logrus.Infof("distribute query to party %s", p)
			ret := DistributeQueryToOtherParty(session, r.GetEnginesInfo(), p)
			retCh <- ret
		}(p)
	}
	var totalErr error
	// wait for response from other parties
	for i := 0; i < distributePartyNum; i++ {
		ret := <-retCh
		if ret.prepareAgain {
			r.SetPrepareAgain()
		}
		if ret.err != nil {
			errStr := fmt.Sprintf("party %s: %s;", ret.party, ret.err)
			if totalErr == nil {
				totalErr = fmt.Errorf(errStr)
			} else {
				totalErr = fmt.Errorf(totalErr.Error() + errStr)
			}
			continue
		}
		session.SaveEndpoint(ret.party, ret.endpoint)
	}
	// err occurred when distributing query
	if totalErr != nil {
		return fmt.Errorf("runQuery distribute: %v", err)
	}
	err = r.Execute(usedTables)
	if err != nil {
		return fmt.Errorf("runQuery Execute err: %s", err)
	}
	return nil
}
