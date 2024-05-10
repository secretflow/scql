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
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/executor"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
)

func (svc *grpcIntraSvc) DoQuery(ctx context.Context, req *pb.QueryRequest) (resp *pb.QueryResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "request for DoQuery is illegal")
	}

	app := svc.app
	err = common.CheckMemberExistInProject(app.MetaMgr, req.GetProjectId(), app.Conf.PartyCode)
	if err != nil {
		return nil, err
	}
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
		DebugOpts:    req.GetDebugOpts(),
	}
	session, err := application.NewSession(ctx, info, app, false, req.GetDryRun())
	if err != nil {
		return nil, err
	}
	app.AddSession(jobID, session)
	logrus.Infof("create session %s with query '%s' in project %s", jobID, req.Query, req.ProjectId)

	defer func() {
		if session.Engine != nil {
			if err := session.Engine.Stop(); err != nil {
				logrus.Warnf("failed to stop engine for query job=%s: %v", jobID, err)
			}
		}
		app.DeleteSession(jobID)
	}()

	err = svc.runQuery(session)
	return session.GetResultSafely(), err
}

func (svc *grpcIntraSvc) SubmitQuery(ctx context.Context, req *pb.QueryRequest) (resp *pb.SubmitResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "request for SubmitQuery is illegal: empty project or query")
	}

	if req.GetDryRun() {
		return nil, status.New(pb.Code_BAD_REQUEST, "Bad Request: dry_run is not supported in SubmitQuery, please use DoQuery instead")
	}

	app := svc.app
	err = common.CheckMemberExistInProject(app.MetaMgr, req.GetProjectId(), app.Conf.PartyCode)
	if err != nil {
		return nil, err
	}
	jobID, err := application.GenerateJobID()
	if err != nil {
		return nil, fmt.Errorf("SubmitQuery: %v", err)
	}
	info := &application.ExecutionInfo{
		ProjectID: req.GetProjectId(),
		JobID:     jobID,
		Query:     req.GetQuery(),
		Issuer: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		EngineClient: app.EngineClient,
		DebugOpts:    req.GetDebugOpts(),
	}
	session, err := application.NewSession(ctx, info, app, true /* async mode */, false)
	if err != nil {
		return nil, err
	}
	app.AddSession(jobID, session)
	logrus.Infof("create session %s with query '%s' in project %s", jobID, req.Query, req.ProjectId)

	defer func(session *application.Session) {
		if err != nil {
			go session.OnError(err)
			app.DeleteSession(jobID)
			// NOTE: no need to clear session info in DB, since the PersistSession is called in the end of runQuery
		}
	}(session)

	err = svc.runQuery(session)
	if err != nil {
		return nil, fmt.Errorf("SubmitQuery: %w", err)
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
		return nil, status.New(pb.Code_BAD_REQUEST, "request for FetchResult is illegal: empty job id")
	}

	info, err := svc.app.GetSessionInfo(req.GetJobId())
	if err != nil {
		err = status.New(pb.Code_NOT_FOUND, fmt.Sprintf("no existing session for job: %v, err: %s", req.GetJobId(), err))
		return
	}
	// Currently when persist_session is enabled, canceled or expired session_infos still exists in the database, while session_results are deleted
	if info.Status == int8(storage.SessionCanceled) {
		return nil, fmt.Errorf("session %s was canceled, no result existing now", req.GetJobId())
	}
	if time.Now().After(info.CreatedAt.Add(svc.app.Conf.SessionExpireTime)) {
		return nil, fmt.Errorf("session %s was expired, no result existing now", req.GetJobId())
	}

	resp, err = svc.app.GetSessionResult(info.SessionID)
	if err != nil {
		return nil, fmt.Errorf("FetchResult: GetSessionResult failed: %v", err)
	}

	if resp == nil {
		err = status.New(pb.Code_NOT_READY, "result not ready, please retry later")
	}
	return
}

func (svc *grpcIntraSvc) CancelQuery(c context.Context, req *pb.CancelQueryRequest) (resp *pb.CancelQueryResponse, err error) {
	if req == nil || req.GetJobId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "request for CancelQeury is illegal: empty job id")
	}

	info, err := svc.app.GetSessionInfo(req.GetJobId())
	if err != nil {
		err = status.New(pb.Code_NOT_FOUND, fmt.Sprintf("no existing session for job: %v, err: %s", req.GetJobId(), err))
		return
	}

	if info.Status == int8(storage.SessionCanceled) {
		return &pb.CancelQueryResponse{
			Status: &pb.Status{
				Code:    int32(0),
				Message: fmt.Sprintf("job %v already canceled", req.GetJobId()),
			},
		}, nil
	}

	err = svc.app.CancelSession(info)
	if err != nil {
		return nil, fmt.Errorf("CancelQuery: cancel session %s failed: %v", info.SessionID, err)
	}

	// async notify other members to release resources
	for _, party := range strings.Split(info.WorkParties, ";") {
		if party == svc.app.Conf.PartyCode {
			continue
		}
		go cancelDistributedQuery(svc.app, party, req.GetJobId())
	}

	logrus.Infof("cancel query job{%s} manually.", req.GetJobId())
	return &pb.CancelQueryResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("cancel query job %v succeed", req.GetJobId()),
		},
	}, nil
}

type DistributeRet struct {
	party        string
	endpoint     string
	err          error
	prepareAgain bool
}

func (dr *DistributeRet) String() string {
	return fmt.Sprintf("{party:%s, endpoint:%s, err:%v, prepareAgain:%v}", dr.party, dr.endpoint, dr.err, dr.prepareAgain)
}

func distributeQueryToOtherParty(session *application.Session, enginesInfo *translator.EnginesInfo, p string) (ret DistributeRet) {
	ret.party = p
	url, err := session.App.PartyMgr.GetBrokerUrlByParty(p)
	if err != nil {
		ret.err = err
		return
	}
	executionInfo := session.ExecuteInfo
	selfCode := session.GetSelfPartyCode()
	var selfEndpoint string
	if !session.DryRun {
		selfEndpoint = session.Engine.GetEndpointForPeer()
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
		DebugOpts:      session.ExecuteInfo.DebugOpts,
		DryRun:         session.DryRun,
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
	// check error code to avoid panic
	if response.GetStatus().GetCode() != 0 && response.GetStatus().GetCode() != int32(pb.Code_DATA_INCONSISTENCY) {
		ret.err = fmt.Errorf("distribute query err: %+v", response.Status)
		return
	}
	// check version when there is no err
	if response.ServerProtocol != application.Version {
		ret.err = fmt.Errorf("failed to check protocol: self is %s, party %s is %s", application.Version, p, response.ServerProtocol)
		return
	}
	ret.endpoint = response.GetEngineEndpoint()
	if slices.Contains(executionInfo.DataParties, p) {
		err = session.SaveRemoteChecksum(p, response.ExpectedServerChecksum)
		if err != nil {
			ret.err = err
			return
		}
	}
	if response.Status.Code == int32(pb.Code_DATA_INCONSISTENCY) {
		logrus.Infof("checksum not equal with party %s for job %s", p, executionInfo.JobID)
		ret.prepareAgain = true
		_, err = common.AskInfoByChecksumResult(session, response.ServerChecksumResult, enginesInfo.GetTablesByParty(p), p)
		if err != nil {
			ret.err = err
			logrus.Warningf("err when running AskInfoByChecksumResult: %s", err)
			return
		}
	}
	return
}

func (svc *grpcIntraSvc) runQuery(session *application.Session) error {
	app := svc.app
	executionInfo := session.ExecuteInfo
	r := executor.NewQueryRunner(session)
	logrus.Infof("create query runner for job %s", session.ExecuteInfo.JobID)
	usedTables, err := core.GetSourceTables(session.ExecuteInfo.Query)
	if err != nil {
		return fmt.Errorf("runQuery: %v", err)
	}
	logrus.Infof("get source tables %+v in project %s from storage", usedTables, executionInfo.ProjectID)
	// prepare info, tableSchema, CCL...
	dataParties, workParties, err := r.Prepare(usedTables)
	if err != nil {
		return fmt.Errorf("runQuery Prepare: %v", err)
	}
	executionInfo.WorkParties = workParties
	executionInfo.DataParties = dataParties
	logrus.Infof("work parties: %+v; data parties: %+v for job %s", workParties, dataParties, session.ExecuteInfo.JobID)
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
			logrus.Infof("distribute query to party %s for job %s", p, session.ExecuteInfo.JobID)
			ret := distributeQueryToOtherParty(session, r.GetEnginesInfo(), p)
			retCh <- ret
		}(p)
	}
	var totalErr error
	// wait for response from other parties
	for i := 0; i < distributePartyNum; i++ {
		ret := <-retCh
		logrus.Infof("distribute query return: %s, for job %s", ret.String(), session.ExecuteInfo.JobID)
		if ret.err != nil {
			totalErr = errors.Join(totalErr, ret.err)
			continue
		}
		if ret.prepareAgain {
			r.SetPrepareAgain()
		}
		session.SaveEndpoint(ret.party, ret.endpoint)
	}
	logrus.Infof("distribute query completed for job %s", session.ExecuteInfo.JobID)
	// err occurred when distributing query
	if totalErr != nil {
		return fmt.Errorf("runQuery distribute: %v", totalErr)
	}

	if session.DryRun {
		// NOTE: dry run doesn't need to persistent session info
		if err := r.DryRun(usedTables); err != nil {
			return err
		}

		result := &pb.QueryResponse{
			Status: &pb.Status{
				Code:    int32(pb.Code_OK),
				Message: "dry run success",
			},
		}

		session.SetResultSafely(result)
		return nil
	}

	err = r.Execute(usedTables)
	if err != nil {
		return fmt.Errorf("runQuery Execute err: %w", err)
	}

	return nil
}

// Try to notify party to cancel jobId
func cancelDistributedQuery(app *application.App, party, jobId string) {
	destUrl, err := app.PartyMgr.GetBrokerUrlByParty(party)
	if err != nil {
		logrus.Warnf("cancelDistributedQuery: get url for party %s failed: %v", party, err)
		return
	}
	req := pb.CancelQueryJobRequest{
		ClientId: &pb.PartyId{Code: app.Conf.PartyCode},
		JobId:    jobId,
	}
	resp := pb.CancelQueryJobResponse{}
	err = app.InterStub.CancelQueryJob(destUrl, &req, &resp)
	if err != nil {
		logrus.Warnf("cancelDistributedQuery: inter request failed: %v", err)
		return
	}
	if resp.GetStatus().GetCode() != int32(pb.Code_OK) {
		logrus.Warnf("cancelDistributedQuery: response error: %v", protojson.Format(&resp))
	}
}
