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
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/executor"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/brokerutil"
	"github.com/secretflow/scql/pkg/util/message"
)

// only issuer can distribute query to other party
func (svc *grpcInterSvc) DistributeQuery(ctx context.Context, req *pb.DistributeQueryRequest) (res *pb.DistributeQueryResponse, err error) {
	// 1. check request
	if req == nil || req.GetProjectId() == "" || req.GetQuery() == "" || (!req.GetDryRun() && req.GetEngineEndpoint() == "") {
		return nil, status.New(pb.Code_BAD_REQUEST, "DistributeQuery: illegal request: missing information")
	}
	if req.ClientProtocol != application.Version {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("DistributeQuery: expected client version %d equals to version %d", req.ClientProtocol, application.Version))
	}

	app := svc.app
	// 2. create session
	if _, err := app.GetSessionInfo(req.JobId); err == nil {
		errStr := fmt.Sprintf("DistributeQuery: already existing job for id %s, err: %v", req.JobId, err)
		logrus.Warning(errStr)
		return nil, fmt.Errorf(errStr)
	}
	err = common.CheckMemberExistInProject(app.MetaMgr, req.GetProjectId(), req.GetClientId().GetCode())
	if err != nil {
		return nil, err
	}

	var existingProject *storage.Project
	existingProject, err = app.MetaMgr.GetProject(req.GetProjectId())

	if err != nil {
		return nil, err
	}

	var projConf pb.ProjectConfig
	err = message.ProtoUnmarshal([]byte(existingProject.ProjectConf), &projConf)
	if err != nil {
		return nil, err
	}

	jobConf := req.GetJobConfig()

	jobConf = brokerutil.UpdateJobConfig(jobConf, &projConf)

	linkCfg := &pb.LinkConfig{
		LinkRecvTimeoutSec:          jobConf.LinkRecvTimeoutSec,
		LinkThrottleWindowSize:      jobConf.LinkThrottleWindowSize,
		LinkChunkedSendParallelSize: jobConf.LinkChunkedSendParallelSize,
		HttpMaxPayloadSize:          jobConf.HttpMaxPayloadSize,
	}

	psiCfg := &pb.PsiConfig{
		PsiCurveType:                              jobConf.PsiCurveType,
		UnbalancePsiRatioThreshold:                jobConf.UnbalancePsiRatioThreshold,
		UnbalancePsiLargerPartyRowsCountThreshold: jobConf.UnbalancePsiLargerPartyRowsCountThreshold,
	}

	info := &application.ExecutionInfo{
		ProjectID:    req.GetProjectId(),
		JobID:        req.GetJobId(),
		Query:        req.GetQuery(),
		Issuer:       req.GetClientId(),
		EngineClient: app.EngineClient,
		DebugOpts:    req.GetDebugOpts(),
		SessionOptions: &application.SessionOptions{
			SessionExpireSeconds: projConf.SessionExpireSeconds,
			LinkConfig:           linkCfg,
			PsiConfig:            psiCfg,
			TimeZone:             req.GetTimeZone(),
		},
	}

	session, err := application.NewSession(ctx, info, app, req.GetIsAsync(), req.GetDryRun())
	if err != nil {
		return
	}
	logrus.Infof("create session %s with query %s in project %s from %s", req.JobId, req.Query, req.ProjectId, req.GetClientId().GetCode())
	// use running options from issuer party
	session.ExecuteInfo.CompileOpts.Batched = req.GetRunningOpts().GetBatched()
	defer func(session *application.Session) {
		if err != nil {
			go session.OnError(err)
			// TODO: Clear session and DB meta
		}
	}(session)

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
	// persist session info for other party to exchange job info if necessary
	err = app.PersistSessionInfo(session)
	if err != nil {
		return
	}
	app.AddSession(req.JobId, session)
	defer func() {
		if err != nil {
			app.DeleteSession(req.JobId)
		}
	}()
	js := newJobSync(session, r.GetEnginesInfo())
	if session.DryRun {
		// if parties is more than two, get checksum from other parties
		err = js.getChecksumFromOtherParties(req.GetClientId().GetCode())
		if err != nil {
			return
		}
		if err = r.DryRun(usedTables); err != nil {
			return
		}
	} else {
		// run sql
		go func() {
			syncTriggered, err := js.syncWithAll()
			if err != nil {
				logrus.Errorf("runsql err: %s", err)
			}
			if syncTriggered {
				r.SetPrepareAgain()
			}
			defer func() {
				// clear session and DB meta if in sync mode or errors occurs
				if !req.GetIsAsync() || err != nil {
					if err := session.Engine.Stop(); err != nil {
						logrus.Warnf("failed to stop engine on query job %s: %v", req.GetJobId(), err)
					}
					app.DeleteSession(req.GetJobId())
					// TODO: clear SessionInfo in DB, ignore it temporarily to reduce DB write times
				}
			}()
			err = r.Execute(usedTables)
			if err != nil {
				logrus.Errorf("runsql err: %s", err)
				return
			}
			logrus.Info("runsql succeed")
		}()
	}

	var selfEndpoint string
	if !session.DryRun {
		selfEndpoint = session.Engine.GetEndpointForPeer()
	}

	response := &pb.DistributeQueryResponse{
		Status:               &pb.Status{Code: int32(pb.Code_OK)},
		ServerProtocol:       application.Version,
		ServerChecksumResult: pb.ChecksumCompareResult_EQUAL,
		EngineEndpoint:       selfEndpoint}
	if slices.Contains(executionInfo.DataParties, session.GetSelfPartyCode()) {
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

func (svc *grpcInterSvc) CancelQueryJob(c context.Context, req *pb.CancelQueryJobRequest) (resp *pb.CancelQueryJobResponse, err error) {
	if req == nil || req.GetJobId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "request for CancelQueryJob is illegal: empty job id")
	}

	info, err := svc.app.GetSessionInfo(req.GetJobId())
	if err != nil {
		err = status.New(pb.Code_NOT_FOUND, fmt.Sprintf("no existing session for job: %v, err: %s", req.GetJobId(), err))
		return
	}

	if info.Status == int8(storage.SessionCanceled) {
		return &pb.CancelQueryJobResponse{
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

	msg := fmt.Sprintf("cancel distributed query %s commanded by %s successfully", req.GetJobId(), req.GetClientId().GetCode())
	logrus.Info(msg)
	return &pb.CancelQueryJobResponse{
		Status: &pb.Status{
			Code:    int32(pb.Code_OK),
			Message: msg,
		},
	}, nil
}
