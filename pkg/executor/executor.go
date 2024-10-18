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

package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
)

type Executor struct {
	ExecutionPlans        map[string]*scql.RunExecutionPlanRequest
	OutputNames           []string
	EngineStub            *EngineStub
	SessionID             string
	PartyCodeToHost       map[string]string
	partyCodeToCredential map[string]string

	mu sync.Mutex
	// the following variables are protected by mutex
	// intermediateResults valid only run in async mode
	intermediateResults map[string]*scql.ReportRequest
}

type ResponseInfo struct {
	Response *scql.RunExecutionPlanResponse
	Err      error
}

func NewExecutor(plans map[string]*scql.RunExecutionPlanRequest, outputNames []string,
	engineStub *EngineStub, id string, partyInfo *graph.PartyInfo) (*Executor, error) {
	partyCodeToHost := make(map[string]string)
	partyCodeToCredential := make(map[string]string)
	for _, partyCode := range partyInfo.GetParties() {
		partyURL, err := partyInfo.GetUrlByParty(partyCode)
		if err != nil {
			return nil, err
		}
		partyCodeToHost[partyCode] = partyURL

		partyCredential, err := partyInfo.GetCredentialByParty(partyCode)
		if err != nil {
			return nil, err
		}
		partyCodeToCredential[partyCode] = partyCredential
	}

	return &Executor{
		ExecutionPlans:        plans,
		OutputNames:           outputNames,
		EngineStub:            engineStub,
		SessionID:             id,
		PartyCodeToHost:       partyCodeToHost,
		partyCodeToCredential: partyCodeToCredential,
		intermediateResults:   make(map[string]*scql.ReportRequest),
	}, nil
}

func (exec *Executor) RunExecutionPlan(ctx context.Context, engineAsync bool) (*scql.SCDBQueryResultResponse, error) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  exec.SessionID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "RunExecutionPlan"),
	}
	reason, result, err := exec.RunExecutionPlanCore(ctx, engineAsync)
	logEntry.CostTime = time.Since(timeStart)
	if err != nil {
		logEntry.Reason = reason
		logEntry.ErrorMsg = err.Error()
		logrus.Error(logEntry)
	} else {
		logrus.Info(logEntry)
	}

	return result, err
}

func newErrorSCQLQueryResult(code scql.Code, errMsg string) *scql.SCDBQueryResultResponse {
	return &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: errMsg,
		},
	}
}

func (exec *Executor) RunExecutionPlanCore(ctx context.Context, engineAsync bool) (string, *scql.SCDBQueryResultResponse, error) {
	var urls []string
	var bodies []string
	var partyCodes []string
	var partyCredentials []string
	var executionPlanReqs []*scql.RunExecutionPlanRequest
	for partyCode, pb := range exec.ExecutionPlans {
		url := exec.PartyCodeToHost[pb.GetJobParams().GetPartyCode()]
		urls = append(urls, url)

		pb.Async = engineAsync
		pb.CallbackUrl = exec.EngineStub.cbURL
		executionPlanReqs = append(executionPlanReqs, pb)
		m := protojson.MarshalOptions{UseProtoNames: true}
		body, err := m.Marshal(pb)
		if err != nil {
			return constant.ReasonInvalidRequest, nil, err
		}
		bodies = append(bodies, string(body))

		partyCodes = append(partyCodes, partyCode)

		partyCredentials = append(partyCredentials, exec.partyCodeToCredential[pb.GetJobParams().GetPartyCode()])
		audit.RecordPlanDetail(partyCode, url, pb)
		audit.RecordSessionParameters(pb.GetJobParams(), url, true)
	}

	c := make(chan ResponseInfo, len(urls))
	for i, url := range urls {
		go func(partyCode, url, credential, rawRequest string, executionPlanReq *scql.RunExecutionPlanRequest) {
			timeStart := time.Now()
			logEntry := &logutil.MonitorLogEntry{
				SessionID:  exec.SessionID,
				ActionName: fmt.Sprintf("%v@%v", "Executor", "RunExecutionPlan"),
				RawRequest: rawRequest,
			}
			response, err := exec.EngineStub.webClient.RunExecutionPlan(url, credential, executionPlanReq)
			logEntry.CostTime = time.Since(timeStart)
			if err != nil {
				logEntry.ErrorMsg = err.Error()
				logrus.Errorf("%v|PartyCode:%v|Url:%v", logEntry, partyCode, url)
			} else {
				logrus.Infof("%v|PartyCode:%v|Url:%v", logEntry, partyCode, url)
			}
			c <- ResponseInfo{
				Response: response,
				Err:      err,
			}
		}(partyCodes[i], url, partyCredentials[i], bodies[i], executionPlanReqs[i])
	}

	outCols := []*scql.Tensor{}
	responses := []*scql.RunExecutionPlanResponse{}
	var affectedRows int64 = 0
	var dimValue int64 = 0
	isFirstCol := true
	for i := 0; i < len(urls); i++ {
		responseInfo := <-c
		if responseInfo.Err != nil {
			return constant.ReasonCallEngineFail, nil, responseInfo.Err
		}
		response := responseInfo.Response
		if response.GetStatus().GetCode() != int32(scql.Code_OK) {
			return constant.ReasonInvalidResponse, nil, status.NewStatusFromProto(response.GetStatus())
		}

		if response.GetNumRowsAffected() != 0 {
			if affectedRows == 0 {
				affectedRows = response.GetNumRowsAffected()
			} else if affectedRows != response.GetNumRowsAffected() {
				errMsg := fmt.Errorf("affected rows not matched, received affectedRows=%v, req.NumRowsAffected=%v", affectedRows, response.GetNumRowsAffected())
				return constant.ReasonInvalidResponse, nil, status.Wrap(scql.Code_ENGINE_RUNSQL_ERROR, errMsg)
			}
		}

		for _, col := range response.GetOutColumns() {

			colShape := col.GetShape()
			if colShape == nil || len(colShape.GetDim()) == 0 {
				return constant.ReasonInvalidResponse, nil, status.Wrap(scql.Code_ENGINE_RUNSQL_ERROR, fmt.Errorf("unexpected nil TensorShape"))
			}
			if isFirstCol {
				dimValue = colShape.GetDim()[0].GetDimValue()
				isFirstCol = false
			} else if dimValue != colShape.GetDim()[0].GetDimValue() {
				errMsg := fmt.Errorf("dim shape not matched, peer shape value=%v, self shape value=%v", dimValue, colShape.GetDim()[0].GetDimValue())
				return constant.ReasonInvalidResponse, nil, status.Wrap(scql.Code_ENGINE_RUNSQL_ERROR, errMsg)
			}
			if _, err := find(exec.OutputNames, col.GetName()); err == nil {
				outCols = append(outCols, col)
			}
		}
		responses = append(responses, response)
	}

	res := &scql.SCDBQueryResultResponse{
		Status:        responses[0].GetStatus(),
		ScdbSessionId: responses[0].GetJobId(),
	}

	if !engineAsync {
		// sync mode can get the query result
		if err := CheckResultSchemas(outCols, exec.OutputNames); err != nil {
			return "", newErrorSCQLQueryResult(scql.Code_INTERNAL, err.Error()), err
		}

		res.OutColumns = outCols
		res.AffectedRows = affectedRows
	}

	return "", res, nil
}

// call it only in async mode
// return true if session finished (on error or succeed)
func (exec *Executor) HandleResultCallback(req *scql.ReportRequest) (finished bool) {

	exec.mu.Lock()
	exec.intermediateResults[req.GetPartyCode()] = req
	reportCnt := len(exec.intermediateResults)
	exec.mu.Unlock()

	// finished on error
	if req.GetStatus().GetCode() != 0 {
		return true
	}

	// finished on all report received
	if reportCnt == len(exec.ExecutionPlans) {
		return true
	}

	return false
}

// call it only in async mode
func (exec *Executor) MergeQueryResults() (*scql.SCDBQueryResultResponse, error) {
	var affectedRows int64 = 0
	var dimValue int64 = 0
	outCols := []*scql.Tensor{}
	isFirstCol := true
	for _, req := range exec.intermediateResults {
		if req.GetStatus().GetCode() != 0 {
			return nil, status.NewStatusFromProto(req.GetStatus())
		}
		if req.GetNumRowsAffected() != 0 {
			if affectedRows == 0 {
				affectedRows = req.GetNumRowsAffected()
			} else if affectedRows != req.GetNumRowsAffected() {
				return nil, fmt.Errorf("affected rows not matched, received affectedRows=%v, req.NumRowsAffected=%v", affectedRows, req.GetNumRowsAffected())
			}
		}
		for _, col := range req.GetOutColumns() {
			colShape := col.GetShape()
			if colShape == nil || len(colShape.GetDim()) == 0 {
				return nil, fmt.Errorf("unexpected nil TensorShape")
			}
			if isFirstCol {
				dimValue = colShape.GetDim()[0].GetDimValue()
				isFirstCol = false
			} else if dimValue != colShape.GetDim()[0].GetDimValue() {
				return nil, fmt.Errorf("dim shape not matched, peer shape value=%v, self shape value=%v", dimValue, colShape.GetDim()[0].GetDimValue())
			}
			if _, err := find(exec.OutputNames, col.GetName()); err == nil {
				outCols = append(outCols, col)
			}
		}
	}

	if err := CheckResultSchemas(outCols, exec.OutputNames); err != nil {
		return nil, err
	}

	return &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    0,
			Message: "ok",
		},
		OutColumns:    outCols,
		ScdbSessionId: exec.SessionID,
		AffectedRows:  affectedRows,
	}, nil
}

func CheckResultSchemas(outCols []*scql.Tensor, expectColNames []string) error {
	if len(outCols) != len(expectColNames) {
		return fmt.Errorf("the size of output column expected to be %d, but got %d", len(expectColNames), len(outCols))
	}

	for i := 0; i < len(expectColNames); i++ {
		if outCols[i].GetName() != expectColNames[i] {
			return fmt.Errorf("output column name not match, expect=%s, got=%s", expectColNames[i], outCols[i].GetName())
		}
	}
	return nil
}

func find(ss []string, s string) (int, error) {
	for i, e := range ss {
		if e == s {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unable to find name %v in %v", s, ss)
}
