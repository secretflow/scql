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
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

type SyncExecutor struct {
	ExecutionPlans        map[string]*scql.RunExecutionPlanRequest
	OutputNames           []string
	EngineStub            *EngineStub
	SessionID             string
	PartyCodeToHost       map[string]string
	partyCodeToCredential map[string]string
}

type ResponseInfo struct {
	ResponseBody string
	Err          error
}

func NewSyncExecutor(plans map[string]*scql.RunExecutionPlanRequest, outputNames []string,
	engineStub *EngineStub, id string, partyInfo *translator.PartyInfo) (*SyncExecutor, error) {
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

	return &SyncExecutor{
		ExecutionPlans:        plans,
		OutputNames:           outputNames,
		EngineStub:            engineStub,
		SessionID:             id,
		PartyCodeToHost:       partyCodeToHost,
		partyCodeToCredential: partyCodeToCredential,
	}, nil
}

func (executor *SyncExecutor) RunExecutionPlan(ctx context.Context) (*scql.SCDBQueryResultResponse, error) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  executor.SessionID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "RunExecutionPlan"),
	}
	reason, result, err := executor.RunExecutionPlanCore(ctx)
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

func (executor *SyncExecutor) RunExecutionPlanCore(ctx context.Context) (string, *scql.SCDBQueryResultResponse, error) {
	var urls []string
	var bodies []string
	var partyCodes []string
	var partyCredentials []string
	for partyCode, pb := range executor.ExecutionPlans {
		partyCodes = append(partyCodes, partyCode)
		m := protojson.MarshalOptions{UseProtoNames: true}
		body, err := m.Marshal(pb)
		if err != nil {
			return constant.ReasonInvalidRequest, nil, err
		}
		bodies = append(bodies, string(body))

		url := url.URL{
			Scheme: executor.EngineStub.protocol,
			Host:   executor.PartyCodeToHost[pb.GetSessionParams().GetPartyCode()],
			Path:   runExecutionPlanPath,
		}
		urls = append(urls, url.String())
		partyCredentials = append(partyCredentials, executor.partyCodeToCredential[pb.GetSessionParams().GetPartyCode()])
	}

	c := make(chan ResponseInfo, len(urls))
	for i, url := range urls {
		go func(partyCode string, url string, credential string, contentType string, postBody string) {
			timeStart := time.Now()
			logEntry := &logutil.MonitorLogEntry{
				SessionID:  executor.SessionID,
				ActionName: fmt.Sprintf("%v@%v", "Executor", "RunExecutionPlan"),
				RawRequest: postBody,
			}
			responseBody, err := executor.EngineStub.webClient.Post(ctx, url, credential, contentType, postBody)
			logEntry.CostTime = time.Since(timeStart)
			if err != nil {
				logEntry.ErrorMsg = err.Error()
				logrus.Errorf("%v|PartyCode:%v|Url:%v", logEntry, partyCode, url)
			} else {
				logrus.Infof("%v|PartyCode:%v|Url:%v", logEntry, partyCode, url)
			}
			c <- ResponseInfo{
				ResponseBody: responseBody,
				Err:          err,
			}
		}(partyCodes[i], url, partyCredentials[i], executor.EngineStub.contentType, bodies[i])
	}

	outCols := []*scql.Tensor{}
	responses := []*scql.RunExecutionPlanResponse{}

	for i := 0; i < len(urls); i++ {
		responseInfo := <-c
		if responseInfo.Err != nil {
			return constant.ReasonCallEngineFail, nil, responseInfo.Err
		}
		body := responseInfo.ResponseBody
		response := &scql.RunExecutionPlanResponse{}
		_, err := message.DeserializeFrom(io.NopCloser(strings.NewReader(body)), response)
		if err != nil {
			return constant.ReasonInvalidResponse, nil, err
		}
		if response.GetStatus().GetCode() != int32(scql.Code_OK) {
			if response.GetStatus().GetCode() == int32(scql.Code_ENGINE_RUNSQL_ERROR) {
				return constant.ReasonInvalidResponse, nil, status.Wrap(scql.Code_ENGINE_RUNSQL_ERROR, fmt.Errorf(response.GetStatus().GetMessage()))
			}
			return constant.ReasonInvalidResponse, nil, status.Wrap(scql.Code_UNKNOWN_ENGINE_ERROR, fmt.Errorf(response.GetStatus().GetMessage()))
		}
		for _, col := range response.GetOutColumns() {
			if _, err := find(executor.OutputNames, col.GetName()); err == nil {
				outCols = append(outCols, col)
			}
		}
		responses = append(responses, response)
	}

	results, err := sortTensorByOutputName(outCols, executor.OutputNames)
	if err != nil {
		return "", newErrorSCQLQueryResult(scql.Code_INTERNAL, err.Error()), err
	}

	// NOTE: Remove ID field in Tensor.Name. e.g. f1.12 => f1
	for _, c := range results {
		c.Name = translator.TensorNameFromUniqueName(c.Name)
	}

	res := &scql.SCDBQueryResultResponse{
		Status:        responses[0].GetStatus(),
		ScdbSessionId: responses[0].GetSessionId(),
		OutColumns:    results,
	}

	return "", res, nil
}
