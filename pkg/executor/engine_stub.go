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
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/interpreter/optimizer"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	enginePb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

const (
	startSessionPath     = "/SCQLEngineService/StartSession"
	endSessionPath       = "/SCQLEngineService/StopSession"
	runDagPath           = "/SCQLEngineService/RunDag"
	runExecutionPlanPath = "/SCQLEngineService/RunExecutionPlan"
)

//go:generate mockgen -source engine_stub.go -destination engine_stub_mock.go -package executor
type EngineClient interface {
	Post(ctx context.Context, url string, credential string, content_type string, body string) (string, error)
}

func NewEngineClient(timeout time.Duration) EngineClient {
	return &httpClient{&http.Client{Timeout: timeout, Transport: &nethttp.Transport{}}}
}

type SimpleHttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type httpClient struct {
	client SimpleHttpClient
}

func (c httpClient) Post(ctx context.Context, url string, credential string, content_type string, body string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader((body)))
	if err != nil {
		log.Errorf("httpClient.Post/NewRequest failed: url: %v, err: %v", url, err)
		return "", err
	}
	req.Header.Add("Content-Type", content_type)
	req.Header.Add("Credential", credential)
	response, err := c.client.Do(req)
	if err != nil {
		log.Errorf("httpClient.Post/Do failed: url: %v, err: %v", url, err)
		return "", err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		log.Errorf("failed to read from response body with err:%v", err)
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", errors.Errorf("httpCode:%v, responseBody:%v", response.StatusCode, string(responseBody))
	}
	return string(responseBody), nil
}

// EngineStub struct
type EngineStub struct {
	executionPlanID string
	callBackHost    string
	callBackUri     string
	webClient       EngineClient
	protocol        string
	contentType     string

	partyInfo *translator.PartyInfo
}

// NewEngineStub creates an engine stub instance
func NewEngineStub(sessionID string,
	callBackHost string,
	callBackUri string,
	client EngineClient,
	engineProtocol string,
	contentType string, partyInfo *translator.PartyInfo) *EngineStub {
	scheme := strings.SplitN(engineProtocol, ":", 2)[0]
	if scheme == "" {
		scheme = "http"
	}
	return &EngineStub{
		executionPlanID: sessionID,
		callBackHost:    callBackHost,
		callBackUri:     callBackUri,
		webClient:       client,
		protocol:        scheme,
		contentType:     contentType,
		partyInfo:       partyInfo,
	}
}

func (stub *EngineStub) RunSubDAG(ctx context.Context, subDAG map[string]*optimizer.PartySubDAG, id int) error {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  stub.executionPlanID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "RunSubDAG"),
	}
	err := stub.runSubDAGCore(ctx, subDAG, id)
	logEntry.CostTime = time.Since(timeStart)
	if nil != err {
		logEntry.ErrorMsg = err.Error()
		log.Errorf("%v|SubDAGID:%v", logEntry, id)
	} else {
		log.Infof("%v|SubDAGID:%v", logEntry, id)
	}
	return err
}

// runSubDAGCore invokes a http request and sends the node to engine
func (stub *EngineStub) runSubDAGCore(ctx context.Context, subDAG map[string]*optimizer.PartySubDAG, id int) error {
	if len(subDAG) == 0 {
		return fmt.Errorf("subDAG == nil")
	}
	var bodies []string
	var partyCodes []string
	var dagIDs []int
	for code, partySubDAG := range subDAG {
		pb := &enginePb.RunDagRequest{
			Nodes:        make([]*enginePb.ExecNode, 0),
			DagId:        int32(id),
			SessionId:    stub.executionPlanID,
			CallbackHost: stub.callBackHost,
			CallbackUri:  stub.callBackUri,
		}
		for _, node := range partySubDAG.Nodes {
			pb.Nodes = append(pb.Nodes, node.ToProto())
		}
		encodingType, exists := message.ContentType2EncodingType[stub.contentType]
		if !exists {
			return fmt.Errorf("unsupported content type:%s", stub.contentType)
		}
		body, err := message.SerializeTo(pb, encodingType)
		if err != nil {
			return err
		}

		bodies = append(bodies, string(body))
		partyCodes = append(partyCodes, code)
		dagIDs = append(dagIDs, id)
		audit.RecordDagDetail(code, "", pb)
	}
	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForRunDag, runDagPath, bodies, partyCodes, dagIDs)
	if err != nil {
		return err
	}
	return checkRespStatus(respBodies)
}

func (stub *EngineStub) StartSession(ctx context.Context, sessionStartReq *enginePb.StartSessionRequest, partyCodes []string) error {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  stub.executionPlanID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "StartSession"),
	}
	err := stub.startSessionCore(ctx, sessionStartReq, partyCodes)
	logEntry.CostTime = time.Since(timeStart)
	if nil != err {
		logEntry.ErrorMsg = err.Error()
		log.Errorf("%v|sessionStartParams:%v|partyCodes:%v",
			logEntry, sessionStartReq, partyCodes)
	} else {
		log.Infof("%v|sessionStartParams:%v|partyCodes:%v",
			logEntry, sessionStartReq, partyCodes)
	}
	return err
}

func (stub *EngineStub) startSessionCore(ctx context.Context, sessionStartReq *enginePb.StartSessionRequest, partyCodes []string) error {
	if sessionStartReq == nil {
		return fmt.Errorf("startSession: invalid params")
	}
	var bodies []string
	for _, code := range partyCodes {
		sessionStartReq.SessionParams.PartyCode = code
		encodingType, exists := message.ContentType2EncodingType[stub.contentType]
		if !exists {
			return fmt.Errorf("unsupported content type:%s", stub.contentType)
		}
		body, err := message.SerializeTo(sessionStartReq, encodingType)
		if err != nil {
			return err
		}
		bodies = append(bodies, string(body))
		audit.RecordSessionParameters(sessionStartReq.GetSessionParams(), "", false)
	}

	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForStartSession, startSessionPath, bodies, partyCodes, nil)
	if err != nil {
		return err
	}
	return checkRespStatus(respBodies)
}

func (stub *EngineStub) EndSession(ctx context.Context, sessionEndParams *enginePb.StopSessionRequest, partyCodes []string) error {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  stub.executionPlanID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "EndSession"),
	}
	err := stub.endSessionCore(ctx, sessionEndParams, partyCodes)
	logEntry.CostTime = time.Since(timeStart)
	if nil != err {
		logEntry.ErrorMsg = err.Error()
		log.Errorf("%v|sessionEndParams:%v|partyCodes:%v", logEntry,
			sessionEndParams, partyCodes)
	} else {
		log.Infof("%v|sessionEndParams:%v|partyCodes:%v", logEntry,
			sessionEndParams, partyCodes)
	}
	return err
}

func (stub *EngineStub) genURLFor(partyCode string, apiPath string) (string, error) {
	host, err := stub.partyInfo.GetUrlByParty(partyCode)
	if err != nil {
		return "", err
	}
	url := url.URL{
		Scheme: stub.protocol,
		Host:   host,
		Path:   apiPath,
	}
	return url.String(), nil
}

func (stub *EngineStub) endSessionCore(ctx context.Context, sessionEndParams *enginePb.StopSessionRequest, partyCodes []string) error {
	if sessionEndParams == nil {
		return fmt.Errorf("end session: invalid params")
	}

	var postBodies []string
	for i := 0; i < len(partyCodes); i++ {

		encodingType, exists := message.ContentType2EncodingType[stub.contentType]
		if !exists {
			return fmt.Errorf("unsupported content type:%s", stub.contentType)
		}

		postBody, err := message.SerializeTo(sessionEndParams, encodingType)
		if err != nil {
			return err
		}
		postBodies = append(postBodies, string(postBody))
	}

	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForEndSession, endSessionPath, postBodies, partyCodes, nil)
	if err != nil {
		return err
	}
	return checkRespStatus(respBodies)
}

func checkRespStatus(respBodies []string) error {
	for _, respBody := range respBodies {
		respStatus := &enginePb.Status{}
		_, err := message.DeserializeFrom(io.NopCloser(strings.NewReader(respBody)), respStatus)
		if err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}
		if respStatus.GetCode() != int32(enginePb.Code_OK) {
			return status.NewStatusFromProto(respStatus)
		}
	}
	return nil
}

func (stub *EngineStub) postRequests(ctx context.Context, actionName string, path string, postBodies []string, partyCodes []string, dagIDs []int) ([]string, error) {
	c := make(chan ResponseInfo, len(partyCodes))
	for i, partyCode := range partyCodes {
		var dagID string
		if len(dagIDs) != 0 {
			if len(dagIDs) != len(partyCodes) {
				return nil, fmt.Errorf("invalid request, input params for size mismatch for dagIDs:%v, partyCodes:%v", dagIDs, partyCodes)
			}
			dagID = fmt.Sprint(dagIDs[i])
		}
		url, err := stub.genURLFor(partyCode, path)
		if err != nil {
			return nil, fmt.Errorf("failed to generate URL for party %v: %+v", partyCode, err)
		}

		credential, err := stub.partyInfo.GetCredentialByParty(partyCode)
		if err != nil {
			return nil, fmt.Errorf("no credential found for party %v: %+v", partyCode, err)
		}

		go func(stub *EngineStub, partyCode, url, partyCredential, contentType, postBody, dagID string) {
			timeStart := time.Now()
			logEntry := &logutil.MonitorLogEntry{
				SessionID:  stub.executionPlanID,
				ActionName: actionName,
				RawRequest: postBody,
			}
			respBody, err := stub.webClient.Post(ctx, url, partyCredential, contentType, postBody)
			logEntry.CostTime = time.Since(timeStart)
			if err != nil {
				logEntry.ErrorMsg = err.Error()
				log.Errorf("%v|PartyCode:%v|DagID:%v|Url:%v", logEntry, partyCode, dagID, url)
			} else {
				log.Infof("%v|PartyCode:%v|DagID:%v|Url:%v", logEntry, partyCode, dagID, url)
			}
			c <- ResponseInfo{
				ResponseBody: respBody,
				Err:          err,
			}

		}(stub, partyCode, url, credential, stub.contentType, postBodies[i], dagID)
	}
	var respBody []string
	for i := 0; i < len(partyCodes); i++ {
		respInfo := <-c
		if respInfo.Err != nil {
			return nil, respInfo.Err
		}
		respBody = append(respBody, respInfo.ResponseBody)
	}
	return respBody, nil
}
