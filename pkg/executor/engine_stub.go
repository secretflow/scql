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
}

// NewEngineStub creates an engine stub instance
func NewEngineStub(sessionID string,
	callBackHost string,
	callBackUri string,
	client EngineClient,
	engineProtocol string,
	contentType string) *EngineStub {
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
	var dagURLs []string
	var partyCodes []string
	var partyCredentials []string
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

		url := url.URL{
			Scheme: stub.protocol,
			Host:   partySubDAG.PartyURL,
			Path:   runDagPath,
		}
		dagURL := url.String()
		dagURLs = append(dagURLs, dagURL)
		bodies = append(bodies, string(body))
		partyCodes = append(partyCodes, code)
		partyCredentials = append(partyCredentials, partySubDAG.Credential)
		dagIDs = append(dagIDs, id)
		audit.RecordDagDetail(code, dagURL, pb)
	}
	partyInfo, err := translator.NewPartyInfo(partyCodes, dagURLs, partyCredentials)
	if err != nil {
		return err
	}

	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForRunDag, bodies, *partyInfo, dagIDs)
	if err != nil {
		return err
	}
	return checkCommonRespStatus(respBodies)
}

func (stub *EngineStub) StartSession(ctx context.Context, sessionStartReq *enginePb.StartSessionRequest, partyInfo *translator.PartyInfo) error {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  stub.executionPlanID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "StartSession"),
	}
	err := stub.startSessionCore(ctx, sessionStartReq, *partyInfo)
	logEntry.CostTime = time.Since(timeStart)
	if nil != err {
		logEntry.ErrorMsg = err.Error()
		log.Errorf("%v|sessionStartParams:%v|partyCodes:%v|partyHosts:%v",
			logEntry, sessionStartReq, partyInfo.GetParties(), partyInfo.GetUrls())
	} else {
		log.Infof("%v|sessionStartParams:%v|partyCodes:%v|partyHosts:%v",
			logEntry, sessionStartReq, partyInfo.GetParties(), partyInfo.GetUrls())
	}
	return err
}

func (stub *EngineStub) startSessionCore(ctx context.Context, sessionStartReq *enginePb.StartSessionRequest, partyInfo translator.PartyInfo) error {
	if sessionStartReq == nil {
		return fmt.Errorf("startSession: invalid params")
	}
	var sessionStartURLs []string
	var bodies []string
	for i, host := range partyInfo.GetUrls() {
		sessionStartReq.SessionParams.PartyCode = partyInfo.GetParties()[i]
		encodingType, exists := message.ContentType2EncodingType[stub.contentType]
		if !exists {
			return fmt.Errorf("unsupported content type:%s", stub.contentType)
		}
		body, err := message.SerializeTo(sessionStartReq, encodingType)
		if err != nil {
			return err
		}
		bodies = append(bodies, string(body))
		url := url.URL{
			Scheme: stub.protocol,
			Host:   host,
			Path:   startSessionPath,
		}
		sessionURL := url.String()
		sessionStartURLs = append(sessionStartURLs, sessionURL)
		audit.RecordSessionParameters(sessionStartReq.GetSessionParams(), sessionURL, false)
	}
	partyInfo.UpdateUrls(sessionStartURLs)

	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForStartSession, bodies, partyInfo, nil)
	if err != nil {
		return err
	}
	return checkCommonRespStatus(respBodies)
}

func (stub *EngineStub) EndSession(ctx context.Context, sessionEndParams *enginePb.StopSessionRequest, partyInfo *translator.PartyInfo) error {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  stub.executionPlanID,
		ActionName: fmt.Sprintf("%v@%v", "EngineStub", "EndSession"),
	}
	err := stub.endSessionCore(ctx, sessionEndParams, *partyInfo)
	logEntry.CostTime = time.Since(timeStart)
	if nil != err {
		logEntry.ErrorMsg = err.Error()
		log.Errorf("%v|sessionEndParams:%v|partyCodes:%v|partyHosts:%v", logEntry,
			sessionEndParams, partyInfo.GetParties(), partyInfo.GetUrls())
	} else {
		log.Infof("%v|sessionEndParams:%v|partyCodes:%v|partyHosts:%v", logEntry,
			sessionEndParams, partyInfo.GetParties(), partyInfo.GetUrls())
	}
	return err
}

func (stub *EngineStub) endSessionCore(ctx context.Context, sessionEndParams *enginePb.StopSessionRequest, partyInfo translator.PartyInfo) error {
	if sessionEndParams == nil {
		return fmt.Errorf("end session: invalid params")
	}

	var sessionEndURLs []string
	var postBodies []string
	for _, host := range partyInfo.GetUrls() {
		url := url.URL{
			Scheme: stub.protocol,
			Host:   host,
			Path:   endSessionPath,
		}
		sessionURL := url.String()
		sessionEndURLs = append(sessionEndURLs, sessionURL)

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

	partyInfo.UpdateUrls(sessionEndURLs)
	respBodies, err := stub.postRequests(ctx, constant.ActionNameEnginePostForEndSession, postBodies, partyInfo, nil)
	if err != nil {
		return err
	}
	return checkCommonRespStatus(respBodies)
}

func checkCommonRespStatus(respBodies []string) error {
	for _, respBody := range respBodies {
		resp := &enginePb.StatusResponse{}
		_, err := message.DeserializeFrom(io.NopCloser(strings.NewReader(respBody)), resp)
		if err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}
		if resp.GetStatus().GetCode() != int32(enginePb.Code_OK) {
			return status.NewStatusFromProto(resp.GetStatus())
		}
	}
	return nil
}

func (stub *EngineStub) postRequests(ctx context.Context, actionName string, postBodies []string, partyInfo translator.PartyInfo, dagIDs []int) ([]string, error) {
	partyURLs := partyInfo.GetUrls()
	partyCodes := partyInfo.GetParties()
	partyCredentials := partyInfo.GetCredentials()
	c := make(chan ResponseInfo, len(partyURLs))
	for i, url := range partyURLs {
		var dagID string
		if len(dagIDs) != 0 {
			if len(dagIDs) != len(partyURLs) {
				return nil, fmt.Errorf("invalid request, input params for size mismatch for dagIDs:%v, partyURLs:%v", partyURLs, dagIDs)
			}
			dagID = fmt.Sprint(dagIDs[i])
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

		}(stub, partyCodes[i], url, partyCredentials[i], stub.contentType, postBodies[i], dagID)
	}
	var respBody []string
	for i := 0; i < len(partyURLs); i++ {
		respInfo := <-c
		if respInfo.Err != nil {
			return nil, respInfo.Err
		}
		respBody = append(respBody, respInfo.ResponseBody)
	}
	return respBody, nil
}
