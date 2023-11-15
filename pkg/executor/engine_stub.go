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

	"github.com/secretflow/scql/pkg/constant"
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
	// callback URL
	cbURL       string
	webClient   EngineClient
	protocol    string
	contentType string

	partyInfo *translator.PartyInfo
}

// NewEngineStub creates an engine stub instance
func NewEngineStub(sessionID string,
	callBackProtocol string,
	callBackHost string,
	callBackPath string,
	client EngineClient,
	engineProtocol string,
	contentType string, partyInfo *translator.PartyInfo) *EngineStub {
	scheme := strings.SplitN(engineProtocol, ":", 2)[0]
	if scheme == "" {
		scheme = "http"
	}

	cbURL := url.URL{
		Scheme: callBackProtocol,
		Host:   callBackHost,
		Path:   callBackPath,
	}
	return &EngineStub{
		executionPlanID: sessionID,
		cbURL:           cbURL.String(),
		webClient:       client,
		protocol:        scheme,
		contentType:     contentType,
		partyInfo:       partyInfo,
	}
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
