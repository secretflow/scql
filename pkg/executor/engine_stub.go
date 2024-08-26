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
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
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
}

// NewEngineStub creates an engine stub instance
func NewEngineStub(sessionID string,
	callBackProtocol string,
	callBackHost string,
	callBackPath string,
	client EngineClient,
	engineProtocol string,
	contentType string) *EngineStub {
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
	}
}
