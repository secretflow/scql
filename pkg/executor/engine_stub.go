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

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/config"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

const (
	runExecutionPlanPath = "/SCQLEngineService/RunExecutionPlan"
)

const (
	EngineClientTypeGRPC = "GRPC"
	EngineClientTypeHTTP = "HTTP"
)

//go:generate mockgen -source engine_stub.go -destination engine_stub_mock.go -package executor
type EngineClient interface {
	RunExecutionPlan(url, credential string, executionPlanReq *pb.RunExecutionPlanRequest) (*pb.RunExecutionPlanResponse, error)
}

func NewEngineClient(clientType string, timeout time.Duration, tlsCfg *config.TLSConf, contentType, protocol string) EngineClient {
	if clientType == EngineClientTypeGRPC {
		return NewGRPCEngineClient(timeout, tlsCfg)
	} else {
		protocol = strings.SplitN(protocol, ":", 2)[0]
		if protocol == "" {
			protocol = "http"
		}
		return NewHttpEngineClient(timeout, contentType, protocol)
	}
}

func NewHttpEngineClient(timeout time.Duration, contentType, protocol string) EngineClient {
	return &HttpEngineClient{
		protocol:    protocol,
		contentType: contentType,
		client:      &http.Client{Timeout: timeout, Transport: &http.Transport{}},
	}
}

func NewGRPCEngineClient(timeout time.Duration, tlsCfg *config.TLSConf) EngineClient {
	return &GrpcEngineClient{timeout: timeout, tlsCfg: tlsCfg}
}

func (h *HttpEngineClient) RunExecutionPlan(host, credential string, executionPlanReq *pb.RunExecutionPlanRequest) (*pb.RunExecutionPlanResponse, error) {
	url := url.URL{
		Scheme: h.protocol,
		Host:   host,
		Path:   runExecutionPlanPath,
	}
	urlStr := url.String()

	m := protojson.MarshalOptions{UseProtoNames: true}
	body, err := m.Marshal(executionPlanReq)
	if err != nil {
		return nil, errors.Errorf("Marshal error: %v", err)
	}
	postBody := string(body)

	ctx := context.TODO()
	responseBody, err := h.Post(ctx, urlStr, credential, h.contentType, postBody)
	if err != nil {
		return nil, errors.Errorf("Post error: %v", err)
	}

	executionPlanRes := &pb.RunExecutionPlanResponse{}
	_, err = message.DeserializeFrom(io.NopCloser(strings.NewReader(responseBody)), executionPlanRes, h.contentType)
	if err != nil {
		return nil, errors.Errorf("Deserialize error: %v", err)
	}

	return executionPlanRes, nil
}

func (g *GrpcEngineClient) RunExecutionPlan(url, credential string, executionPlanReq *pb.RunExecutionPlanRequest) (*pb.RunExecutionPlanResponse, error) {
	conn, err := NewEngineClientConn(url, credential, g.tlsCfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewSCQLEngineServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()

	executionPlanRes, err := client.RunExecutionPlan(ctx, executionPlanReq)
	if err != nil {
		return nil, err
	}
	return executionPlanRes, nil
}

func (h *HttpEngineClient) Post(ctx context.Context, url, credential, contentType, body string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		log.Errorf("HttpEngineClient.Post/NewRequest failed: url: %v, err: %v", url, err)
		return "", err
	}
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Credential", credential)
	response, err := h.client.Do(req)
	if err != nil {
		log.Errorf("HttpEngineClient.Post/Do failed: url: %v, err: %v", url, err)
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

type HttpEngineClient struct {
	protocol    string
	contentType string
	client      *http.Client
}

type GrpcEngineClient struct {
	timeout time.Duration
	tlsCfg  *config.TLSConf
}

// EngineStub struct
type EngineStub struct {
	executionPlanID string
	// callback URL
	cbURL     string
	webClient EngineClient
}

// NewEngineStub creates an engine stub instance
func NewEngineStub(
	sessionID string,
	callBackProtocol string,
	callBackHost string,
	callBackPath string,
	client EngineClient) *EngineStub {
	cbURL := url.URL{
		Scheme: callBackProtocol,
		Host:   callBackHost,
		Path:   callBackPath,
	}

	return &EngineStub{
		executionPlanID: sessionID,
		cbURL:           cbURL.String(),
		webClient:       client,
	}
}
