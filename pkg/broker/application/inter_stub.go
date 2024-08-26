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

package application

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/auth"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
	urlutil "github.com/secretflow/scql/pkg/util/url"
)

type InterStub struct {
	Timeout      time.Duration
	EncodingType message.ContentEncodingType
	Auth         *auth.Auth
}

func (stub *InterStub) baseCall(url, path string, req proto.Message, response proto.Message) (err error) {
	err = stub.Auth.SignMessage(req)
	if err != nil {
		return
	}
	requestStr, err := message.SerializeTo(req, stub.EncodingType)
	if err != nil {
		return
	}
	httpClient := &http.Client{Timeout: stub.Timeout}
	interResp, err := httpClient.Post(urlutil.JoinHostPath(url, path),
		message.EncodingType2ContentType[stub.EncodingType], strings.NewReader(requestStr))
	if err != nil {
		return
	}
	if interResp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(interResp.Body)
		if err != nil {
			return fmt.Errorf("status: %v, read body err: %v", interResp.Status, err)
		}
		return fmt.Errorf("status: %v, body: %s", interResp.Status, body)
	}

	defer interResp.Body.Close()
	_, err = message.DeserializeFrom(interResp.Body, response, interResp.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	return nil
}

func (stub *InterStub) ExchangeJobInfo(url string, req *pb.ExchangeJobInfoRequest, response *pb.ExchangeJobInfoResponse) (err error) {
	return stub.baseCall(url, constant.ExchangeJobInfoPath, req, response)
}

func (stub *InterStub) AskInfo(url string, req *pb.AskInfoRequest, response *pb.AskInfoResponse) (err error) {
	return stub.baseCall(url, constant.AskInfoPath, req, response)
}

func (stub *InterStub) InviteToProject(url string, req *pb.InviteToProjectRequest, response *pb.InviteToProjectResponse) (err error) {
	return stub.baseCall(url, constant.InviteToProjectPath, req, response)
}

func (stub *InterStub) SyncInfo(url string, req *pb.SyncInfoRequest, response *pb.SyncInfoResponse) (err error) {
	return stub.baseCall(url, constant.SyncInfoPath, req, response)
}

func (stub *InterStub) ReplyInvitation(url string, req *pb.ReplyInvitationRequest, response *pb.ReplyInvitationResponse) (err error) {
	return stub.baseCall(url, constant.ReplyInvitationPath, req, response)
}

func (stub *InterStub) DistributeQuery(url string, req *pb.DistributeQueryRequest, response *pb.DistributeQueryResponse) (err error) {
	return stub.baseCall(url, constant.DistributeQueryPath, req, response)
}

func (stub *InterStub) CancelQueryJob(url string, req *pb.CancelQueryJobRequest, response *pb.CancelQueryJobResponse) (err error) {
	return stub.baseCall(url, constant.CancelQueryJobPath, req, response)
}
