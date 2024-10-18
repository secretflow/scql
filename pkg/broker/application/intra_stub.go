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
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
	urlutil "github.com/secretflow/scql/pkg/util/url"
)

type IntraStub struct {
	Timeout time.Duration
}

func (stub *IntraStub) baseCall(url, path string, req proto.Message, response proto.Message) (err error) {
	if url == "" {
		return fmt.Errorf("url cannot be empty")
	}
	requestStr, err := message.SerializeTo(req, message.EncodingTypeJson)
	if err != nil {
		return
	}
	httpClient := &http.Client{Timeout: stub.Timeout}
	intraResp, err := httpClient.Post(urlutil.JoinHostPath(url, path),
		"application/json", strings.NewReader(requestStr))
	if err != nil {
		return
	}
	if intraResp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(intraResp.Body)
		if err != nil {
			return fmt.Errorf("status: %v, read body err: %v", intraResp.Status, err)
		}
		return fmt.Errorf("status: %v, body: %s", intraResp.Status, body)
	}
	defer intraResp.Body.Close()

	_, err = message.DeserializeFrom(intraResp.Body, response, intraResp.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	return nil
}

func (stub *IntraStub) CreateProject(url string, req *pb.CreateProjectRequest, response *pb.CreateProjectResponse) (err error) {
	return stub.baseCall(url, constant.CreateProjectPath, req, response)
}

func (stub *IntraStub) ListProjects(url string, req *pb.ListProjectsRequest, response *pb.ListProjectsResponse) (err error) {
	return stub.baseCall(url, constant.ListProjectsPath, req, response)
}

func (stub *IntraStub) InviteMember(url string, req *pb.InviteMemberRequest, response *pb.InviteMemberResponse) (err error) {
	return stub.baseCall(url, constant.InviteMemberPath, req, response)
}

func (stub *IntraStub) ListInvitations(url string, req *pb.ListInvitationsRequest, response *pb.ListInvitationsResponse) (err error) {
	return stub.baseCall(url, constant.ListInvitationsPath, req, response)
}

func (stub *IntraStub) ProcessInvitation(url string, req *pb.ProcessInvitationRequest, response *pb.ProcessInvitationResponse) (err error) {
	return stub.baseCall(url, constant.ProcessInvitationPath, req, response)
}

func (stub *IntraStub) CreateTable(url string, req *pb.CreateTableRequest, response *pb.CreateTableResponse) (err error) {
	return stub.baseCall(url, constant.CreateTablePath, req, response)
}

func (stub *IntraStub) DropTable(url string, req *pb.DropTableRequest, response *pb.DropTableResponse) (err error) {
	return stub.baseCall(url, constant.DropTablePath, req, response)
}

func (stub *IntraStub) ListTables(url string, req *pb.ListTablesRequest, response *pb.ListTablesResponse) (err error) {
	return stub.baseCall(url, constant.ListTablesPath, req, response)
}

func (stub *IntraStub) GrantCCL(url string, req *pb.GrantCCLRequest, response *pb.GrantCCLResponse) (err error) {
	return stub.baseCall(url, constant.GrantCCLPath, req, response)
}

func (stub *IntraStub) RevokeCCL(url string, req *pb.RevokeCCLRequest, response *pb.RevokeCCLResponse) (err error) {
	return stub.baseCall(url, constant.RevokeCCLPath, req, response)
}

func (stub *IntraStub) ListCCLs(url string, req *pb.ShowCCLRequest, response *pb.ShowCCLResponse) (err error) {
	return stub.baseCall(url, constant.ShowCCLPath, req, response)
}

func (stub *IntraStub) RunQuery(url string, req *pb.QueryRequest, response *pb.QueryResponse) (err error) {
	return stub.baseCall(url, constant.DoQueryPath, req, response)
}

func (stub *IntraStub) CreateJob(url string, req *pb.QueryRequest, response *pb.SubmitResponse) (err error) {
	return stub.baseCall(url, constant.SubmitQueryPath, req, response)
}

func (stub *IntraStub) GetResult(url string, req *pb.FetchResultRequest, response *pb.FetchResultResponse) (err error) {
	return stub.baseCall(url, constant.FetchResultPath, req, response)
}

func (stub *IntraStub) GetExplain(url string, req *pb.ExplainQueryRequest, response *pb.ExplainQueryResponse) (err error) {
	return stub.baseCall(url, constant.ExplainQueryPath, req, response)
}

func (stub *IntraStub) CancelJob(url string, req *pb.CancelQueryRequest, response *pb.CancelQueryResponse) (err error) {
	return stub.baseCall(url, constant.CancelQueryPath, req, response)
}

func (stub *IntraStub) CheckAndUpdateStatus(url string, req *pb.CheckAndUpdateStatusRequest, response *pb.CheckAndUpdateStatusResponse) (err error) {
	return stub.baseCall(url, constant.CheckAndUpdateStatusPath, req, response)
}
