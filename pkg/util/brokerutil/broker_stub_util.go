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

// Due to circular references, please avoid moving this file to testutil.
package brokerutil

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/application"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
)

// Simplify the complexity of calling HTTP interfaces when writing debugging tools or test code.
type Command struct {
	host      string
	timeoutS  int
	intraStub application.IntraStub
}

func NewCommand(host string, timeoutS int) *Command {
	return &Command{
		host:      host,
		timeoutS:  timeoutS,
		intraStub: application.IntraStub{Timeout: time.Duration(timeoutS) * time.Second},
	}
}

func (c *Command) CreateProject(projectID, projectConf string) (string, error) {
	var spuConf spu.RuntimeConfig
	err := protojson.Unmarshal([]byte(projectConf), &spuConf)
	if err != nil {
		return "", fmt.Errorf("CreateProject: unmarshal: %v", err)
	}
	req := &pb.CreateProjectRequest{
		ProjectId: projectID,
		Conf: &pb.ProjectConfig{
			SpuRuntimeCfg: &spuConf,
		},
	}
	response := &pb.CreateProjectResponse{}
	err = c.intraStub.CreateProject(c.host, req, response)
	if err != nil {
		return "", fmt.Errorf("CreateProject: %v", err)
	}
	if response.Status == nil {
		return "", fmt.Errorf("CreateProject: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return response.GetProjectId(), nil
	} else {
		return "", fmt.Errorf("CreateProject: status: %v", response.GetStatus())
	}
}

func (c *Command) CreateTable(projectID, tableName, dbType, refTable string, columns []*pb.CreateTableRequest_ColumnDesc) error {
	req := &pb.CreateTableRequest{
		TableName: tableName,
		DbType:    dbType,
	}
	if projectID == "" {
		return fmt.Errorf("CreateTable: projectId must not be empty")
	} else {
		req.ProjectId = projectID
	}
	if refTable == "" {
		return fmt.Errorf("CreateTable: refTable must not be empty")
	} else {
		req.RefTable = refTable
	}
	req.Columns = columns

	response := &pb.CreateTableResponse{}
	err := c.intraStub.CreateTable(c.host, req, response)
	if err != nil {
		return fmt.Errorf("CreateTable: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("CreateTable: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("CreateTable status: %v", response.GetStatus())
	}
}

func (c *Command) DeleteTable(projectID, name string) error {
	if projectID == "" {
		return fmt.Errorf("DeleteTable: projectId must not be empty")
	}
	req := &pb.DropTableRequest{
		ProjectId: projectID,
		TableName: name,
	}
	response := &pb.DropTableResponse{}
	err := c.intraStub.DropTable(c.host, req, response)
	if err != nil {
		return fmt.Errorf("DeleteTable: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("DeleteTable: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("DeleteTable response: %v", protojson.Format(response))
	}

}

func (c *Command) GetProject(projectID string) (*pb.ListProjectsResponse, error) {
	req := &pb.ListProjectsRequest{}
	if projectID != "" {
		req.Ids = []string{projectID}
	}
	response := &pb.ListProjectsResponse{}
	err := c.intraStub.ListProjects(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetProject: %v", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetProject: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("GetProject response: %v", protojson.Format(response))
	}
	return response, nil
}

func (c *Command) GetTable(projectID string, tableNames []string) (*pb.ListTablesResponse, error) {
	if projectID == "" {
		fmt.Printf("GetTable: projectId must not be empty")
	}
	req := &pb.ListTablesRequest{
		ProjectId: projectID,
		Names:     tableNames,
	}
	response := &pb.ListTablesResponse{}
	err := c.intraStub.ListTables(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetTable: %v", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetTable: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("GetTable response: %v", protojson.Format(response))
	}
	return response, nil
}

func (c *Command) GetCCL(projectID string, tables, destParties []string) (*pb.ShowCCLResponse, error) {
	if projectID == "" {
		fmt.Printf("GetCCL: projectID must not be empty")
	}
	req := &pb.ShowCCLRequest{
		ProjectId:   projectID,
		Tables:      tables,
		DestParties: destParties,
	}

	response := &pb.ShowCCLResponse{}
	err := c.intraStub.ListCCLs(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetCCL: %v", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetCCL: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("GetCCL response: %v", protojson.Format(response))
	}
	return response, nil
}

func (c *Command) GetInvitation() (*pb.ListInvitationsResponse, error) {
	req := &pb.ListInvitationsRequest{}
	response := &pb.ListInvitationsResponse{}
	err := c.intraStub.ListInvitations(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetInvitation: %v", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetInvitation: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("GetInvitation response: %v", protojson.Format(response))
	}
	return response, nil
}

func (c *Command) GrantCCL(projectID string, ccls []*pb.ColumnControl) error {
	req := &pb.GrantCCLRequest{}
	if projectID == "" {
		return fmt.Errorf("GrantCCL: projectID must not be empty")
	} else {
		req.ProjectId = projectID
	}
	req.ColumnControlList = ccls
	response := &pb.GrantCCLResponse{}
	err := c.intraStub.GrantCCL(c.host, req, response)
	if err != nil {
		return fmt.Errorf("GrantCCL: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("GrantCCL: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("GrantCCL response: %v", protojson.Format(response))
	}
}

func (c *Command) InviteMember(projectID, member string) error {
	req := &pb.InviteMemberRequest{
		ProjectId: projectID,
		Invitee:   member,
	}
	response := &pb.InviteMemberResponse{}
	err := c.intraStub.InviteMember(c.host, req, response)
	if err != nil {
		return fmt.Errorf("InviteMember: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("InviteMember: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("InviteMember response: %v", protojson.Format(response))
	}
}

func (c *Command) ProcessInvitation(ids string, accept bool) error {
	id, err := strconv.ParseUint(ids, 10, 64)
	if err != nil {
		return fmt.Errorf("ProcessInvitation: %v", err)
	}
	resp := pb.InvitationRespond_DECLINE
	if accept {
		resp = pb.InvitationRespond_ACCEPT
	}
	req := &pb.ProcessInvitationRequest{
		InvitationId: id,
		Respond:      resp,
	}
	response := &pb.ProcessInvitationResponse{}

	err = c.intraStub.ProcessInvitation(c.host, req, response)
	if err != nil {
		return fmt.Errorf("ProcessInvitation: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("ProcessInvitation: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("ProcessInvitation response: %v", protojson.Format(response))
	}
}

func (c *Command) RevokeCCL(projectID, party string, ccls []*pb.ColumnControl) error {
	req := &pb.RevokeCCLRequest{}
	if projectID == "" {
		return fmt.Errorf("RevokeCCL: projectID must not be empty")
	} else {
		req.ProjectId = projectID
	}

	req.ColumnControlList = ccls
	response := &pb.RevokeCCLResponse{}
	err := c.intraStub.RevokeCCL(c.host, req, response)
	if err != nil {
		return fmt.Errorf("RevokeCCL: %v", err)
	}
	if response.Status == nil {
		return fmt.Errorf("RevokeCCL: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("RevokeCCL response: %v", protojson.Format(response))
	}
}

func (c *Command) DoQuery(projectID, query string, debugOpts *pb.DebugOptions) (*pb.QueryResponse, error) {
	req := &pb.QueryRequest{
		ProjectId: projectID,
		Query:     query,
		DebugOpts: debugOpts,
	}
	response := &pb.QueryResponse{}
	err := c.intraStub.RunQuery(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("DoQuery: %v", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("DoQuery: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("DoQuery response: %v", protojson.Format(response))
	}
	return response, nil
}

func (c *Command) CreateJob(projectID, query string, debugOpts *pb.DebugOptions) (string, error) {
	req := &pb.QueryRequest{
		ProjectId: projectID,
		Query:     query,
		DebugOpts: debugOpts,
	}
	response := &pb.SubmitResponse{}
	err := c.intraStub.CreateJob(c.host, req, response)
	if err != nil {
		return "", fmt.Errorf("CreateJob: %w", err)
	}
	if response.Status == nil {
		return "", fmt.Errorf("CreateJob: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return response.JobId, nil
	} else {
		return "", fmt.Errorf("CreateJob status: %v", response.GetStatus())
	}
}

func (c *Command) GetResult(jobID string) (result *pb.QueryResponse, not_ready bool, err error) {
	req := &pb.FetchResultRequest{}
	if jobID == "" {
		return nil, false, fmt.Errorf("GetResult: jobID must not be empty")
	} else {
		req.JobId = jobID
	}
	response := &pb.QueryResponse{}
	err = c.intraStub.GetResult(c.host, req, response)
	if err != nil {
		return
	}
	if response.Status == nil {
		return nil, false, fmt.Errorf("GetResult: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return response, false, nil
	} else if response.GetStatus().GetCode() == int32(pb.Code_NOT_READY) {
		return nil, true, nil
	} else {
		return nil, false, fmt.Errorf("GetResult response: %v", protojson.Format(response))
	}
}

func (c *Command) CancelJob(jobID string) error {
	req := &pb.CancelQueryRequest{
		JobId: jobID,
	}
	response := &pb.CancelQueryResponse{}
	err := c.intraStub.CancelJob(c.host, req, response)
	if err != nil {
		return fmt.Errorf("CancelJob: %w", err)
	}
	if response.Status == nil {
		return fmt.Errorf("CancelJob: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("CancelJob status: %v", response.GetStatus())
	}
}
