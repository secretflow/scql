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

	"github.com/secretflow/scql/pkg/broker/application"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

// Simplify the complexity of calling HTTP interfaces when writing debugging tools or test code.
type Command struct {
	host      string
	timeoutS  int
	intraStub application.IntraStub
}

type ResponseCheck interface {
	GetStatusCode() int32
	FormatResponse() string
}

type ResponseWrapper[T StatusGetter] struct {
	Response T
}

type StatusGetter interface {
	GetStatus() *pb.Status
}

func (resp *ResponseWrapper[T]) GetStatusCode() int32 {
	status := resp.Response.GetStatus()
	if status == nil {
		return -1
	}
	return status.GetCode()
}

func (resp *ResponseWrapper[T]) FormatResponse() string {
	status := resp.Response.GetStatus()
	if status == nil {
		return "status is nil"
	}
	return fmt.Sprintf("status: %d, message: %s", status.GetCode(), status.GetMessage())
}

func handleResponse(response ResponseCheck, name string) error {
	if response.GetStatusCode() == -1 {
		return fmt.Errorf("%s: invalid response: status is nil", name)
	}

	if response.GetStatusCode() == 0 {
		return nil
	} else {
		return fmt.Errorf("%s response: %v", name, response.FormatResponse())
	}
}

func NewResponseWrapper[T StatusGetter](response T) *ResponseWrapper[T] {
	return &ResponseWrapper[T]{
		Response: response,
	}
}

func NewCommand(host string, timeoutS int) *Command {
	return &Command{
		host:      host,
		timeoutS:  timeoutS,
		intraStub: application.IntraStub{Timeout: time.Duration(timeoutS) * time.Second},
	}
}

func (c *Command) CreateProject(projectID, projectConf string) (string, error) {
	var projConf pb.ProjectConfig
	err := message.ProtoUnmarshal([]byte(projectConf), &projConf)
	if err != nil {
		return "", fmt.Errorf("CreateProject: failed to deserialize project config: %v", err)
	}
	req := &pb.CreateProjectRequest{
		ProjectId: projectID,
		Conf:      &projConf,
	}
	response := &pb.CreateProjectResponse{}
	err = c.intraStub.CreateProject(c.host, req, response)
	if err != nil {
		return "", fmt.Errorf("CreateProject: failed to call creating project service: %v", err)
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

func (c *Command) ArchiveProject(projectID string) (string, error) {
	if projectID == "" {
		return "", fmt.Errorf("ArchiveProject: projectId must not be empty")
	}

	req := &pb.ArchiveProjectRequest{ProjectId: projectID}
	response := &pb.ArchiveProjectResponse{}
	err := c.intraStub.ArchiveProject(c.host, req, response)
	if err != nil {
		return "", fmt.Errorf("ArchiveProject: failed to call archiving project service: %v", err)
	}

	if response.Status == nil {
		return "", fmt.Errorf("ArchiveProject: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return response.GetStatus().Message, nil
	} else {
		return "", fmt.Errorf("ArchiveProject: status: %v", response.GetStatus())
	}
}

func (c *Command) CreateView(projectId, viewName, body string) error {
	if projectId == "" {
		return fmt.Errorf("CreateView: projectId must not be empty")
	}

	if viewName == "" {
		return fmt.Errorf("CreateView: viewName must not be empty")
	}

	if body == "" {
		return fmt.Errorf("CreateView: body must not be empty")
	}

	req := &pb.CreateViewRequest{
		ProjectId: projectId,
		ViewName:  viewName,
		Query:     body,
	}

	response := &pb.CreateViewResponse{}
	err := c.intraStub.CreateView(c.host, req, response)
	if err != nil {
		return fmt.Errorf("CreateView: failed to call creating view service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "CreateView")
}

func (c *Command) CreateTable(projectID, tableName, dbType, refTable string, columns []*pb.CreateTableRequest_ColumnDesc) error {
	if projectID == "" {
		return fmt.Errorf("CreateTable: projectId must not be empty")
	}
	if refTable == "" {
		return fmt.Errorf("CreateTable: refTable must not be empty")
	}
	req := &pb.CreateTableRequest{
		TableName: tableName,
		DbType:    dbType,
		ProjectId: projectID,
		RefTable:  refTable,
		Columns:   columns,
	}

	response := &pb.CreateTableResponse{}
	err := c.intraStub.CreateTable(c.host, req, response)
	if err != nil {
		return fmt.Errorf("CreateTable: failed to call creating table service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "CreateTable")
}

func (c *Command) DeleteView(projectID, viewName string) error {
	if projectID == "" {
		return fmt.Errorf("DeleteView: projectId must not be empty")
	}

	req := &pb.DropViewRequest{
		ProjectId: projectID,
		ViewName:  viewName,
	}
	response := &pb.DropViewResponse{}
	err := c.intraStub.DropView(c.host, req, response)
	if err != nil {
		return fmt.Errorf("DeleteView: failed to call deleting view service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "DeleteView")
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
		return fmt.Errorf("DeleteTable: failed to call dropping table service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "DeleteTable")
}

func (c *Command) GetProject(projectID string) (*pb.ListProjectsResponse, error) {
	req := &pb.ListProjectsRequest{}
	if projectID != "" {
		req.Ids = []string{projectID}
	}
	response := &pb.ListProjectsResponse{}
	err := c.intraStub.ListProjects(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetProject: failed to call listing project service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "GetProject")
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Command) GetView(projectID string, viewNames []string) (*pb.ListViewsResponse, error) {
	if projectID == "" {
		return nil, fmt.Errorf("GetView: projectId must not be empty")
	}

	req := &pb.ListViewsRequest{
		ProjectId: projectID,
		Names:     viewNames,
	}

	response := &pb.ListViewsResponse{}

	err := c.intraStub.ListViews(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetView: failed to call listing view service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "GetView")
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("GetTable: failed to call getting talbe info service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "GetTable")
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("GetCCL: failed to call listing CCL service: %v", err)
	}

	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "GetCCL")
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Command) GetInvitation() (*pb.ListInvitationsResponse, error) {
	req := &pb.ListInvitationsRequest{}
	response := &pb.ListInvitationsResponse{}
	err := c.intraStub.ListInvitations(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetInvitation: failed to call listing invitation service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "GetInvitation")
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Command) GrantCCL(projectID string, ccls []*pb.ColumnControl) error {
	if projectID == "" {
		return fmt.Errorf("GrantCCL: projectID must not be empty")
	}
	req := &pb.GrantCCLRequest{
		ProjectId: projectID,
	}

	req.ColumnControlList = ccls
	response := &pb.GrantCCLResponse{}
	err := c.intraStub.GrantCCL(c.host, req, response)
	if err != nil {
		return fmt.Errorf("GrantCCL: failed to call granting CLL service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "GrantCCL")
}

func (c *Command) InviteMember(projectID, member string) error {
	req := &pb.InviteMemberRequest{
		ProjectId: projectID,
		Invitee:   member,
	}
	response := &pb.InviteMemberResponse{}
	err := c.intraStub.InviteMember(c.host, req, response)
	if err != nil {
		return fmt.Errorf("InviteMember: failed to call inviting member service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "InviteMember")
}

func (c *Command) ProcessInvitation(ids string, accept bool) error {
	id, err := strconv.ParseUint(ids, 10, 64)
	if err != nil {
		return fmt.Errorf("ProcessInvitation: failed to parse input string as invitation id: %v", err)
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
		return fmt.Errorf("ProcessInvitation: failed to call processing invitation service: %v", err)
	}
	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "ProcessInvitation")
}

func (c *Command) RevokeCCL(projectID, party string, ccls []*pb.ColumnControl) error {
	if projectID == "" {
		return fmt.Errorf("RevokeCCL: projectID must not be empty")
	}
	req := &pb.RevokeCCLRequest{
		ProjectId: projectID,
	}

	req.ColumnControlList = ccls
	response := &pb.RevokeCCLResponse{}
	err := c.intraStub.RevokeCCL(c.host, req, response)
	if err != nil {
		return fmt.Errorf("RevokeCCL: failed to call revoking CCL service: %v", err)
	}

	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "RevokeCCL")
}

func (c *Command) DoQuery(projectID, query string, debugOpts *pb.DebugOptions, jobConfStr string) (*pb.QueryResponse, error) {
	jobConf := pb.JobConfig{}

	err := message.ProtoUnmarshal([]byte(jobConfStr), &jobConf)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize job config: %v", err)
	}

	req := &pb.QueryRequest{
		ProjectId: projectID,
		Query:     query,
		DebugOpts: debugOpts,
		JobConfig: &jobConf,
	}
	response := &pb.QueryResponse{}
	err = c.intraStub.RunQuery(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("DoQuery: failed to call query service: %v", err)
	}

	wrapper := NewResponseWrapper(response)
	err = handleResponse(wrapper, "DoQuery")
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Command) CreateJob(projectID, query string, debugOpts *pb.DebugOptions, jobConfStr string) (string, error) {
	var jobConfig pb.JobConfig
	err := message.ProtoUnmarshal([]byte(jobConfStr), &jobConfig)
	if err != nil {
		return "", fmt.Errorf("CreateJob: failed to deserialize job config: %v", err)
	}

	req := &pb.QueryRequest{
		ProjectId: projectID,
		Query:     query,
		DebugOpts: debugOpts,
		JobConfig: &jobConfig,
	}
	response := &pb.SubmitResponse{}
	err = c.intraStub.CreateJob(c.host, req, response)
	if err != nil {
		return "", fmt.Errorf("CreateJob: failed to call creating job service %w", err)
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

func (c *Command) GetResult(jobID string) (result *pb.FetchResultResponse, err error) {
	if jobID == "" {
		return nil, fmt.Errorf("GetResult: jobID must not be empty")
	}
	req := &pb.FetchResultRequest{
		JobId: jobID,
	}

	response := &pb.FetchResultResponse{}
	err = c.intraStub.GetResult(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetResult: failed to call getting result service: %w", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetResult: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == int32(pb.Code_OK) || response.GetStatus().GetCode() == int32(pb.Code_NOT_READY) {
		return response, nil
	} else {
		return nil, fmt.Errorf("GetResult status: %v", response.GetStatus())
	}
}

func (c *Command) GetExplain(projectID, query, jobConfStr string) (result *pb.ExplainInfo, err error) {
	if query == "" {
		return nil, fmt.Errorf("GetExplain: query must not be empty")
	}

	jobConf := &pb.JobConfig{}
	err = message.ProtoUnmarshal([]byte(jobConfStr), jobConf)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize job config: %v", err)
	}

	req := &pb.ExplainQueryRequest{
		ProjectId: projectID,
		Query:     query,
		JobConfig: jobConf,
	}

	response := &pb.ExplainQueryResponse{}
	err = c.intraStub.GetExplain(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("GetExplain: failed to call getting explain service: %w", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("GetExplain: invalid response: status is nil")
	}
	if response.GetStatus().GetCode() == 0 {
		return response.GetExplain(), nil
	} else {
		return nil, fmt.Errorf("GetExplain status: %v", response.GetStatus())
	}
}

func (c *Command) CancelJob(jobID string) error {
	req := &pb.CancelQueryRequest{
		JobId: jobID,
	}
	response := &pb.CancelQueryResponse{}
	err := c.intraStub.CancelJob(c.host, req, response)
	if err != nil {
		return fmt.Errorf("CancelJob: failed to call canceling job service: %w", err)
	}

	wrapper := NewResponseWrapper(response)
	return handleResponse(wrapper, "CancelJob")
}

func (c *Command) CheckAndUpdateStatus(ids []string) (*pb.CheckAndUpdateStatusResponse, error) {
	req := &pb.CheckAndUpdateStatusRequest{
		ProjectIds: ids,
	}
	response := &pb.CheckAndUpdateStatusResponse{}
	err := c.intraStub.CheckAndUpdateStatus(c.host, req, response)
	if err != nil {
		return nil, fmt.Errorf("CheckAndUpdateStatus: failed to call checking and updating status service: %w", err)
	}
	if response.Status == nil {
		return nil, fmt.Errorf("CheckAndUpdateStatus: invalid response: status is nil")
	}
	return response, err
}
