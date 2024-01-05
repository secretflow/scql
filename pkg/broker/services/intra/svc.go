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

package intra

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

var _ pb.IntraBrokerServiceServer = &grpcIntraSvc{}

type grpcIntraSvc struct {
	app *application.App
}

type IntraSvc struct {
	grpcIntraSvc
}

func NewIntraSvc(app *application.App) *IntraSvc {
	return &IntraSvc{
		grpcIntraSvc{
			app: app,
		},
	}
}

func (svc *IntraSvc) CreateProjectHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "CreateProject"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.CreateProjectRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "CreateProjectHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.CreateProject(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.CreateProjectResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) ListProjectsHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "ListProjects"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.ListProjectsRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListProjectsHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.ListProjects(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListProjectsResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) InviteMemberHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "InviteMember"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.InviteMemberRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "InviteMemberHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.InviteMember(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.InviteMemberResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) ListInvitationsHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "ListInvitations"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.ListInvitationsRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListInvitationsRequest: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.ListInvitations(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListInvitationsResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) ProcessInvitationHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "ProcessInvitation"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.ProcessInvitationRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ProcessInvitationRequest: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.ProcessInvitation(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ProcessInvitationResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) DoQueryHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "DoQuery"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.QueryRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "DoQueryHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.DoQuery(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.QueryResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) SubmitQueryHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "SubmitQuery"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.QueryRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "SubmitQueryHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.SubmitQuery(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.SubmitResponse{}, err, inputEncodingType)
		return
	}
	logEntry.JobID = resp.JobId
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) FetchResultHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "FetchResult"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.FetchResultRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "FetchResultHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	logEntry.JobID = req.JobId
	resp, err := svc.FetchResult(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.QueryResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) CreateTableHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "CreateTable"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.CreateTableRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "CreateTableHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.CreateTable(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.CreateTableResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) ListTablesHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "ListTables"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.ListTablesRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListTablesHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.ListTables(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListTablesResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ListTablesHandler: %v", err),
		}}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) DropTableHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "DropTable"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.DropTableRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "DropTableHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.DropTable(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.DropTableResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) GrantCCLHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "GrantCCL"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.GrantCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "GrantCCLHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.GrantCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.GrantCCLResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) RevokeCCLHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "RevokeCCL"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.RevokeCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "RevokeCCLHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.RevokeCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.RevokeCCLResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) ShowCCLHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "ShowCCL"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	var req pb.ShowCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ShowCCLHandler: unable to parse request body: %v", err)
		return
	}
	logEntry.RawRequest = req.String()
	resp, err := svc.ShowCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ShowCCLResponse{}, err, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, err, inputEncodingType)
}

func (svc *IntraSvc) EngineCallbackHandler(c *gin.Context) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "Intra", "EngineCallback"),
	}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		// engine call back handler don't print request in log due to result in request
		common.LogWithError(logEntry, err)
	}()
	var req pb.ReportRequest
	_, err = message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("EngineCallbackHandler: unable to parse request body: %v", err)})
		return
	}
	_, err = svc.Report(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("EngineCallbackHandler: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}
