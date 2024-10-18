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
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/reflect/protoreflect"

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

func (svc *IntraSvc) HealthHandler(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

func (svc *IntraSvc) CreateProjectHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.CreateProjectRequest{},
		&pb.CreateProjectResponse{},
		func(ctx context.Context, req *pb.CreateProjectRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.CreateProjectResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "CreateProject")
			return svc.CreateProject(ctx, req)
		})
}

func (svc *IntraSvc) ListProjectsHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ListProjectsRequest{},
		&pb.ListProjectsResponse{},
		func(ctx context.Context, req *pb.ListProjectsRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ListProjectsResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ListProjects")
			return svc.ListProjects(ctx, req)
		})
}

func (svc *IntraSvc) InviteMemberHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.InviteMemberRequest{},
		&pb.InviteMemberResponse{},
		func(ctx context.Context, req *pb.InviteMemberRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.InviteMemberResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "InviteMember")
			return svc.InviteMember(ctx, req)
		})
}

func (svc *IntraSvc) ListInvitationsHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ListInvitationsRequest{},
		&pb.ListInvitationsResponse{},
		func(ctx context.Context, req *pb.ListInvitationsRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ListInvitationsResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ListInvitations")
			return svc.ListInvitations(ctx, req)
		})
}

func (svc *IntraSvc) ProcessInvitationHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ProcessInvitationRequest{},
		&pb.ProcessInvitationResponse{},
		func(ctx context.Context, req *pb.ProcessInvitationRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ProcessInvitationResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ProcessInvitation")
			return svc.ProcessInvitation(ctx, req)
		})
}

func (svc *IntraSvc) DoQueryHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.QueryRequest{},
		&pb.QueryResponse{},
		func(ctx context.Context, req *pb.QueryRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.QueryResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "DoQuery")
			return svc.DoQuery(ctx, req)
		})
}

func (svc *IntraSvc) SubmitQueryHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.QueryRequest{},
		&pb.SubmitResponse{},
		func(ctx context.Context, req *pb.QueryRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.SubmitResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "SubmitQuery")
			resp, err := svc.SubmitQuery(ctx, req)
			if err == nil {
				logEntry.JobID = resp.JobId
			}
			return resp, err
		})
}

func (svc *IntraSvc) FetchResultHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.FetchResultRequest{},
		&pb.FetchResultResponse{},
		func(ctx context.Context, req *pb.FetchResultRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.FetchResultResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.JobID = req.JobId
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "FetchResult")
			return svc.FetchResult(ctx, req)
		})
}

func (svc *IntraSvc) ExplainQueryHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ExplainQueryRequest{},
		&pb.ExplainQueryResponse{},
		func(ctx context.Context, req *pb.ExplainQueryRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ExplainQueryResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ExplainQuery")
			return svc.ExplainQuery(ctx, req)
		})
}

func (svc *IntraSvc) CancelQueryHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.CancelQueryRequest{},
		&pb.CancelQueryResponse{},
		func(ctx context.Context, req *pb.CancelQueryRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.CancelQueryResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.JobID = req.JobId
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "CancelQuery")
			return svc.CancelQuery(ctx, req)
		})
}

func (svc *IntraSvc) CreateTableHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.CreateTableRequest{},
		&pb.CreateTableResponse{},
		func(ctx context.Context, req *pb.CreateTableRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.CreateTableResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "CreateTable")
			return svc.CreateTable(ctx, req)
		})
}

func (svc *IntraSvc) ListTablesHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ListTablesRequest{},
		&pb.ListTablesResponse{},
		func(ctx context.Context, req *pb.ListTablesRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ListTablesResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ListTables")
			return svc.ListTables(ctx, req)
		})
}

func (svc *IntraSvc) DropTableHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.DropTableRequest{},
		&pb.DropTableResponse{},
		func(ctx context.Context, req *pb.DropTableRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.DropTableResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "DropTable")
			return svc.DropTable(ctx, req)
		})
}

func (svc *IntraSvc) GrantCCLHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.GrantCCLRequest{},
		&pb.GrantCCLResponse{},
		func(ctx context.Context, req *pb.GrantCCLRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.GrantCCLResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "GrantCCL")
			return svc.GrantCCL(ctx, req)
		})
}

func (svc *IntraSvc) RevokeCCLHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.RevokeCCLRequest{},
		&pb.RevokeCCLResponse{},
		func(ctx context.Context, req *pb.RevokeCCLRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.RevokeCCLResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "RevokeCCL")
			return svc.RevokeCCL(ctx, req)
		})
}

func (svc *IntraSvc) ShowCCLHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.ShowCCLRequest{},
		&pb.ShowCCLResponse{},
		func(ctx context.Context, req *pb.ShowCCLRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ShowCCLResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "ShowCCL")
			return svc.ShowCCL(ctx, req)
		})
}

func (svc *IntraSvc) CheckAndUpdateStatusHandler(c *gin.Context) {
	handlerWrapper(c,
		&pb.CheckAndUpdateStatusRequest{},
		&pb.CheckAndUpdateStatusResponse{},
		func(ctx context.Context, req *pb.CheckAndUpdateStatusRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.CheckAndUpdateStatusResponse, error) {
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Intra", "CheckAndUpdateStatus")
			return svc.CheckAndUpdateStatus(ctx, req)
		})
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
	_, err = message.DeserializeFrom(c.Request.Body, &req, c.Request.Header.Get("Content-Type"))
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

func handlerWrapper[In, Out protoreflect.ProtoMessage](
	c *gin.Context,
	req In, resp Out,
	fn func(context.Context, In, *logutil.BrokerMonitorLogEntry) (Out, error)) {
	var err error
	timeStart := time.Now()
	logEntry := &logutil.BrokerMonitorLogEntry{}
	defer func() {
		logEntry.CostTime = time.Since(timeStart)
		common.LogWithError(logEntry, err)
	}()
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, req, c.Request.Header.Get("Content-Type"))
	if err != nil {
		c.String(http.StatusBadRequest, "unable to parse request body: %v", err)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			stackInfo := string(debug.Stack())
			err = fmt.Errorf("unexpected error: %+v", r)
			logrus.Infof("err=%v, stack=%s\n", err, stackInfo)
		}
		common.FeedResponse(c, resp, err, inputEncodingType)
	}()
	response, err := fn(c.Request.Context(), req, logEntry)
	if err != nil {
		return
	}
	resp = response
}
