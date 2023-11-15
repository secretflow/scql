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

	"github.com/gin-gonic/gin"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
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
	var req pb.CreateProjectRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "CreateProjectHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.CreateProject(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.CreateProjectResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("CreateProjectHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) ListProjectsHandler(c *gin.Context) {
	var req pb.ListProjectsRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListProjectsHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.ListProjects(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListProjectsResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ListProjectsHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) InviteMemberHandler(c *gin.Context) {
	var req pb.InviteMemberRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "InviteMemberHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.InviteMember(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.InviteMemberResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("InviteMemberHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) ListInvitationsHandler(c *gin.Context) {
	var req pb.ListInvitationsRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListInvitationsRequest: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.ListInvitations(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListInvitationsResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ListInvitationsHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) ProcessInvitationHandler(c *gin.Context) {
	var req pb.ProcessInvitationRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ProcessInvitationRequest: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.ProcessInvitation(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ProcessInvitationResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ProcessInvitationHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) DoQueryHandler(c *gin.Context) {
	var req pb.QueryRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "DoQueryHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.DoQuery(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.QueryResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("DoQueryHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) SubmitQueryHandler(c *gin.Context) {
	var req pb.QueryRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "SubmitQueryHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.SubmitQuery(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.SubmitResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("SubmitQueryHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) FetchResultHandler(c *gin.Context) {
	var req pb.FetchResultRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "FetchResultHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.FetchResult(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.QueryResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("FetchResultHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) CreateTableHandler(c *gin.Context) {
	var req pb.CreateTableRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "CreateTableHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.CreateTable(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.CreateTableResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("CreateTableHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) ListTablesHandler(c *gin.Context) {
	var req pb.ListTablesRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ListTablesHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.ListTables(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ListTablesResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ListTablesHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) DropTableHandler(c *gin.Context) {
	var req pb.DropTableRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "DropTableHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.DropTable(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.DropTableResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("DropTableHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) GrantCCLHandler(c *gin.Context) {
	var req pb.GrantCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "GrantCCLHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.GrantCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.GrantCCLResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("GrantCCLHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) RevokeCCLHandler(c *gin.Context) {
	var req pb.RevokeCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "RevokeCCLHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.RevokeCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.RevokeCCLResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("RevokeCCLHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) ShowCCLHandler(c *gin.Context) {
	var req pb.ShowCCLRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ShowCCLHandler: unable to parse request body: %v", err)
		return
	}

	resp, err := svc.ShowCCL(c.Request.Context(), &req)
	if err != nil {
		common.FeedResponse(c, &pb.ShowCCLResponse{Status: &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: fmt.Sprintf("ShowCCLHandler: %v", err),
		}}, inputEncodingType)
		return
	}
	common.FeedResponse(c, resp, inputEncodingType)
}

func (svc *IntraSvc) EngineCallbackHandler(c *gin.Context) {
	var req pb.ReportRequest
	_, err := message.DeserializeFrom(c.Request.Body, &req)
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
