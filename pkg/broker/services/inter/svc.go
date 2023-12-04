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

package inter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

var _ pb.InterBrokerServiceServer = &grpcInterSvc{}

type grpcInterSvc struct {
	app *application.App
}

func (svc *grpcInterSvc) SyncInfo(c context.Context, req *pb.SyncInfoRequest) (resp *pb.SyncInfoResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetChangeEntry() == nil {
		return nil, errors.New("SyncInfo illegal request")
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	action := req.GetChangeEntry().GetAction()
	switch action {
	case pb.ChangeEntry_AddProjectMember:
		// add row lock to avoid adding member concurrently
		proj, err := storage.AddExclusiveLock(txn).GetProject(req.GetProjectId())
		if err != nil {
			return nil, fmt.Errorf("SyncInfo: get project %v err: %v", req.GetProjectId(), err)
		}
		if req.GetClientId().GetCode() != proj.Creator {
			return nil, fmt.Errorf("SyncInfo AddProjectMember: project %v not owned by client %v", proj.ID, req.GetClientId().GetCode())
		}
		var member string
		err = json.Unmarshal(req.GetChangeEntry().GetData(), &member)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo AddProjectMember: unmarshal: %v", err)
		}
		err = txn.AddProjectMember(req.GetProjectId(), member)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo AddProjectMember: %v", err)
		}
	case pb.ChangeEntry_CreateTable:
		_, err = txn.GetProject(req.GetProjectId())
		if err != nil {
			return nil, fmt.Errorf("SyncInfo: get project %v err: %v", req.GetProjectId(), err)
		}
		var tableMeta storage.TableMeta
		err = json.Unmarshal(req.GetChangeEntry().GetData(), &tableMeta)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo CreateTable: %v", err)
		}
		// check table owner
		err = common.AddTableWithCheck(txn, req.GetProjectId(), req.GetClientId().GetCode(), tableMeta)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo CreateTable err: %v", err)
		}
	case pb.ChangeEntry_DropTable:
		var tableId storage.TableIdentifier
		err = json.Unmarshal(req.GetChangeEntry().GetData(), &tableId)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo DropTable: %v", err)
		}

		_, err = common.DropTableWithCheck(txn, req.GetProjectId(), req.GetClientId().GetCode(), tableId)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo DropTableWithCheck: %v", err)
		}
	case pb.ChangeEntry_GrantCCL:
		var privs []storage.ColumnPriv
		err = json.Unmarshal(req.GetChangeEntry().GetData(), &privs)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo GrantCCL: %v", err)
		}
		err = common.GrantColumnConstraintsWithCheck(txn, req.GetProjectId(), req.GetClientId().GetCode(), privs)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo GrantCCL GrantColumnConstraints err: %v", err)
		}
	case pb.ChangeEntry_RevokeCCL:
		var privIDs []storage.ColumnPrivIdentifier
		err = json.Unmarshal(req.GetChangeEntry().GetData(), &privIDs)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo RevokeCCL: %v", err)
		}

		err = common.RevokeColumnConstraintsWithCheck(txn, req.GetProjectId(), req.GetClientId().GetCode(), privIDs)
		if err != nil {
			return nil, fmt.Errorf("SyncInfo RevokeCCL: %v", err)
		}

	default:
		return nil, fmt.Errorf("SyncInfo not supported Action: %v", pb.ChangeEntry_Action_name[int32(action)])
	}

	return &pb.SyncInfoResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("sync info for project %v action %v succeed", req.GetProjectId(), pb.ChangeEntry_Action_name[int32(action)]),
		},
	}, nil
}

func (svc *grpcInterSvc) AskInfo(c context.Context, req *pb.AskInfoRequest) (resp *pb.AskInfoResponse, err error) {
	if req == nil || len(req.GetResourceSpecs()) == 0 {
		return nil, errors.New("AskInfo illegal request")
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	resp = &pb.AskInfoResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "ask info succeed",
		},
	}
	for _, resource := range req.GetResourceSpecs() {
		switch resource.Kind {
		case pb.ResourceSpec_Project:
			if resource.GetProjectId() == "" {
				return nil, fmt.Errorf("AskInfo Project: with empty project_id")
			}

			proj, err := txn.GetProject(resource.GetProjectId())
			if err != nil {
				return nil, fmt.Errorf("AskInfo Project: %v", err)
			}
			if proj.Creator != svc.app.Conf.PartyCode {
				return nil, fmt.Errorf("AskInfo Project: project %v not created by server", resource.GetProjectId())
			}

			projBytes, err := json.Marshal(proj)
			if err != nil {
				return nil, fmt.Errorf("AskInfo Project: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, projBytes)
		case pb.ResourceSpec_Table:
			if resource.GetProjectId() == "" || len(resource.GetTableNames()) == 0 {
				return nil, fmt.Errorf("AskInfo Table: empty project_id or table_name in %+v", resource)
			}
			tableMetas, err := txn.GetTablesByTableNames(resource.GetProjectId(), resource.GetTableNames())
			if err != nil {
				return nil, fmt.Errorf("AskInfo Table: get table failed: %v", err)
			}
			for _, meta := range tableMetas {
				if meta.Table.Owner != svc.app.Conf.PartyCode {
					return nil, fmt.Errorf("AskInfo Table: table %v not owned by server party %v", meta.Table.TableName, svc.app.Conf.PartyCode)
				}
			}

			tableBytes, err := json.Marshal(tableMetas)
			if err != nil {
				return nil, fmt.Errorf("AskInfo Table: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, tableBytes)

		case pb.ResourceSpec_CCL:
			if resource.GetProjectId() == "" || len(resource.GetTableNames()) == 0 {
				return nil, fmt.Errorf("AskInfo CCL: empty project_id or table_name in %+v", resource)
			}

			columnPrivs, err := txn.ListColumnConstraints(resource.GetProjectId(), resource.GetTableNames(), resource.GetDestParties())
			if err != nil {
				return nil, fmt.Errorf("AskInfo CCL: get privs failed: %v", err)
			}
			// TODO: Check privs owned by server

			privBytes, err := json.Marshal(columnPrivs)
			if err != nil {
				return nil, fmt.Errorf("AskInfo CCL: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, privBytes)

		default:
			return nil, fmt.Errorf("AskInfo illegal resource kind %v", resource.Kind)
		}
	}

	return resp, nil
}

func (svc *grpcInterSvc) ExchangeJobInfo(ctx context.Context, req *pb.ExchangeJobInfoRequest) (*pb.ExchangeJobInfoResponse, error) {
	if req == nil || req.GetProjectId() == "" || req.GetJobId() == "" {
		return nil, fmt.Errorf("ExchangeJobInfo illegal request: %+v", req)
	}
	s, exist := svc.app.GetSession(req.JobId)
	if !exist {
		return &pb.ExchangeJobInfoResponse{Status: &pb.Status{Code: int32(pb.Code_SESSION_NOT_FOUND), Message: "session not found"}}, nil
	}
	executionInfo := s.ExecuteInfo
	selfCode := s.GetSelfPartyCode()
	selfEndpoint, err := s.GetEndpoint(selfCode)
	if err != nil {
		return nil, err
	}
	statusCode := pb.Code_OK
	equalResult := pb.ChecksumCompareResult_EQUAL
	// get self checksum
	checksum, err := s.GetSelfChecksum()
	if err != nil {
		return &pb.ExchangeJobInfoResponse{Status: &pb.Status{Code: int32(pb.Code_SESSION_NOT_FOUND), Message: "checksum not ready"}}, nil
	}
	if slices.Contains(executionInfo.DataParties, selfCode) {
		statusCode = pb.Code_CHECKSUM_CHECK_FAILED
		equalResult = checksum.CompareWith(application.NewChecksumFromProto(req.ServerChecksum))
	}
	return &pb.ExchangeJobInfoResponse{
		Status:                 &pb.Status{Code: int32(statusCode)},
		Endpoint:               selfEndpoint,
		ServerChecksumResult:   equalResult,
		ExpectedServerChecksum: &pb.Checksum{TableSchema: checksum.TableSchema, Ccl: checksum.CCL},
	}, nil
}

type InterSvc struct {
	grpcInterSvc
}

func NewInterSvc(app *application.App) *InterSvc {
	return &InterSvc{
		grpcInterSvc{
			app: app,
		},
	}
}

func (svc *InterSvc) InviteToProjectHandler(c *gin.Context) {
	var req pb.InviteToProjectRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "InviteToProjectHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.InviteToProjectResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("InviteToProjectHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		response.Status.Message = fmt.Sprintf("InviteToProjectHandler: unable to check signature: %v", err)
		return
	}

	resp, err := svc.InviteToProject(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("InviteToProjectHandler: %v", err)
		return
	}

	response = resp
}

func (svc *InterSvc) DistributedQueryHandler(c *gin.Context) {
	var req pb.DistributeQueryRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "DistributedQueryHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.DistributeQueryResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("CreateProjectHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		response.Status.Message = fmt.Sprintf("DistributedQueryHandler: unable to check signature: %v", err)
		return
	}

	resp, err := svc.DistributeQuery(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("DistributedQueryHandler: %v", err)
		return
	}

	response = resp
}

func (svc *InterSvc) ReplyInvitationHandler(c *gin.Context) {
	var req pb.ReplyInvitationRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ReplyInvitationPathHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.ReplyInvitationResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("ReplyInvitationPathHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		c.String(http.StatusBadRequest, "ReplyInvitationPathHandler: unable to check signature: %v", err)
		return
	}

	resp, err := svc.ReplyInvitation(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("ReplyInvitationPathHandler: %v", err)
		return
	}

	response = resp
}

func (svc *InterSvc) SyncInfoHandler(c *gin.Context) {
	var req pb.SyncInfoRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "SyncInfoHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.SyncInfoResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("SyncInfoHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		c.String(http.StatusBadRequest, "SyncInfoHandler: unable to check signature: %v", err)
		return
	}

	resp, err := svc.SyncInfo(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("SyncInfoHandler: %v", err)
		return
	}

	response = resp
}

func (svc *InterSvc) AskInfoHandler(c *gin.Context) {
	var req pb.AskInfoRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "AskInfoHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.AskInfoResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("AskInfoHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		c.String(http.StatusBadRequest, "AskInfoHandler: unable to check signature: %v", err)
		return
	}

	resp, err := svc.AskInfo(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("AskInfoHandler: %v", err)
		return
	}

	response = resp
}

func (svc *InterSvc) ExchangeJobInfoHandler(c *gin.Context) {
	var req pb.ExchangeJobInfoRequest
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, &req)
	if err != nil {
		c.String(http.StatusBadRequest, "ExchangeJobInfoHandler: unable to parse request body: %v", err)
		return
	}

	response := &pb.ExchangeJobInfoResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL)}}
	defer func() {
		common.FeedResponse(c, response, inputEncodingType)
	}()
	// check signature with pubKey
	pubKey, err := svc.app.PartyMgr.GetPubKeyByParty(req.GetClientId().GetCode())
	if err != nil {
		response.Status.Message = fmt.Sprintf("ExchangeJobInfoHandler: %v", err)
		return
	}
	err = svc.app.Auth.CheckSign(&req, pubKey)
	if err != nil {
		response.Status.Code = int32(pb.Code_UNAUTHENTICATED)
		response.Status.Message = fmt.Sprintf("ExchangeJobInfoHandler: unable to check signature: %v", err)
		return
	}
	resp, err := svc.ExchangeJobInfo(c.Request.Context(), &req)
	if err != nil {
		response.Status.Message = fmt.Sprintf("ExchangeJobInfoHandler: %v", err)
		return
	}

	response = resp
}
