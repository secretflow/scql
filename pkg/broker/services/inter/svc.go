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
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

var _ pb.InterBrokerServiceServer = &grpcInterSvc{}

type grpcInterSvc struct {
	app *application.App
}

func (svc *grpcInterSvc) SyncInfo(c context.Context, req *pb.SyncInfoRequest) (resp *pb.SyncInfoResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetChangeEntry() == nil {
		return nil, status.New(pb.Code_BAD_REQUEST, "SyncInfo illegal request")
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	action := req.GetChangeEntry().GetAction()
	switch action {
	case pb.ChangeEntry_AddProjectMember:
		proj, err := storage.AddShareLock(txn).GetProject(req.GetProjectId())
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
		err = txn.AddProjectMembers([]storage.Member{{ProjectID: req.GetProjectId(), Member: member}})
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
		err = common.GrantColumnConstraintsWithCheck(txn, req.GetProjectId(), privs, common.OwnerChecker{Owner: req.GetClientId().GetCode()})
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
		return nil, fmt.Errorf("SyncInfo not supported Action: %v", action.String())
	}

	return &pb.SyncInfoResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("sync info for project %v action %v succeed", req.GetProjectId(), action.String()),
		},
	}, nil
}

func (svc *grpcInterSvc) AskInfo(c context.Context, req *pb.AskInfoRequest) (resp *pb.AskInfoResponse, err error) {
	if req == nil || len(req.GetResourceSpecs()) == 0 {
		return nil, status.New(pb.Code_BAD_REQUEST, "AskInfo illegal request")
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
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

			projectAndMembers, err := txn.GetProjectAndMembers(resource.GetProjectId())
			if err != nil {
				return nil, fmt.Errorf("AskInfo Project: %v", err)
			}
			if projectAndMembers.Proj.Creator != svc.app.Conf.PartyCode {
				return nil, fmt.Errorf("AskInfo Project: project %v not created by server", resource.GetProjectId())
			}
			data, err := json.Marshal(projectAndMembers)
			if err != nil {
				return nil, fmt.Errorf("AskInfo Project: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, data)
		case pb.ResourceSpec_Table:
			if resource.GetProjectId() == "" || len(resource.GetTableNames()) == 0 {
				return nil, fmt.Errorf("AskInfo Table: empty project_id or table_name in %+v", resource)
			}
			tableMetas, _, err := txn.GetTableMetasByTableNames(resource.GetProjectId(), resource.GetTableNames())
			if err != nil {
				return nil, fmt.Errorf("AskInfo Table: get table failed: %v", err)
			}
			for _, meta := range tableMetas {
				if meta.Table.Owner != svc.app.Conf.PartyCode {
					return nil, fmt.Errorf("AskInfo Table: table %v not owned by server party %v", meta.Table.TableName, svc.app.Conf.PartyCode)
				}
			}

			data, err := json.Marshal(tableMetas)
			if err != nil {
				return nil, fmt.Errorf("AskInfo Table: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, data)
		case pb.ResourceSpec_CCL:
			if resource.GetProjectId() == "" || len(resource.GetTableNames()) == 0 {
				return nil, fmt.Errorf("AskInfo CCL: empty project_id or table_name in %+v", resource)
			}

			columnPrivs, err := txn.ListColumnConstraints(resource.GetProjectId(), resource.GetTableNames(), resource.GetDestParties())
			if err != nil {
				return nil, fmt.Errorf("AskInfo CCL: get privs failed: %v", err)
			}
			// TODO: Check privs owned by server

			data, err := json.Marshal(columnPrivs)
			if err != nil {
				return nil, fmt.Errorf("AskInfo CCL: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, data)
		case pb.ResourceSpec_All:
			if resource.GetProjectId() == "" {
				return nil, fmt.Errorf("AskInfo All: empty project_id in %+v", resource)
			}
			// TODO(xiaoyuan): add project info later
			projectMeta, err := txn.GetProjectMeta(resource.GetProjectId(), resource.GetTableNames(), resource.GetDestParties(), svc.app.Conf.PartyCode)
			if err != nil {
				return nil, fmt.Errorf("AskInfo All: get project meta failed: %v", err)
			}
			data, err := json.Marshal(projectMeta)
			if err != nil {
				return nil, fmt.Errorf("AskInfo All: marshal err: %v", err)
			}
			resp.Datas = append(resp.Datas, data)
		default:
			return nil, fmt.Errorf("AskInfo illegal resource kind %v", resource.Kind)
		}
	}

	return resp, nil
}

func (svc *grpcInterSvc) ExchangeJobInfo(ctx context.Context, req *pb.ExchangeJobInfoRequest) (resp *pb.ExchangeJobInfoResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetJobId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("ExchangeJobInfo illegal request: %+v", req))
	}

	info, err := svc.app.GetSessionInfo(req.GetJobId())
	if err != nil {
		return &pb.ExchangeJobInfoResponse{Status: &pb.Status{Code: int32(pb.Code_SESSION_NOT_FOUND), Message: fmt.Sprintf("get session info failed: %s", err.Error())}}, nil
	}
	selfEndpoint := info.EngineUrl
	checksum := application.Checksum{
		TableSchema: []byte(info.TableChecksum),
		CCL:         []byte(info.CCLChecksum),
	}

	statusCode := pb.Code_OK
	equalResult := checksum.CompareWith(application.NewChecksumFromProto(req.ServerChecksum))
	if equalResult != pb.ChecksumCompareResult_EQUAL {
		statusCode = pb.Code_DATA_INCONSISTENCY
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
	handlerWrapper(c, svc.app,
		&pb.InviteToProjectRequest{},
		&pb.InviteToProjectResponse{},
		func(ctx context.Context, req *pb.InviteToProjectRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.InviteToProjectResponse, error) {
			logEntry.RequestParty = req.GetInvitationCode()
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "InviteToProject")
			return svc.InviteToProject(ctx, req)
		})
}

func (svc *InterSvc) DistributedQueryHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.DistributeQueryRequest{},
		&pb.DistributeQueryResponse{},
		func(ctx context.Context, req *pb.DistributeQueryRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.DistributeQueryResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.JobID = req.GetJobId()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "DistributeQuery")
			return svc.DistributeQuery(ctx, req)
		})
}

func (svc *InterSvc) CancelQueryJobHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.CancelQueryJobRequest{},
		&pb.CancelQueryJobResponse{},
		func(ctx context.Context, req *pb.CancelQueryJobRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.CancelQueryJobResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.JobID = req.GetJobId()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "CancelQueryJob")
			return svc.CancelQueryJob(ctx, req)
		})
}

func (svc *InterSvc) ReplyInvitationHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.ReplyInvitationRequest{},
		&pb.ReplyInvitationResponse{},
		func(ctx context.Context, req *pb.ReplyInvitationRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ReplyInvitationResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "ReplyInvitation")
			return svc.ReplyInvitation(ctx, req)
		})
}

func (svc *InterSvc) SyncInfoHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.SyncInfoRequest{},
		&pb.SyncInfoResponse{},
		func(ctx context.Context, req *pb.SyncInfoRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.SyncInfoResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "SyncInfo")
			return svc.SyncInfo(ctx, req)
		})
}

func (svc *InterSvc) AskInfoHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.AskInfoRequest{},
		&pb.AskInfoResponse{},
		func(ctx context.Context, req *pb.AskInfoRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.AskInfoResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "AskInfo")
			return svc.AskInfo(ctx, req)
		})
}

func (svc *InterSvc) ExchangeJobInfoHandler(c *gin.Context) {
	handlerWrapper(c, svc.app,
		&pb.ExchangeJobInfoRequest{},
		&pb.ExchangeJobInfoResponse{},
		func(ctx context.Context, req *pb.ExchangeJobInfoRequest, logEntry *logutil.BrokerMonitorLogEntry) (*pb.ExchangeJobInfoResponse, error) {
			logEntry.RequestParty = req.ClientId.GetCode()
			logEntry.RawRequest = req.String()
			logEntry.ActionName = fmt.Sprintf("%v@%v", "Inter", "ExchangeJobInfo")
			return svc.ExchangeJobInfo(ctx, req)
		})
}

func handlerWrapper[In, Out protoreflect.ProtoMessage](
	c *gin.Context,
	app *application.App,
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
	code, err := getClientCodeFrom(req)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to get client code: %+v", err)
		return
	}
	// check signature with pubKey
	pubKey, err := app.PartyMgr.GetPubKeyByParty(code)
	if err != nil {
		err = status.New(pb.Code_UNAUTHENTICATED, err.Error())
		return
	}
	err = app.Auth.CheckSign(req, pubKey)
	if err != nil {
		err = status.New(pb.Code_UNAUTHENTICATED, err.Error())
		return
	}
	response, err := fn(c.Request.Context(), req, logEntry)
	if err != nil {
		return
	}
	resp = response
}

func getClientCodeFrom(req protoreflect.ProtoMessage) (string, error) {
	clientDesc := req.ProtoReflect().Descriptor().Fields().ByJSONName("clientId")
	if clientDesc == nil {
		return "", fmt.Errorf("failed to get client id")
	}
	clientId := req.ProtoReflect().Get(clientDesc).Message()
	codeDes := clientId.Descriptor().Fields().ByJSONName("code")
	if codeDes == nil {
		return "", fmt.Errorf("failed to get client code")
	}
	return clientId.Get(codeDes).String(), nil
}
