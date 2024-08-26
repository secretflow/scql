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

package intra_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/intra"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/brokerutil"
	"github.com/secretflow/scql/pkg/util/message"
)

type intraTestSuite struct {
	suite.Suite
	svcAlice       *intra.IntraSvc
	svcBob         *intra.IntraSvc
	svcCarol       *intra.IntraSvc
	ctx            context.Context
	testAppBuilder *brokerutil.TestAppBuilder
}

func TestServerSuit(t *testing.T) {
	suite.Run(t, new(intraTestSuite))
}

func (s *intraTestSuite) SetupSuite() {
	s.testAppBuilder = &brokerutil.TestAppBuilder{}
	s.NoError(s.testAppBuilder.BuildAppTests(s.Suite.T().TempDir()))
	s.svcAlice = intra.NewIntraSvc(s.testAppBuilder.AppAlice)
	s.svcBob = intra.NewIntraSvc(s.testAppBuilder.AppBob)
	s.svcCarol = intra.NewIntraSvc(s.testAppBuilder.AppCarol)
	s.ctx = context.Background()
	s.testAppBuilder.ServerAlice.Start()
	s.testAppBuilder.ServerBob.Start()
	s.testAppBuilder.ServerCarol.Start()
}

func (s *intraTestSuite) bootstrap(manager *storage.MetaManager) {
	txn := manager.CreateMetaTransaction()
	defer txn.Finish(nil)
	projConf, _ := message.ProtoMarshal(&pb.ProjectConfig{SpuRuntimeCfg: &spu.RuntimeConfig{Protocol: spu.ProtocolKind_SEMI2K, Field: spu.FieldType_FM64}})
	s.NoError(txn.CreateProject(storage.Project{ID: "1", Name: "test project", Creator: "alice", ProjectConf: string(projConf)}))
}

func (s *intraTestSuite) SetupTest() {
	s.NoError(s.testAppBuilder.AppAlice.MetaMgr.Bootstrap())
	s.bootstrap(s.testAppBuilder.AppAlice.MetaMgr)

	s.NoError(s.testAppBuilder.AppBob.MetaMgr.Bootstrap())

	s.NoError(s.testAppBuilder.AppCarol.MetaMgr.Bootstrap())
}

func (s *intraTestSuite) TearDownTest() {
	s.NoError(s.testAppBuilder.AppAlice.MetaMgr.DropTables())
	s.testAppBuilder.AppAlice.Sessions.Flush()

	s.NoError(s.testAppBuilder.AppBob.MetaMgr.DropTables())
	s.testAppBuilder.AppBob.Sessions.Flush()

	s.NoError(s.testAppBuilder.AppCarol.MetaMgr.DropTables())
	s.testAppBuilder.AppCarol.Sessions.Flush()
}

func (s *intraTestSuite) TestProcessInvitationNormal() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverBob := s.testAppBuilder.ServerBob
	inviteReq := &pb.InviteMemberRequest{
		ProjectId: "1",
		Invitee:   "bob",
	}
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.ReplyInvitationPath {
			var req pb.ReplyInvitationRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			resp := &pb.ReplyInvitationResponse{
				Status:      &pb.Status{Code: 0},
				ProjectInfo: []byte{},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	serverBob.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.InviteToProjectPath {
			var req pb.InviteToProjectRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			txn := s.testAppBuilder.AppBob.MetaMgr.CreateMetaTransaction()
			defer txn.Finish(nil)
			projectConf, err := message.ProtoMarshal(&pb.ProjectConfig{
				SpuRuntimeCfg:        req.GetProject().GetConf().GetSpuRuntimeCfg(),
				SessionExpireSeconds: req.GetProject().GetConf().SessionExpireSeconds,
			})
			s.NoError(err)
			s.NoError(txn.AddInvitations([]storage.Invitation{storage.Invitation{
				ProjectID:   req.GetProject().GetProjectId(),
				Name:        req.GetProject().GetName(),
				Description: req.GetProject().GetDescription(),
				Creator:     req.GetProject().GetCreator(),
				Member:      strings.Join(req.GetProject().GetMembers(), ";"),
				ProjectConf: string(projectConf),
				Inviter:     req.GetInviter(),
				Invitee:     "bob",
				InviteTime:  time.Now(),
			}}))
			resp := &pb.InviteToProjectResponse{
				Status: &pb.Status{Code: 0},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	res, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	s.Equal(int32(pb.Code_OK), res.Status.Code)
	processReq := &pb.ProcessInvitationRequest{
		InvitationId: 1,
		Respond:      pb.InvitationRespond_ACCEPT,
	}
	result, err := s.svcBob.ProcessInvitation(s.ctx, processReq)
	s.NoError(err)
	s.Equal(int32(pb.Code_OK), result.Status.Code)
	invitationsRes, err := s.svcBob.ListInvitations(s.ctx, &pb.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(invitationsRes.Invitations))
	s.Equal(pb.InvitationStatus_ACCEPTED, invitationsRes.Invitations[0].Status)
}

func (s *intraTestSuite) TestListInvitations() {
	txn := s.testAppBuilder.AppBob.MetaMgr.CreateMetaTransaction()
	projConf, _ := message.ProtoMarshal(&pb.ProjectConfig{
		SpuRuntimeCfg: &spu.RuntimeConfig{
			Protocol: spu.ProtocolKind_SEMI2K,
			Field:    spu.FieldType_FM64,
		},
	})
	s.NoError(txn.AddInvitations([]storage.Invitation{
		{
			ProjectID:   "mock_id1",
			Name:        "mock_name1",
			ProjectConf: string(projConf),
			Creator:     "alice",
			Member:      "alice;carol",
			Inviter:     "alice",
			Invitee:     "bob",
			InviteTime:  time.Now(),
		},
		{
			ProjectID:   "mock_id2",
			Name:        "mock_name2",
			ProjectConf: string(projConf),
			Creator:     "carol",
			Member:      "alice;carol",
			Inviter:     "carol",
			Invitee:     "bob",
			InviteTime:  time.Now(),
		},
		{
			ProjectID:   "mock_id3",
			Name:        "mock_name3",
			ProjectConf: string(projConf),
			Creator:     "alice",
			Member:      "alice;carol",
			Inviter:     "alice",
			Invitee:     "bob",
			Status:      int8(pb.InvitationStatus_INVALID),
			InviteTime:  time.Now(),
		},
	}))
	txn.Finish(nil)
	// inviter: alice
	invitationsRes, err := s.svcBob.ListInvitations(s.ctx, &pb.ListInvitationsRequest{Filter: &pb.ListInvitationsRequest_Inviter{Inviter: "alice"}})
	s.NoError(err)
	s.Equal(2, len(invitationsRes.Invitations))
	// status: undecided
	invitationsRes, err = s.svcBob.ListInvitations(s.ctx, &pb.ListInvitationsRequest{Filter: &pb.ListInvitationsRequest_Status{Status: pb.InvitationStatus_UNDECIDED}})
	s.NoError(err)
	s.Equal(2, len(invitationsRes.Invitations))
	// status: invalid
	invitationsRes, err = s.svcBob.ListInvitations(s.ctx, &pb.ListInvitationsRequest{Filter: &pb.ListInvitationsRequest_Status{Status: pb.InvitationStatus_INVALID}})
	s.NoError(err)
	s.Equal(1, len(invitationsRes.Invitations))
}

func (s *intraTestSuite) TestProcessInvitationError() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverBob := s.testAppBuilder.ServerBob
	inviteReq := &pb.InviteMemberRequest{
		ProjectId: "1",
		Invitee:   "bob",
	}
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.ReplyInvitationPath {
			var req pb.ReplyInvitationRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			resp := &pb.ReplyInvitationResponse{
				Status:      &pb.Status{Code: int32(pb.Code_DATA_INCONSISTENCY)},
				ProjectInfo: []byte{},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	serverBob.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.InviteToProjectPath {
			var req pb.InviteToProjectRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			txn := s.testAppBuilder.AppBob.MetaMgr.CreateMetaTransaction()
			defer txn.Finish(nil)
			s.NoError(err)
			projConf, _ := message.ProtoMarshal(&pb.ProjectConfig{
				SpuRuntimeCfg:        req.Project.Conf.SpuRuntimeCfg,
				SessionExpireSeconds: req.Project.Conf.SessionExpireSeconds,
			})
			s.NoError(err)
			s.NoError(txn.AddInvitations([]storage.Invitation{storage.Invitation{
				ProjectID:   req.GetProject().GetProjectId(),
				Name:        req.GetProject().GetName(),
				Description: req.GetProject().GetDescription(),
				Creator:     req.GetProject().GetCreator(),
				Member:      strings.Join(req.GetProject().GetMembers(), ";"),
				ProjectConf: string(projConf),
				Inviter:     req.GetInviter(),
				Invitee:     "bob",
				InviteTime:  time.Now(),
			}}))
			resp := &pb.InviteToProjectResponse{
				Status: &pb.Status{Code: 0},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	res, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	s.Equal(int32(pb.Code_OK), res.Status.Code)
	processReq := &pb.ProcessInvitationRequest{
		InvitationId: 1,
		Respond:      pb.InvitationRespond_ACCEPT,
	}
	_, err = s.svcBob.ProcessInvitation(s.ctx, processReq)
	s.Error(err)
	s.Equal("ProcessInvitation: failed to reply invitation due to invitation 1 is not same with project", err.Error())
	invitationsRes, err := s.svcBob.ListInvitations(s.ctx, &pb.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(invitationsRes.Invitations))
	s.Equal(pb.InvitationStatus_INVALID, invitationsRes.Invitations[0].Status)
	projects, err := s.svcBob.ListProjects(s.ctx, &pb.ListProjectsRequest{})
	s.NoError(err)
	s.Equal(0, len(projects.Projects))
}

func (s *intraTestSuite) TestParameterCheck() {
	inviteReq := &pb.InviteMemberRequest{ProjectId: "not_exist", Invitee: "carol"}
	_, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.Error(err)
	inviteReq = &pb.InviteMemberRequest{ProjectId: "1", Invitee: "carol"}
	_, err = s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	listResponse, err := s.svcAlice.ListInvitations(s.ctx, &pb.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(listResponse.GetInvitations()))
	processInvitationReq := &pb.ProcessInvitationRequest{InvitationId: listResponse.GetInvitations()[0].GetInvitationId(), Respond: 2}
	_, err = s.svcAlice.ProcessInvitation(s.ctx, processInvitationReq)
	s.Error(err)
	s.Equal("Error: code=100, msg=\"ProcessInvitation: not support respond type: 2\"", err.Error())
	listProjectReq := &pb.ListProjectsRequest{Ids: []string{"1", "not_exist"}}
	_, err = s.svcAlice.ListProjects(s.ctx, listProjectReq)
	s.Error(err)
	listTablesReq := &pb.ListTablesRequest{ProjectId: "not_exist", Names: []string{}}
	_, err = s.svcAlice.ListTables(s.ctx, listTablesReq)
	s.Error(err)
	listTablesReq = &pb.ListTablesRequest{ProjectId: "1", Names: []string{"not_exist_table"}}
	_, err = s.svcAlice.ListTables(s.ctx, listTablesReq)
	s.Error(err)
	listCCLsReq := &pb.ShowCCLRequest{ProjectId: "not_exist"}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("ShowCCL: GetProjectAndMembers err: record not found", err.Error())
	listCCLsReq = &pb.ShowCCLRequest{ProjectId: "1", Tables: []string{"not_exist_table"}}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("ShowCCL: tables [not_exist_table] not all exist", err.Error())
	listCCLsReq = &pb.ShowCCLRequest{ProjectId: "1", Tables: []string{}, DestParties: []string{"not_exist_party"}}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("ShowCCL: dest parties [not_exist_party] not found in project members", err.Error())
	grantCCLReq := &pb.GrantCCLRequest{ProjectId: "1", ColumnControlList: []*pb.ColumnControl{&pb.ColumnControl{Col: &pb.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 1}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("GrantCCL: GrantColumnConstraintsWithCheck: table [not_exist_table] not found", err.Error())
	grantCCLReq = &pb.GrantCCLRequest{ProjectId: "1", ColumnControlList: []*pb.ColumnControl{&pb.ColumnControl{Col: &pb.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 10}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("GrantCCL: ColumnControlList2ColumnPriv: illegal constraint: 10", err.Error())
	grantCCLReq = &pb.GrantCCLRequest{ProjectId: "not_exist", ColumnControlList: []*pb.ColumnControl{&pb.ColumnControl{Col: &pb.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 10}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("GrantCCL: project not_exist has no members or project doesn't exist", err.Error())
	revokeCCLReq := &pb.RevokeCCLRequest{ProjectId: "1", ColumnControlList: []*pb.ColumnControl{&pb.ColumnControl{Col: &pb.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice"}}}
	_, err = s.svcAlice.RevokeCCL(s.ctx, revokeCCLReq)
	s.Error(err)
	s.Equal("RevokeCCL: RevokeColumnConstraintsWithCheck: table [not_exist_table] not found", err.Error())
}

func (s *intraTestSuite) TearDownSuite() {
	s.testAppBuilder.ServerAlice.Close()
	s.testAppBuilder.ServerBob.Close()
	s.testAppBuilder.ServerCarol.Close()
	s.testAppBuilder.ServerEngine.Close()
}

func (s *intraTestSuite) TestCheckAndUpdateStatusNormal() {
	tb := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{TableName: "tb", ProjectID: "3"},
			RefTable:        "physic.tb",
			DBType:          "MYSQL",
			Owner:           "bob",
		},
		Columns: []storage.ColumnMeta{{ColumnName: "id", DType: "int"}},
	}
	{
		txn := s.testAppBuilder.AppAlice.MetaMgr.CreateMetaTransaction()
		s.NoError(txn.CreateProject(storage.Project{ID: "2", Name: "test project creator conflict", Creator: "bob"}))
		s.NoError(txn.CreateProject(storage.Project{ID: "3", Name: "test status update normally", Creator: "bob"}))
		s.NoError(txn.AddTable(tb))
		txn.Finish(nil)
	}
	priv := storage.ColumnPriv{
		ColumnPrivIdentifier: storage.ColumnPrivIdentifier{
			ProjectID:  "3",
			TableName:  "tb2",
			ColumnName: "col1",
			DestParty:  "alice",
		},
		Priv: "PLAINTEXT_AFTER_JOIN",
	}
	serverBob := s.testAppBuilder.ServerBob
	serverBob.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.AskInfoPath {
			var req pb.AskInfoRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req, r.Header.Get("Content-Type"))
			s.NoError(err)
			s.Equal(1, len(req.GetResourceSpecs()))
			s.Equal(pb.ResourceSpec_All, req.GetResourceSpecs()[0].GetKind())
			var status storage.ProjectMeta
			if req.GetResourceSpecs()[0].GetProjectId() == "2" {
				status = storage.ProjectMeta{
					Proj: storage.ProjectWithMember{
						Proj: storage.Project{
							ID:      "2",
							Creator: "alice",
						},
					},
				}
			} else if req.GetResourceSpecs()[0].GetProjectId() == "3" {
				tb.Table.CreatedAt = time.Now()
				tb.Columns = append(tb.Columns, storage.ColumnMeta{ColumnName: "name", DType: "string"})
				status = storage.ProjectMeta{
					Proj: storage.ProjectWithMember{
						Proj: storage.Project{
							ID:      "3",
							Name:    "test status update normally",
							Creator: "bob",
						},
						Members: []string{"alice", "bob"},
					},
					Tables: []storage.TableMeta{
						tb,
						{
							Table: storage.Table{
								TableIdentifier: storage.TableIdentifier{ProjectID: "3", TableName: "tb2"},
								Owner:           "bob",
								RefTable:        "physic.tb2",
							},
							Columns: []storage.ColumnMeta{
								{
									ColumnName: "col1",
									DType:      "long",
								},
							},
						}},
					CCLs: []storage.ColumnPriv{priv},
				}
			}
			statusBytes, err := json.Marshal(status)
			s.NoError(err)
			resp := &pb.AskInfoResponse{
				Status: &pb.Status{Code: 0},
				Datas:  [][]byte{statusBytes},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	req := pb.CheckAndUpdateStatusRequest{
		ProjectIds: []string{"2"},
	}
	res, err := s.svcAlice.CheckAndUpdateStatus(s.ctx, &req)
	s.NoError(err)
	s.Equal(int32(pb.Code_PROJECT_CONFLICT), res.Status.Code, protojson.Format(res))
	// check update normally
	req = pb.CheckAndUpdateStatusRequest{
		ProjectIds: []string{"3"},
	}
	res, err = s.svcAlice.CheckAndUpdateStatus(s.ctx, &req)
	s.NoError(err)
	s.Equal(int32(pb.Code_OK), res.Status.Code, protojson.Format(res))
	txn := s.testAppBuilder.AppAlice.MetaMgr.CreateMetaTransaction()
	defer txn.Finish(nil)
	tables, _, err := txn.GetTableMetasByTableNames("3", nil)
	s.NoError(err)
	s.Equal(2, len(tables), fmt.Sprintf("tables: %v", tables))
	s.Equal(2, len(tables[0].Columns))
	s.Equal("tb2", tables[1].Table.TableName)
	s.Equal(1, len(tables[1].Columns))
	s.Equal("col1", tables[1].Columns[0].ColumnName)
	s.Equal("long", tables[1].Columns[0].DType)
	ccls, err := txn.ListColumnConstraints("3", []string{}, []string{})
	s.NoError(err)
	s.Equal(1, len(ccls), fmt.Sprintf("tables: %v", ccls))
	s.Equal(priv.ColumnPrivIdentifier, ccls[0].ColumnPrivIdentifier)
	s.Equal(priv.DestParty, ccls[0].DestParty)
}
