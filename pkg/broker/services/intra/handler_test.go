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
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/intra"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
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
	s.NoError(s.testAppBuilder.BuildAppTests())
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
	s.NoError(txn.CreateProject(storage.Project{ID: "1", Name: "test project", Creator: "alice", ProjectConf: storage.ProjectConfig{SpuConf: `{"protocol": "SEMI2K","field": "FM64"}`}}))
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
	inviteReq := &scql.InviteMemberRequest{
		ProjectId: "1",
		Invitee:   "bob",
	}
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.ReplyInvitationPath {
			var req scql.ReplyInvitationRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req)
			s.NoError(err)
			resp := &scql.ReplyInvitationResponse{
				Status:      &scql.Status{Code: 0},
				ProjectInfo: []byte{},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	serverBob.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.InviteToProjectPath {
			var req scql.InviteToProjectRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req)
			s.NoError(err)
			txn := s.testAppBuilder.AppBob.MetaMgr.CreateMetaTransaction()
			defer txn.Finish(nil)
			spuCfg, err := protojson.Marshal(req.GetProject().GetConf().GetSpuRuntimeCfg())
			s.NoError(err)
			s.NoError(txn.AddInvitations([]storage.Invitation{storage.Invitation{
				ProjectID:   req.GetProject().GetProjectId(),
				Name:        req.GetProject().GetName(),
				Description: req.GetProject().GetDescription(),
				Creator:     req.GetProject().GetCreator(),
				Member:      strings.Join(req.GetProject().GetMembers(), ";"),
				ProjectConf: storage.ProjectConfig{
					SpuConf: string(spuCfg),
				},
				Inviter:    req.GetInviter(),
				Invitee:    "bob",
				InviteTime: time.Now(),
			}}))
			resp := &scql.InviteToProjectResponse{
				Status: &scql.Status{Code: 0},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	res, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	s.Equal(int32(scql.Code_OK), res.Status.Code)
	processReq := &scql.ProcessInvitationRequest{
		InvitationId: 1,
		Respond:      scql.InvitationRespond_ACCEPT,
	}
	result, err := s.svcBob.ProcessInvitation(s.ctx, processReq)
	s.NoError(err)
	s.Equal(int32(scql.Code_OK), result.Status.Code)
	invitationsRes, err := s.svcBob.ListInvitations(s.ctx, &scql.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(invitationsRes.Invitations))
	s.Equal(scql.InvitationStatus_ACCEPTED, invitationsRes.Invitations[0].Status)
}

func (s *intraTestSuite) TestProcessInvitationError() {
	serverAlice := s.testAppBuilder.ServerAlice
	serverBob := s.testAppBuilder.ServerBob
	inviteReq := &scql.InviteMemberRequest{
		ProjectId: "1",
		Invitee:   "bob",
	}
	serverAlice.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.ReplyInvitationPath {
			var req scql.ReplyInvitationRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req)
			s.NoError(err)
			resp := &scql.ReplyInvitationResponse{
				Status:      &scql.Status{Code: int32(pb.Code_DATA_INCONSISTENCY)},
				ProjectInfo: []byte{},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	serverBob.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.InviteToProjectPath {
			var req scql.InviteToProjectRequest
			inputEncodingType, err := message.DeserializeFrom(r.Body, &req)
			s.NoError(err)
			txn := s.testAppBuilder.AppBob.MetaMgr.CreateMetaTransaction()
			defer txn.Finish(nil)
			spuCfg, err := protojson.Marshal(req.GetProject().GetConf().GetSpuRuntimeCfg())
			s.NoError(err)
			s.NoError(txn.AddInvitations([]storage.Invitation{storage.Invitation{
				ProjectID:   req.GetProject().GetProjectId(),
				Name:        req.GetProject().GetName(),
				Description: req.GetProject().GetDescription(),
				Creator:     req.GetProject().GetCreator(),
				Member:      strings.Join(req.GetProject().GetMembers(), ";"),
				ProjectConf: storage.ProjectConfig{
					SpuConf: string(spuCfg),
				},
				Inviter:    req.GetInviter(),
				Invitee:    "bob",
				InviteTime: time.Now(),
			}}))
			resp := &scql.InviteToProjectResponse{
				Status: &scql.Status{Code: 0},
			}
			body, _ := message.SerializeTo(resp, inputEncodingType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})
	res, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	s.Equal(int32(scql.Code_OK), res.Status.Code)
	processReq := &scql.ProcessInvitationRequest{
		InvitationId: 1,
		Respond:      scql.InvitationRespond_ACCEPT,
	}
	_, err = s.svcBob.ProcessInvitation(s.ctx, processReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"ProcessInvitation: failed to reply invitation due to invitation 1 is not same with project\"", err.Error())
	invitationsRes, err := s.svcBob.ListInvitations(s.ctx, &scql.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(invitationsRes.Invitations))
	s.Equal(pb.InvitationStatus_INVALID, invitationsRes.Invitations[0].Status)
	projects, err := s.svcBob.ListProjects(s.ctx, &scql.ListProjectsRequest{})
	s.NoError(err)
	s.Equal(0, len(projects.Projects))
}

func (s *intraTestSuite) TestParameterCheck() {
	inviteReq := &scql.InviteMemberRequest{ProjectId: "not_exist", Invitee: "carol"}
	_, err := s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.Error(err)
	inviteReq = &scql.InviteMemberRequest{ProjectId: "1", Invitee: "carol"}
	_, err = s.svcAlice.InviteMember(s.ctx, inviteReq)
	s.NoError(err)
	listResponse, err := s.svcAlice.ListInvitations(s.ctx, &scql.ListInvitationsRequest{})
	s.NoError(err)
	s.Equal(1, len(listResponse.GetInvitations()))
	processInvitationReq := &scql.ProcessInvitationRequest{InvitationId: listResponse.GetInvitations()[0].GetInvitationId(), Respond: 2}
	_, err = s.svcAlice.ProcessInvitation(s.ctx, processInvitationReq)
	s.Error(err)
	s.Equal("Error: code=100, msg=\"ProcessInvitation: not support respond type: 2\"", err.Error())
	listProjectReq := &scql.ListProjectsRequest{Ids: []string{"1", "not_exist"}}
	_, err = s.svcAlice.ListProjects(s.ctx, listProjectReq)
	s.Error(err)
	listTablesReq := &scql.ListTablesRequest{ProjectId: "not_exist", Names: []string{}}
	_, err = s.svcAlice.ListTables(s.ctx, listTablesReq)
	s.Error(err)
	listTablesReq = &scql.ListTablesRequest{ProjectId: "1", Names: []string{"not_exist_table"}}
	_, err = s.svcAlice.ListTables(s.ctx, listTablesReq)
	s.Error(err)
	listCCLsReq := &scql.ShowCCLRequest{ProjectId: "not_exist"}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"ShowCCL: GetProjectAndMembers err: record not found\"", err.Error())
	listCCLsReq = &scql.ShowCCLRequest{ProjectId: "1", Tables: []string{"not_exist_table"}}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"ShowCCL: tables [not_exist_table] not all exist\"", err.Error())
	listCCLsReq = &scql.ShowCCLRequest{ProjectId: "1", Tables: []string{}, DestParties: []string{"not_exist_party"}}
	_, err = s.svcAlice.ShowCCL(s.ctx, listCCLsReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"ShowCCL: dest parties [not_exist_party] not found in project members\"", err.Error())
	grantCCLReq := &scql.GrantCCLRequest{ProjectId: "1", ColumnControlList: []*scql.ColumnControl{&scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 1}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"GrantCCL: GrantColumnConstraintsWithCheck: table not_exist_table not found\"", err.Error())
	grantCCLReq = &scql.GrantCCLRequest{ProjectId: "1", ColumnControlList: []*scql.ColumnControl{&scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 10}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"GrantCCL: ColumnControlList2ColumnPriv: illegal constraint: 10\"", err.Error())
	grantCCLReq = &scql.GrantCCLRequest{ProjectId: "not_exist", ColumnControlList: []*scql.ColumnControl{&scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice", Constraint: 10}}}
	_, err = s.svcAlice.GrantCCL(s.ctx, grantCCLReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"GrantCCL: project not_exist has no members or project doesn't exist\"", err.Error())
	revokeCCLReq := &scql.RevokeCCLRequest{ProjectId: "1", ColumnControlList: []*scql.ColumnControl{&scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: "not_exist_col", TableName: "not_exist_table"}, PartyCode: "alice"}}}
	_, err = s.svcAlice.RevokeCCL(s.ctx, revokeCCLReq)
	s.Error(err)
	s.Equal("Error: code=300, msg=\"RevokeCCL: RevokeColumnConstraintsWithCheck: table not_exist_table not found\"", err.Error())
}

func (s *intraTestSuite) TearDownSuite() {
	s.testAppBuilder.ServerAlice.Close()
	s.testAppBuilder.ServerBob.Close()
	s.testAppBuilder.ServerCarol.Close()
	s.testAppBuilder.ServerEngine.Close()
	os.Remove(s.testAppBuilder.PartyInfoTmpPath)
	for _, path := range s.testAppBuilder.PemFilePaths {
		os.Remove(path)
	}
}
