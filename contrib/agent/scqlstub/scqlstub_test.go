// Copyright 2024 Ant Group Co., Ltd.
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

package scqlstub

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/secretflow/kuscia/proto/api/v1alpha1/appconfig"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"

	taskconfig "github.com/secretflow/scql/contrib/agent/proto"
	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

type ScqlStubTestSuit struct {
	suite.Suite
	stub   *ScqlStub
	server *httptest.Server
}

func (suite *ScqlStubTestSuit) SetupTest() {
	suite.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	tc := &taskconfig.ScqlTaskInputConfig{
		ProjectId: "project_id",
		Query:     "SELECT id FROM ta WHERE age > 18;",
		OutputIds: map[string]string{
			"alice": "alice_output",
		},
		Tables: map[string]*taskconfig.TableList{
			"alice": {
				Tbls: []*scql.TableMeta{
					{
						TableName:  "ta",
						RefTable:   "domainDataId1",
						DbType:     "csvdb",
						TableOwner: "alice",
						Columns: []*scql.TableMeta_Column{
							{
								Name:  "id",
								Dtype: "string",
							},
							{
								Name:  "age",
								Dtype: "int",
							},
						},
					},
				},
			},
			"bob": {
				Tbls: []*scql.TableMeta{
					{
						TableName:  "tb",
						RefTable:   "domainDataId2",
						DbType:     "csvdb",
						TableOwner: "bob",
						Columns: []*scql.TableMeta_Column{
							{
								Name:  "id",
								Dtype: "string",
							},
							{
								Name:  "income",
								Dtype: "int",
							},
						},
					},
				},
			},
		},
		Ccls: map[string]*scql.PrivacyPolicy{
			"alice": {
				ColumnControlList: []*scql.ColumnControl{
					{
						Col: &scql.ColumnDef{
							ColumnName: "id",
							TableName:  "ta",
						},
						PartyCode:  "alice",
						Constraint: scql.Constraint_PLAINTEXT,
					},
					{
						Col: &scql.ColumnDef{
							ColumnName: "id",
							TableName:  "ta",
						},
						PartyCode:  "bob",
						Constraint: scql.Constraint_PLAINTEXT_AFTER_JOIN,
					},
				},
			},
		},
	}
	str, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(tc)
	suite.Nil(err)
	logrus.Warn(string(str))

	suite.stub = &ScqlStub{
		stub:      &application.IntraStub{Timeout: 100 * time.Millisecond},
		url:       "http://" + suite.server.Listener.Addr().String(),
		jobId:     "job_id",
		selfParty: "alice",
		clusterDefine: &appconfig.ClusterDefine{
			SelfPartyIdx: 0,
			Parties: []*appconfig.Party{
				{Name: "alice"},
				{Name: "bob"},
			},
		},
		waitTimeout:      2500 * time.Millisecond,
		waitQueryTimeout: 2500 * time.Millisecond,
		taskConfig:       tc,
	}

	// strr := `{
	// 	"initiator": "alice",
	// 	"project_id": "projectid",
	// 	"query": "SELECT count(*) FROM ta join tb on ta.id1=tb.id2;",
	// 	"tables":{
	// 		"alice":{"tbls":[{"table_name":"ta", "ref_table":"alice-table", "db_type":"csvdb", "table_owner": "alice", "columns": [{"name":"id1", "dtype":"string"},{"name":"age", "dtype":"float"}]}]},
	// 		"bob":{"tbls":[{"table_name":"tb", "ref_table":"bob-table", "db_type":"csvdb", "table_owner": "bob", "columns": [{"name":"id2", "dtype":"string"},{"name":"contact_cellular", "dtype":"float"}]}]}
	// 	},
	// 	"ccls":{
	// 		"alice":{"column_control_list":[
	// 			{"col":{"column_name":"id1", "table_name":"ta"}, "party_code":"alice","constraint":"PLAINTEXT"},
	// 			{"col":{"column_name":"id1", "table_name":"ta"}, "party_code":"bob","constraint":"PLAINTEXT_AFTER_JOIN"}
	// 		]},
	// 		"bob":{"column_control_list":[
	// 			{"col":{"column_name":"id2", "table_name":"tb"}, "party_code":"alice","constraint":"PLAINTEXT"},
	// 			{"col":{"column_name":"id2", "table_name":"tb"}, "party_code":"bob","constraint":"PLAINTEXT_AFTER_JOIN"}
	// 		]}
	// 	}
	// }`
	// tcc := &taskconfig.ScqlTaskInputConfig{}
	// suite.NoError(protojson.Unmarshal([]byte(strr), tcc))
	// logrus.Warn(tcc.String())
}

func (suite *ScqlStubTestSuit) TearDownTest() {
	suite.server.Close()
}

func (suite *ScqlStubTestSuit) TestCreateProject() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.CreateProjectResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.CreateProject())

	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.CreateProjectResponse{
			Status: &scql.Status{
				Code:    0,
				Message: "success",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.CreateProject())
}

func (suite *ScqlStubTestSuit) TestInviteMember() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.InviteMemberResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.InviteMember())

	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.InviteMemberResponse{
			Status: &scql.Status{
				Code:    0,
				Message: "success",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.InviteMember())
}

func (suite *ScqlStubTestSuit) TestWaitAndAcceptInvitation() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == constant.ListInvitationsPath {
			resp := &scql.ListInvitationsResponse{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
				Invitations: []*scql.ProjectInvitation{
					{
						InvitationId: 0,
						Inviter:      "alice",
						Project: &scql.ProjectDesc{
							ProjectId: "project_id",
						},
					},
				},
			}
			body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		} else if r.URL.Path == constant.ProcessInvitationPath {
			resp := &scql.ProcessInvitationResponse{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
			}
			body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}
	})

	suite.NoError(suite.stub.WaitAndAcceptInvitation())
}

func (suite *ScqlStubTestSuit) TestWaitMemberReady() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.ListProjectsResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.WaitMemberReady())

	respCnt := 0
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respList := []*scql.ListProjectsResponse{
			{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
				Projects: []*scql.ProjectDesc{
					{
						ProjectId: "project_id",
						Members:   []string{"alice"},
					},
				},
			},
			{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
				Projects: []*scql.ProjectDesc{
					{
						ProjectId: "project_id",
						Members:   []string{"alice", "bob"},
					},
				},
			},
		}
		body, _ := message.SerializeTo(respList[respCnt], message.EncodingTypeJson)
		respCnt++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.WaitMemberReady())
}

func (suite *ScqlStubTestSuit) TestCreateTable() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.CreateTableResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.CreateTable())

	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.CreateTableResponse{
			Status: &scql.Status{
				Code:    0,
				Message: "success",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.CreateTable())
}

func (suite *ScqlStubTestSuit) TestGrantCcl() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.GrantCCLResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.GrantCcl())

	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.GrantCCLResponse{
			Status: &scql.Status{
				Code:    0,
				Message: "success",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.GrantCcl())
}

func (suite *ScqlStubTestSuit) TestWaitCclReady() {
	respCnt := 0
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respList := []*scql.ShowCCLResponse{
			{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
				ColumnControlList: []*scql.ColumnControl{
					{Col: &scql.ColumnDef{TableName: "ta", ColumnName: "id"}, PartyCode: "alice", Constraint: scql.Constraint_PLAINTEXT},
				},
			},
			{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
				ColumnControlList: []*scql.ColumnControl{
					{Col: &scql.ColumnDef{TableName: "ta", ColumnName: "id"}, PartyCode: "alice", Constraint: scql.Constraint_PLAINTEXT},
					{Col: &scql.ColumnDef{TableName: "ta", ColumnName: "id"}, PartyCode: "bob", Constraint: scql.Constraint_PLAINTEXT_AFTER_JOIN},
				},
			},
		}
		body, _ := message.SerializeTo(respList[respCnt], message.EncodingTypeJson)
		respCnt++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})

	suite.NoError(suite.stub.WaitCclReady())
}

func (suite *ScqlStubTestSuit) TestCreateJob() {
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.SubmitResponse{
			Status: &scql.Status{
				Code:    1,
				Message: "failed",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.Error(suite.stub.CreateJob())

	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &scql.SubmitResponse{
			Status: &scql.Status{
				Code:    0,
				Message: "success",
			},
		}
		body, _ := message.SerializeTo(resp, message.EncodingTypeJson)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	suite.NoError(suite.stub.CreateJob())
}

func (suite *ScqlStubTestSuit) TestWaitResult() {
	respCnt := 0
	suite.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respList := []*scql.FetchResultResponse{
			{
				Status: &scql.Status{
					Code:    int32(scql.Code_NOT_READY),
					Message: "not ready",
				},
			},
			{
				Status: &scql.Status{
					Code:    0,
					Message: "success",
				},
			},
		}
		body, _ := message.SerializeTo(respList[respCnt], message.EncodingTypeJson)
		respCnt++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	})
	_, e := suite.stub.WaitResult(false)
	suite.NoError(e)
}

func TestServerSuit(t *testing.T) {
	suite.Run(t, new(ScqlStubTestSuit))
}
