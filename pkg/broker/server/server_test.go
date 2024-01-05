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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/services/auth"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/broker/testdata"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
	urlutil "github.com/secretflow/scql/pkg/util/url"
)

const (
	partyInfo = `{
		"participants": [
			{
				"party_code": "alice",
				"endpoint": "http://localhost:ALICE_INTER_PORT",
				"pubkey": "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA="
			},
			{
				"party_code": "bob",
				"endpoint": "http://localhost:BOB_INTER_PORT",
				"pubkey": "MCowBQYDK2VwAyEAN3w+v2uks/QEaVZiprZ8oRChMkBOZJSAl6V/5LvOnt4="
			},
			{
				"party_code": "carol",
				"endpoint": "http://localhost:CAROL_INTER_PORT",
				"pubkey": "MCowBQYDK2VwAyEANhiAXTvL4x2jYUiAbQRo9XuOTrFFnAX4Q+YlEAgULs8="
			}
		]
	}
	`
)

type ServerTestSuit struct {
	suite.Suite
	appAlice  *application.App
	appBob    *application.App
	freePorts []int
}

func (suite *ServerTestSuit) SetupTest() {
	freePorts, err := GetFreePorts(4)
	suite.NoError(err)
	err = testdata.CreateTestPemFiles("../testdata")
	suite.NoError(err)
	f, err := os.CreateTemp("", "broker_server_test")
	suite.NoError(err)
	defer os.Remove(f.Name())
	content := strings.ReplaceAll(partyInfo, "ALICE_INTER_PORT", fmt.Sprint(freePorts[1]))
	content = strings.ReplaceAll(content, "BOB_INTER_PORT", fmt.Sprint(freePorts[3]))
	_, err = f.Write([]byte(content))
	suite.NoError(err)
	suite.freePorts = freePorts

	aliceApp, err := buildTestApp("alice", freePorts[:2], f.Name())
	suite.NoError(err)
	suite.appAlice = aliceApp

	bobApp, err := buildTestApp("bob", freePorts[2:], f.Name())
	suite.NoError(err)
	suite.appBob = bobApp

	time.Sleep(500 * time.Millisecond) // wait http start
}

func (suite *ServerTestSuit) TestServer() {
	httpClient := &http.Client{Timeout: 1 * time.Second}
	urlAlice := fmt.Sprintf("http://localhost:%v", suite.freePorts[0])
	urlBob := fmt.Sprintf("http://localhost:%v", suite.freePorts[2])

	// Create project: alice creates a project
	{
		reqStr := `{"project_id":"test_id", "name":"test", "conf":{"spu_runtime_cfg":{"protocol": "SEMI2K"}}}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.CreateProjectPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.CreateProjectResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0), response.String())
		resp.Body.Close()
	}

	// CHECK Create project: alice lists projects
	{
		reqStr := `{}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.ListProjectsPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListProjectsResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetProjects()), 1)
		suite.Equal(response.GetProjects()[0].Name, "test")
		resp.Body.Close()
	}

	// Invite member: alice invites bob
	{
		reqStr := `{"project_id":"test_id", "invitee":"bob"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.InviteMemberPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.InviteMemberResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Invite member: alice / bob list invitations
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListInvitationsPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListInvitationsResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetInvitations()), 1)
		suite.Equal(response.GetInvitations()[0].GetProject().GetName(), "test")
		suite.Equal(response.GetInvitations()[0].GetInviter(), "alice")
		suite.Equal(response.GetInvitations()[0].GetStatus(), pb.InvitationStatus_UNDECIDED)
		resp.Body.Close()
	}

	// Invite member: alice invites bob again
	{
		reqStr := `{"project_id":"test_id", "invitee":"bob"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.InviteMemberPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.InviteMemberResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Invite member: alice / bob list invitations
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListInvitationsPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListInvitationsResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetInvitations()), 2)
		suite.Equal(response.GetInvitations()[0].GetStatus(), pb.InvitationStatus_INVALID)
		resp.Body.Close()
	}

	// evil carol try to join project in alice
	{
		req := &pb.ReplyInvitationRequest{
			ClientId: &pb.PartyId{
				Code: "carol",
			},
			ProjectId: "test_id",
			Respond:   pb.InvitationRespond_ACCEPT,
		}
		auth, err := auth.NewAuth("../testdata/private_key_carol.pem")
		suite.NoError(err)
		err = auth.SignMessage(req)
		suite.NoError(err)
		reqBytes, err := protojson.Marshal(req)
		suite.NoError(err)

		resp, err := httpClient.Post(urlutil.JoinHostPath(fmt.Sprintf("http://localhost:%v", suite.freePorts[1]), constant.ReplyInvitationPath),
			"application/x-protobuf", strings.NewReader(string(reqBytes)))
		suite.NoError(err)

		response := &pb.ReplyInvitationResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(pb.Code_INTERNAL), protojson.Format(response))
		resp.Body.Close()
	}

	// Process Invitation: bob agrees to join project
	{
		reqStr := `{"invitation_id":2, "respond": 0, "respond_comment":"I agree to join you"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlBob, constant.ProcessInvitationPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ProcessInvitationResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Process Invitation: project member and invitation accepted status changed
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListProjectsPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListProjectsResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetProjects()), 1)
		suite.Equal(response.GetProjects()[0].Name, "test")
		suite.Equal(response.GetProjects()[0].Members, []string{"alice", "bob"})
		resp.Body.Close()
	}
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListInvitationsPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListInvitationsResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetInvitations()), 2)
		suite.Equal(response.GetInvitations()[1].GetProject().GetName(), "test")
		suite.Equal(response.GetInvitations()[1].GetInviter(), "alice")
		suite.Equal(response.GetInvitations()[1].GetStatus(), pb.InvitationStatus_ACCEPTED)
		resp.Body.Close()
	}

	// Create Table: alice create table
	{
		reqStr := `{"project_id":"test_id", "table_name": "table_alice", "ref_table":"alice", "db_type": "MySQL", "columns":[{"name":"col1", "dtype":"string"},{"name":"col2", "dtype":"float"}]}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.CreateTablePath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.CreateTableResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
		{
			// Create same table is not allowed
			resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.CreateTablePath),
				"application/json", strings.NewReader(reqStr))
			suite.NoError(err)
			response := &pb.CreateTableResponse{}
			_, err = message.DeserializeFrom(resp.Body, response)
			suite.NoError(err)
			suite.Equal(response.GetStatus().GetCode(), int32(pb.Code_INTERNAL))
		}
	}

	// CHECK Create Table: alice and bob list tables
	time.Sleep(100 * time.Millisecond)
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{"project_id":"test_id"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListTablesPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListTablesResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetTables()), 1)
		suite.Equal(response.GetTables()[0].GetTableName(), "table_alice")
		suite.Equal(response.GetTables()[0].GetTableOwner(), "alice")
		resp.Body.Close()
	}

	// Grant CCL: alice grant ccl
	{
		reqStr := `{"project_id":"test_id", "column_control_list":[{"col":{"column_name":"col1", "table_name":"table_alice"}, "party_code":"bob", "constraint":1}]}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.GrantCCLPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.GrantCCLResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Grant CCL: alice and bob show ccl
	time.Sleep(100 * time.Millisecond)
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{"project_id":"test_id"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ShowCCLPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ShowCCLResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetColumnControlList()), 1)
		suite.Equal(response.GetColumnControlList()[0].GetCol().GetTableName(), "table_alice")
		suite.Equal(response.GetColumnControlList()[0].GetPartyCode(), "bob")
		suite.Equal(response.GetColumnControlList()[0].GetConstraint(), pb.Constraint_PLAINTEXT)

		resp.Body.Close()
	}

	// AskInfo: bob ask information from alice
	{
		req := &pb.AskInfoRequest{
			ClientId: &pb.PartyId{
				Code: "bob",
			},
			ResourceSpecs: []*pb.ResourceSpec{
				{
					Kind:      pb.ResourceSpec_Project,
					ProjectId: "test_id",
				},
				{
					Kind:       pb.ResourceSpec_Table,
					ProjectId:  "test_id",
					TableNames: []string{"table_alice"},
				},
				{
					Kind:       pb.ResourceSpec_CCL,
					ProjectId:  "test_id",
					TableNames: []string{"table_alice"},
				},
			},
		}
		response := &pb.AskInfoResponse{}
		err := suite.appBob.InterStub.AskInfo(fmt.Sprintf("http://localhost:%v", suite.freePorts[1]), req, response)
		suite.NoError(err)
		// check result
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetDatas()), 3)
		// check project
		var proj storage.ProjectWithMember
		err = json.Unmarshal(response.GetDatas()[0], &proj)
		suite.NoError(err)
		suite.Equal(proj.Proj.Name, "test")
		// check table
		var tables []storage.TableMeta
		err = json.Unmarshal(response.GetDatas()[1], &tables)
		suite.NoError(err)
		suite.Equal(len(tables), 1)
		suite.Equal(tables[0].Table.TableIdentifier, storage.TableIdentifier{
			ProjectID: "test_id",
			TableName: "table_alice"})
		suite.Equal(len(tables[0].Columns), 2)
		suite.Equal(tables[0].Columns[0].ColumnName, "col1")
		suite.Equal(tables[0].Columns[1].ColumnName, "col2")
		// check ccl
		var ccls []storage.ColumnPriv
		err = json.Unmarshal(response.GetDatas()[2], &ccls)
		suite.NoError(err)
		suite.Equal(len(ccls), 1)
		suite.Equal(ccls[0].ColumnPrivIdentifier, storage.ColumnPrivIdentifier{
			ProjectID:  "test_id",
			TableName:  "table_alice",
			ColumnName: "col1",
			DestParty:  "bob",
		})
		suite.Equal(ccls[0].Priv, "PLAINTEXT")
	}

	// Revoke CCL: alice revoke ccl
	{
		reqStr := `{"project_id":"test_id", "column_control_list":[{"col":{"column_name":"col1", "table_name":"table_alice"}, "party_code":"bob", "constraint":1}]}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.RevokeCCLPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.RevokeCCLResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Revoke CCL: alice and bob show ccl
	time.Sleep(100 * time.Millisecond)
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{"project_id":"test_id"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ShowCCLPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ShowCCLResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetColumnControlList()), 1)
		suite.Equal(pb.Constraint_UNKNOWN, response.GetColumnControlList()[0].Constraint)

		resp.Body.Close()
	}

	// Drop Table: alice drop table
	{
		reqStr := `{"project_id":"test_id", "table_name": "table_alice"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.DropTablePath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.DropTableResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		resp.Body.Close()
	}

	// CHECK Drop Table: alice and bob list tables
	time.Sleep(100 * time.Millisecond)
	for _, party := range []string{urlAlice, urlBob} {
		reqStr := `{"project_id":"test_id"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(party, constant.ListTablesPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.ListTablesResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0))
		suite.Equal(len(response.GetTables()), 0)
		resp.Body.Close()
	}

	// EngineCallback: alice report results
	{
		{
			// prepare session for reporting
			app := suite.appAlice
			info := &application.ExecutionInfo{
				ProjectID:    "test_id",
				JobID:        "job_id",
				Query:        "select * from ta",
				Issuer:       &pb.PartyId{Code: "alice"},
				EngineClient: app.EngineClient,
			}
			session, err := application.NewSession(context.Background(), info, app, false)
			suite.NoError(err)
			app.AddSession("job_id", session)
		}
		reqStr := `{"status":{"code": 0, "message": "ok"}, "session_id": "job_id", "num_rows_affected": 10}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.EngineCallbackPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)
		suite.Equal(resp.StatusCode, http.StatusOK, resp.Status)
	}
	// CHECK EngineCallback: FetResult from alice
	{
		time.Sleep(100 * time.Millisecond)
		reqStr := `{"job_id":"job_id"}`
		resp, err := httpClient.Post(urlutil.JoinHostPath(urlAlice, constant.FetchResultPath),
			"application/json", strings.NewReader(reqStr))
		suite.NoError(err)

		response := &pb.QueryResponse{}
		_, err = message.DeserializeFrom(resp.Body, response)
		suite.NoError(err)
		suite.Equal(response.GetStatus().GetCode(), int32(0), protojson.Format(response))
		suite.Equal(response.GetAffectedRows(), int64(10), protojson.Format(response))
		resp.Body.Close()
	}
}

func TestServerSuit(t *testing.T) {
	suite.Run(t, new(ServerTestSuit))
}

func buildTestApp(party string, ports []int, infoFile string) (app *application.App, err error) {
	cfg := &config.Config{
		InterServer: config.ServerConfig{
			Port: ports[1],
		},
		IntraServer: config.ServerConfig{
			Port: ports[0],
		},
		PartyCode:      party,
		PartyInfoFile:  infoFile,
		PrivatePemPath: fmt.Sprintf("../testdata/private_key_%s.pem", party),
		Engine: config.EngineConfig{
			ClientTimeout: 1 * time.Second,
			Protocol:      "SEMI2K",
			ContentType:   "application/json",
			Uris:          []config.EngineUri{{ForPeer: "fake url"}},
		},
	}

	partyMgr, err := partymgr.NewFilePartyMgr(cfg.PartyInfoFile, cfg.PartyCode)
	if err != nil {
		return nil, fmt.Errorf("Failed to create partyMgr from: %v", err)
	}
	db, err := gorm.Open(sqlite.Open(":memory:"),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Warn,
				}),
		})
	if err != nil {
		return nil, fmt.Errorf("Failed to create broker db: %v", err)
	}
	metaMgr := storage.NewMetaManager(db)
	err = metaMgr.Bootstrap()
	if err != nil {
		return nil, fmt.Errorf("Failed to boot strap meta manager: %v", err)
	}
	app, err = application.NewApp(partyMgr, metaMgr, cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to create app: %v", err)
	}

	IntraSvr, err := NewIntraServer(app)
	if err != nil {
		return nil, fmt.Errorf("Failed to create intraSvc: %v", err)
	}
	go func() {
		err := IntraSvr.ListenAndServe()
		if err != nil {
			log.Fatalf("start service failed")
		}
	}()
	InterSvr, err := NewInterServer(app)
	if err != nil {
		return nil, fmt.Errorf("Failed to create interSvc: %v", err)
	}
	go func() {
		err := InterSvr.ListenAndServe()
		if err != nil {
			log.Fatalf("start service failed")
		}
	}()

	return app, nil
}

func GetFreePorts(count int) ([]int, error) {
	var ports []int
	for i := 0; i < count; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		// WARN: port occupation is released before return,
		// other programs may seize the free port, resulting in freePort usage conflicts
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}
