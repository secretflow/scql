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

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/interpreter"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/util/brokerutil"
)

func TestQueryRunner(t *testing.T) {
	r := require.New(t)

	// mock data
	meta, err := buildTestStorage()
	r.NoError(err)
	transaction := meta.CreateMetaTransaction()
	projectID1 := "p1"
	projectName1 := "n1"
	projectConf, _ := json.Marshal(scql.ProjectConfig{
		SpuRuntimeCfg: &spu.RuntimeConfig{
			Protocol: spu.ProtocolKind_SEMI2K,
			Field:    spu.FieldType_FM64,
			TtpBeaverConfig: &spu.TTPBeaverConfig{
				ServerHost: "127.0.0.1",
			},
		}})
	alice := "alice"
	// create project
	err = transaction.CreateProject(storage.Project{ID: projectID1, Name: projectName1, ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)
	tableName1 := "t1"
	t1Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: tableName1}
	tableMeta := storage.TableMeta{
		Table: storage.Table{TableIdentifier: t1Identifier, RefTable: "real.t1", Owner: alice, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
		},
	}
	// create table
	err = transaction.AddTable(tableMeta)
	r.NoError(err)
	bob := "bob"
	tableName2 := "t2"
	t2Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: tableName2}
	tableMeta = storage.TableMeta{
		Table: storage.Table{TableIdentifier: t2Identifier, RefTable: "real.t2", Owner: bob, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
		},
	}
	// create table
	err = transaction.AddTable(tableMeta)
	r.NoError(err)
	// grant
	c1Identifier := storage.ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: t1Identifier.TableName, ColumnName: "id"}
	c2Identifier := storage.ColumnIdentifier{ProjectID: t2Identifier.ProjectID, TableName: t2Identifier.TableName, ColumnName: "id"}
	privsAlice := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"}}
	privsBob := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	// add bob to project before granting ccl
	err = transaction.AddProjectMembers([]storage.Member{storage.Member{ProjectID: t1Identifier.ProjectID, Member: bob}})
	r.NoError(err)
	err = common.GrantColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, privsAlice, common.OwnerChecker{Owner: alice})
	r.NoError(err)
	err = common.GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, privsBob, common.OwnerChecker{Owner: bob})
	r.NoError(err)
	filesMap, err := brokerutil.CreateTestPemFiles(nil, t.TempDir())
	r.NoError(err)
	// mock config
	cfg, err := config.NewConfig("config_test.yml")
	r.NoError(err)
	cfg.PrivateKeyPath = filesMap[brokerutil.AlicePemFilKey]
	// mock party meta
	partyMgr, err := partymgr.NewFilePartyMgr(filesMap[brokerutil.PartyInfoFileKey])
	r.NoError(err)
	app, err := application.NewApp(partyMgr, meta, cfg)
	r.NoError(err)
	info := &application.ExecutionInfo{
		ProjectID:    projectID1,
		JobID:        "mock job id",
		Query:        "select t1.id from t1 join t2 on t1.id = t2.id",
		Issuer:       &scql.PartyId{Code: "alice"},
		EngineClient: app.EngineClient,
		DebugOpts:    &scql.DebugOptions{EnablePsiDetailLog: true},
		SessionOptions: &application.SessionOptions{
			SessionExpireSeconds: 86400,
		},
	}
	transaction.Finish(nil)
	session, err := application.NewSession(context.Background(), info, app, false, false)
	r.NoError(err)
	executionInfo := session.ExecuteInfo
	session.SaveEndpoint(bob, "bob.com")
	runner := NewQueryRunner(session)
	usedTables, err := core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	dataParties, workParties, err := runner.Prepare(usedTables)
	r.NoError(err)
	executionInfo.WorkParties = workParties
	executionInfo.DataParties = dataParties
	// 3.2 sync info and get endpoints from other party
	localChecksums, err := runner.CreateChecksum()
	r.NoError(err)
	for code, checksum := range localChecksums {
		session.SaveLocalChecksum(code, checksum)
	}
	r.Equal(2, len(runner.GetEnginesInfo().GetPartyInfo().GetParticipants()))
	r.Equal(1, len(runner.GetEnginesInfo().GetTablesByParty(alice)))
	r.Equal(1, len(runner.GetEnginesInfo().GetTablesByParty(bob)))
	selfTableSchemaChecksum, err := session.GetLocalChecksum(alice)
	r.NoError(err)
	r.Equal([]byte{0x81, 0x6, 0x79, 0x59, 0x6e, 0x25, 0x3d, 0x1f, 0x8a, 0x37, 0x98, 0x3e, 0x3a, 0x6d, 0x97, 0xf8, 0xb, 0xd7, 0xb9, 0x1f, 0x36, 0x13, 0xdf, 0xcc, 0x6a, 0x94, 0x7c, 0x82, 0x1b, 0xd9, 0x64, 0x3e}, selfTableSchemaChecksum.TableSchema)
	r.Equal([]byte{0xb2, 0x89, 0xde, 0x6d, 0xb5, 0x20, 0xb7, 0x1a, 0x59, 0x47, 0x6c, 0xe7, 0x92, 0x3f, 0xf0, 0xc7, 0x7b, 0x12, 0xe8, 0x8a, 0x64, 0x2, 0x5b, 0x78, 0x4b, 0x8c, 0xcc, 0x4d, 0x53, 0x9d, 0x66, 0xa2}, selfTableSchemaChecksum.CCL)
	refT, err := runner.GetEnginesInfo().GetRefTableName(strings.Join([]string{projectID1, tableName1}, "."))
	expectT := core.NewDbTable("real", "t1")
	expectT.SetDBType(core.DBTypeMySQL)
	r.Equal(expectT, refT)
	compileReq := runner.buildCompileQueryRequest()
	intrpr := interpreter.NewInterpreter()
	compiledPlan, err := intrpr.Compile(context.Background(), compileReq)
	r.NoError(err)
	r.Equal(`digraph G {
0 [label="runsql:{in:[],out:[Out:{t_0,},],attr:[sql:select t1.id from real.t1,table_refs:[real.t1],],party:[alice,]}"]
1 [label="runsql:{in:[],out:[Out:{t_1,},],attr:[sql:select t2.id from real.t2,table_refs:[real.t2],],party:[bob,]}"]
2 [label="join:{in:[Left:{t_0,},Right:{t_1,},],out:[LeftJoinIndex:{t_2,},RightJoinIndex:{t_3,},],attr:[input_party_codes:[alice bob],join_type:0,psi_algorithm:1,],party:[alice,bob,]}"]
3 [label="filter_by_index:{in:[Data:{t_0,},RowsIndexFilter:{t_2,},],out:[Out:{t_4,},],attr:[],party:[alice,]}"]
4 [label="filter_by_index:{in:[Data:{t_1,},RowsIndexFilter:{t_3,},],out:[Out:{t_5,},],attr:[],party:[bob,]}"]
5 [label="publish:{in:[In:{t_4,},],out:[Out:{t_6,},],attr:[],party:[alice,]}"]
0 -> 2 [label = "t_0:{id:PRIVATE:INT64}"]
0 -> 3 [label = "t_0:{id:PRIVATE:INT64}"]
1 -> 2 [label = "t_1:{id:PRIVATE:INT64}"]
1 -> 4 [label = "t_1:{id:PRIVATE:INT64}"]
2 -> 3 [label = "t_2:{id:PRIVATE:INT64}"]
2 -> 4 [label = "t_3:{id:PRIVATE:INT64}"]
3 -> 5 [label = "t_4:{id:PRIVATE:INT64}"]
}`, compiledPlan.Explain.GetExeGraphDot())
	r.Equal("8b6a91c03d6a9d977d6d55919b890caff6874e2b6a49ade0e669fcad17d24469", compiledPlan.WholeGraphChecksum)
}

func TestCaseSensitive(t *testing.T) {
	r := require.New(t)
	meta, err := buildTestStorage()
	r.NoError(err)
	transaction := meta.CreateMetaTransaction()
	projectID1 := "Project1"
	projectConf, _ := json.Marshal(scql.ProjectConfig{SpuRuntimeCfg: &spu.RuntimeConfig{Protocol: spu.ProtocolKind_SEMI2K}})
	alice := "alice"
	// create project
	err = transaction.CreateProject(storage.Project{ID: projectID1, Name: "n1", ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)
	tableName1 := "t1"
	t1Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: tableName1}
	tableMeta := storage.TableMeta{
		Table: storage.Table{TableIdentifier: t1Identifier, RefTable: "real.T1", Owner: alice, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
			{ColumnName: "Data", DType: "int"},
		},
	}
	bob := "bob"
	// add bob to project before granting ccl
	err = transaction.AddProjectMembers([]storage.Member{storage.Member{ProjectID: t1Identifier.ProjectID, Member: bob}})
	// create table
	err = common.AddTableWithCheck(transaction, projectID1, alice, tableMeta)
	r.NoError(err)
	tableName2 := "T2"
	t2Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: tableName2}
	tableMeta = storage.TableMeta{
		Table: storage.Table{TableIdentifier: t2Identifier, RefTable: "real.t2", Owner: bob, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
			{ColumnName: "Data", DType: "int"},
		},
	}
	// create table
	err = common.AddTableWithCheck(transaction, projectID1, bob, tableMeta)
	r.NoError(err)
	// grant id
	c1Identifier := storage.ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: t1Identifier.TableName, ColumnName: "id"}
	c2Identifier := storage.ColumnIdentifier{ProjectID: t2Identifier.ProjectID, TableName: t2Identifier.TableName, ColumnName: "id"}
	privsAlice := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"}}
	privsBob := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	r.NoError(err)
	err = common.GrantColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, privsAlice, common.OwnerChecker{Owner: alice})
	r.NoError(err)
	err = common.GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, privsBob, common.OwnerChecker{Owner: bob})
	r.NoError(err)
	// grant data
	c1Identifier = storage.ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: t1Identifier.TableName, ColumnName: "Data"}
	c2Identifier = storage.ColumnIdentifier{ProjectID: t2Identifier.ProjectID, TableName: t2Identifier.TableName, ColumnName: "Data"}
	privsAlice = []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"}}
	privsBob = []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	err = common.GrantColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, privsAlice, common.OwnerChecker{Owner: alice})
	r.NoError(err)
	err = common.GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, privsBob, common.OwnerChecker{Owner: bob})
	r.NoError(err)
	transaction.Finish(nil)
	// mock config
	cfg, err := config.NewConfig("config_test.yml")
	r.NoError(err)
	filesMap, err := brokerutil.CreateTestPemFiles(nil, t.TempDir())
	r.NoError(err)
	// mock party meta
	partyMgr, err := partymgr.NewFilePartyMgr(filesMap[brokerutil.PartyInfoFileKey])
	r.NoError(err)
	cfg.PrivateKeyPath = filesMap[brokerutil.AlicePemFilKey]
	app, err := application.NewApp(partyMgr, meta, cfg)
	r.NoError(err)
	info := &application.ExecutionInfo{
		ProjectID:    projectID1,
		JobID:        "mock job id",
		Query:        "select t1.id, t1.Data + t2.Data from Project1.t1 join Project1.T2 as t2 on t1.id = t2.id",
		Issuer:       &scql.PartyId{Code: "alice"},
		EngineClient: app.EngineClient,
		SessionOptions: &application.SessionOptions{
			SessionExpireSeconds: 86400,
		},
	}
	session, err := application.NewSession(context.Background(), info, app, false, false)
	r.NoError(err)
	session.SaveEndpoint(bob, "bob.com")
	runner := NewQueryRunner(session)
	usedTables, err := core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	dataParties, workParties, err := runner.Prepare(usedTables)
	r.NoError(err)
	session.ExecuteInfo.WorkParties = workParties
	session.ExecuteInfo.DataParties = dataParties
	compileReq := runner.buildCompileQueryRequest()
	intrpr := interpreter.NewInterpreter()
	compiledPlan, err := intrpr.Compile(context.Background(), compileReq)
	r.NoError(err)
	r.NotNil(compiledPlan)
	// check alice
	attrSQL, ok := compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select T1.Data,T1.id from real.T1", attrSQL.T.GetStringData()[0])
	attrTable, ok := compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("real.T1", attrTable.T.GetStringData()[0])
	// check bob
	attrSQL, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select t2.Data,t2.id from real.t2 as t2", attrSQL.T.GetStringData()[0])
	attrTable, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("real.t2", attrTable.T.GetStringData()[0])
	// Project1.T2 -> Project1.t2
	// check table case sensitive
	session.ExecuteInfo.Query = "select t1.id, t1.Data + t2.Data from Project1.t1 as t1 join Project1.t2 as t2 on t1.id = t2.id"
	usedTables, err = core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	runner.Clear()
	dataParties, workParties, err = runner.Prepare(usedTables)
	r.Error(err)
	r.Equal("prepareData: table [t2] not found", err.Error())
	// Project1.T2 -> project1.T2
	// check project id case sensitive
	session.ExecuteInfo.Query = "select t1.id, t1.Data + t2.Data from Project1.t1 as t1 join project1.T2 as t2 on t1.id = t2.id"
	usedTables, err = core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	runner.Clear()
	dataParties, workParties, err = runner.Prepare(usedTables)
	r.NoError(err)
	session.ExecuteInfo.WorkParties = workParties
	session.ExecuteInfo.DataParties = dataParties
	compileReq = runner.buildCompileQueryRequest()
	compiledPlan, err = intrpr.Compile(context.Background(), compileReq)
	r.Error(err)
	r.Equal("TableByName: Table 'project1.T2' doesn't exist", err.Error())
	// Data -> data, id -> ID
	// check column case sensitive
	session.ExecuteInfo.Query = "select t1.ID, t1.data + t2.data from Project1.t1 as t1 join Project1.T2 as t2 on t1.ID = t2.ID"
	usedTables, err = core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	runner.Clear()
	dataParties, workParties, err = runner.Prepare(usedTables)
	r.NoError(err)
	session.ExecuteInfo.WorkParties = workParties
	session.ExecuteInfo.DataParties = dataParties
	compileReq = runner.buildCompileQueryRequest()
	compiledPlan, err = intrpr.Compile(context.Background(), compileReq)
	r.NoError(err)
	r.NotNil(compiledPlan)
	// check alice
	attrSQL, ok = compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select t1.Data,t1.id from real.T1 as t1", attrSQL.T.GetStringData()[0])
	attrTable, ok = compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("real.T1", attrTable.T.GetStringData()[0])
	// check bob
	attrSQL, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select t2.Data,t2.id from real.t2 as t2", attrSQL.T.GetStringData()[0])
	attrTable, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)

	// create table T1
	tableT1Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: "T1"}
	tableMeta = storage.TableMeta{
		Table: storage.Table{TableIdentifier: tableT1Identifier, RefTable: "real.UpperT1", Owner: alice, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
			{ColumnName: "Data", DType: "int"},
		},
	}
	transaction = meta.CreateMetaTransaction()
	// create table
	err = common.AddTableWithCheck(transaction, projectID1, alice, tableMeta)
	r.NoError(err)
	// grant CCL
	idIdentifier := storage.ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: "T1", ColumnName: "id"}
	dataIdentifier := storage.ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: "T1", ColumnName: "data"}
	privs := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: idIdentifier.ProjectID, TableName: idIdentifier.TableName, ColumnName: idIdentifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: idIdentifier.ProjectID, TableName: idIdentifier.TableName, ColumnName: idIdentifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: dataIdentifier.ProjectID, TableName: dataIdentifier.TableName, ColumnName: dataIdentifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: dataIdentifier.ProjectID, TableName: dataIdentifier.TableName, ColumnName: dataIdentifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	err = common.GrantColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, privs, common.OwnerChecker{Owner: alice})
	r.NoError(err)
	transaction.Finish(nil)
	session.ExecuteInfo.Query = "select t1.ID, t1.data + t2.data from Project1.T1 as t1 join Project1.T2 as t2 on t1.ID = t2.ID"
	usedTables, err = core.GetSourceTables(session.ExecuteInfo.Query)
	r.NoError(err)
	runner.Clear()
	dataParties, workParties, err = runner.Prepare(usedTables)
	r.NoError(err)
	session.ExecuteInfo.WorkParties = workParties
	session.ExecuteInfo.DataParties = dataParties
	compileReq = runner.buildCompileQueryRequest()
	compiledPlan, err = intrpr.Compile(context.Background(), compileReq)
	r.NoError(err)
	r.NotNil(compiledPlan)
	// check alice
	attrSQL, ok = compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select t1.Data,t1.id from real.UpperT1 as t1", attrSQL.T.GetStringData()[0])
	attrTable, ok = compiledPlan.SubGraphs["alice"].Nodes["0"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("real.UpperT1", attrTable.T.GetStringData()[0])
	// check bob
	attrSQL, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["sql"].Value.(*scql.AttributeValue_T)
	r.True(ok)
	r.Equal("select t2.Data,t2.id from real.t2 as t2", attrSQL.T.GetStringData()[0])
	attrTable, ok = compiledPlan.SubGraphs["bob"].Nodes["1"].Attributes["table_refs"].Value.(*scql.AttributeValue_T)
	r.True(ok)

	// test column case sensitive
	transaction = meta.CreateMetaTransaction()
	// create table test
	// error due to duplicate columns
	tableIdentifier := storage.TableIdentifier{ProjectID: projectID1, TableName: "T1"}
	tableMeta = storage.TableMeta{
		Table: storage.Table{TableIdentifier: tableIdentifier, RefTable: "real.UpperT1", Owner: alice, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
			{ColumnName: "Data", DType: "int"},
			{ColumnName: "data", DType: "int"},
		},
	}
	err = common.AddTableWithCheck(transaction, projectID1, alice, tableMeta)
	r.Error(err)

	// revoke CCL test
	idPrivID := storage.ColumnPrivIdentifier{ProjectID: t1Identifier.ProjectID, TableName: "t1", ColumnName: "id", DestParty: alice}
	// column name case insensitive `Data` -> `data`
	dataPrivID := storage.ColumnPrivIdentifier{ProjectID: t1Identifier.ProjectID, TableName: "t1", ColumnName: "data", DestParty: alice}
	err = common.RevokeColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, alice, []storage.ColumnPrivIdentifier{idPrivID, dataPrivID})
	r.NoError(err)
	privs, err = transaction.ListColumnConstraints(projectID1, []string{"t1"}, []string{alice})
	r.NoError(err)
	for _, priv := range privs {
		if priv.ColumnName == "Data" {
			r.Equal("UNKNOWN", priv.Priv)
		}
	}
	transaction.Finish(nil)
}

// create different in memory db
func buildTestStorage() (*storage.MetaManager, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	// use uuid as dbname
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
	// mock data
	db, err := gorm.Open(sqlite.Open(connStr),
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
		return nil, err
	}
	meta := storage.NewMetaManager(db, false)
	err = meta.Bootstrap()
	if err != nil {
		return nil, err
	}
	return meta, nil
}
