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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestQueryRunner(t *testing.T) {
	r := require.New(t)

	// mock data
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

	r.NoError(err)
	meta := storage.NewMetaManager(db)
	err = meta.Bootstrap()
	r.NoError(err)
	transaction := meta.CreateMetaTransaction()
	projectID1 := "p1"
	projectName1 := "n1"
	projectConf := storage.ProjectConfig{SpuConf: `{
        "protocol": "SEMI2K",
        "field": "FM64",
        "ttp_beaver_config": {"server_host": "127.0.0.1"}
    }`}
	alice := "alice"
	// create project
	err = transaction.CreateProject(storage.Project{ID: projectID1, Name: projectName1, ProjectConf: projectConf, Creator: alice, Member: alice})
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
	privs := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	err = transaction.GrantColumnConstraints(privs)
	r.NoError(err)
	// mock config
	cfg, err := config.NewConfig("../testdata/config_test.yml")
	r.NoError(err)
	// mock party meta
	partyMgr, err := partymgr.NewFilePartyMgr("../testdata/party_info_test.json", cfg.PartyCode, cfg.Engines)
	r.NoError(err)
	app, err := application.NewApp(partyMgr, meta, cfg)
	r.NoError(err)
	info := &application.ExecutionInfo{
		ProjectID:    projectID1,
		JobID:        "mock job id",
		Query:        "select t1.id from t1 join t2 on t1.id = t2.id",
		Issuer:       &scql.PartyId{Code: "alice"},
		EngineClient: app.EngineClient,
	}
	transaction.Finish(nil)
	session, err := application.NewSession(context.Background(), info, app, false)
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
	// 	graph, err := runner.Compile()
	// 	r.NoError(err)
	// 	r.Equal(`digraph G {
	// 0 [label="runsql:{in:[],out:[Out:{t_0,},],attr:[sql:select t1.id from real.t1,table_refs:[real.t1],],party:[alice,]}"]
	// 1 [label="runsql:{in:[],out:[Out:{t_1,},],attr:[sql:select t2.id from real.t2,table_refs:[real.t2],],party:[bob,]}"]
	// 2 [label="join:{in:[Left:{t_0,},Right:{t_1,},],out:[LeftJoinIndex:{t_2,},RightJoinIndex:{t_3,},],attr:[input_party_codes:[alice bob],join_type:0,],party:[alice,bob,]}"]
	// 3 [label="filter_by_index:{in:[Data:{t_0,},RowsIndexFilter:{t_2,},],out:[Out:{t_4,},],attr:[],party:[alice,]}"]
	// 4 [label="filter_by_index:{in:[Data:{t_1,},RowsIndexFilter:{t_3,},],out:[Out:{t_5,},],attr:[],party:[bob,]}"]
	// 5 [label="publish:{in:[In:{t_4,},],out:[Out:{t_6,},],attr:[],party:[alice,]}"]
	// 0 -> 2 [label = "t_0:{id:PRIVATE:INT64}"]
	// 0 -> 3 [label = "t_0:{id:PRIVATE:INT64}"]
	// 1 -> 2 [label = "t_1:{id:PRIVATE:INT64}"]
	// 1 -> 4 [label = "t_1:{id:PRIVATE:INT64}"]
	// 2 -> 3 [label = "t_2:{id:PRIVATE:INT64}"]
	// 2 -> 4 [label = "t_3:{id:PRIVATE:INT64}"]
	// 3 -> 5 [label = "t_4:{id:PRIVATE:INT64}"]
	// }`, graph.DumpGraphviz())
	// 	syncer, err := runner.CreateSyncExecutor(graph)
	// 	r.NoError(err)
	// 	r.NotNil(syncer)
}
