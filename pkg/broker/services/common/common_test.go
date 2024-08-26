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

package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/message"
	prom "github.com/secretflow/scql/pkg/util/prometheus"
)

func TestVerifyTableMeta(t *testing.T) {
	r := require.New(t)
	mockMeta := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{
				ProjectID: "test_db",
				TableName: "test_t",
			},
			DBType:   "MYSQL",
			RefTable: "d1.ta",
		},
		Columns: []storage.ColumnMeta{
			{
				ColumnName: "c1",
				DType:      "int",
			},
			{
				ColumnName: "c2",
				DType:      "float",
			},
			{
				ColumnName: "c3",
				DType:      "string",
			},
		},
	}
	r.NoError(VerifyTableMeta(mockMeta))
	// set illegal table name
	mockMeta.Table.TableName = "@test_t"
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Table.TableName = " test_t"
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Table.TableName = "test_t "
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Table.TableName = "test t "
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Table.TableName = "test_t"

	// set illegal project id
	mockMeta.Table.ProjectID = "@test_db"
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Table.ProjectID = "test_db"

	// test uuid as project id
	id, err := application.GenerateProjectID()
	r.NoError(err)
	mockMeta.Table.ProjectID = id
	r.NoError(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Table.ProjectID = "test_db"

	// set illegal column name
	mockMeta.Columns[0].ColumnName = "@c1"
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Columns[0].ColumnName = " c1"
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Columns[0].ColumnName = "c1 "
	r.Error(VerifyTableMeta(mockMeta))
	mockMeta.Columns[0].ColumnName = "c1 c2"
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Columns[0].ColumnName = "c1"

	// set illegal column type
	mockMeta.Columns[0].DType = "decimal"
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Columns[0].DType = "int"

	// set illegal db
	mockMeta.Table.DBType = "Redis"
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Table.DBType = "MYSQL"

	// set illegal ref table
	mockMeta.Table.RefTable = "a1.b1.c1"
	r.Error(VerifyTableMeta(mockMeta))
	// reset
	mockMeta.Table.DBType = "d1.ta"
}

func TestVerifyProjectID(t *testing.T) {
	r := require.New(t)
	r.NoError(VerifyProjectID("test_db"))
	// lex error, parse 4e2 as float
	r.Error(VerifyProjectID("4e2b8e84794811ee9fac047bcba5a41e"))
	r.Error(VerifyProjectID("@test_db"))
	r.Error(VerifyProjectID(" test_db"))
	r.Error(VerifyProjectID("test_db "))
	r.Error(VerifyProjectID(" test_db "))
	r.Error(VerifyProjectID(" test db "))
	r.Error(VerifyProjectID(""))
}

func TestStorageWithCheck(t *testing.T) {
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
	meta := storage.NewMetaManager(db, false)
	err = meta.Bootstrap()
	r.NoError(err)
	transaction := meta.CreateMetaTransaction()
	projectID1 := "p1"
	projectName1 := "n1"
	projectConf, _ := message.ProtoMarshal(&pb.ProjectConfig{
		SpuRuntimeCfg: &spu.RuntimeConfig{
			Protocol: spu.ProtocolKind_SEMI2K,
			Field:    spu.FieldType_FM64,
			TtpBeaverConfig: &spu.TTPBeaverConfig{
				ServerHost: "127.0.0.1",
			},
		},
	})
	alice := "alice"
	bob := "bob"
	// create project
	err = transaction.CreateProject(storage.Project{ID: projectID1, Name: projectName1, ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)
	err = transaction.AddProjectMembers([]storage.Member{storage.Member{ProjectID: projectID1, Member: bob}})
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
	err = AddTableWithCheck(transaction, tableMeta.Table.ProjectID, tableMeta.Table.Owner, tableMeta)
	r.NoError(err)
	tableName2 := "t2"
	t2Identifier := storage.TableIdentifier{ProjectID: projectID1, TableName: tableName2}
	tableMeta = storage.TableMeta{
		Table: storage.Table{TableIdentifier: t2Identifier, RefTable: "real.t2", Owner: bob, DBType: "MYSQL"},
		Columns: []storage.ColumnMeta{
			{ColumnName: "id", DType: "int"},
		},
	}
	// create table
	err = AddTableWithCheck(transaction, tableMeta.Table.ProjectID, tableMeta.Table.Owner, tableMeta)
	r.NoError(err)
	// allow create table repeatedly
	err = AddTableWithCheck(transaction, tableMeta.Table.ProjectID, tableMeta.Table.Owner, tableMeta)
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
	// normal grant
	err = GrantColumnConstraintsWithCheck(transaction, t1Identifier.ProjectID, privsAlice, OwnerChecker{Owner: alice})
	r.NoError(err)
	err = GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, privsBob, OwnerChecker{Owner: bob})
	r.NoError(err)

	tableNotExistPriv := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: "not_exist", ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: "not_exist", ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plaintext"},
	}
	err = GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, tableNotExistPriv, OwnerChecker{Owner: bob})
	r.Error(err)
	r.Equal("GrantColumnConstraintsWithCheck: table [not_exist] not found", err.Error())
	columnNotExistPriv := []storage.ColumnPriv{
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: "column_not_exist", DestParty: alice}, Priv: "plaintext"},
		{ColumnPrivIdentifier: storage.ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: "error_column", DestParty: bob}, Priv: "plaintext"},
	}
	err = GrantColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, columnNotExistPriv, OwnerChecker{Owner: alice})
	r.Error(err)
	r.Equal("GrantColumnConstraintsWithCheck: unknown column name column_not_exist in table t1", err.Error())

	tableNotExistPrivID := []storage.ColumnPrivIdentifier{
		{ProjectID: c2Identifier.ProjectID, TableName: "not_exist", ColumnName: c2Identifier.ColumnName, DestParty: alice},
		{ProjectID: c2Identifier.ProjectID, TableName: "not_exist", ColumnName: c2Identifier.ColumnName, DestParty: bob},
	}
	err = RevokeColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, bob, tableNotExistPrivID)
	r.Error(err)
	r.Equal("RevokeColumnConstraintsWithCheck: table [not_exist] not found", err.Error())
	columnNotExistPrivID := []storage.ColumnPrivIdentifier{
		{ProjectID: c2Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: "column_not_exist", DestParty: alice},
		{ProjectID: c2Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: "error_column", DestParty: bob},
	}
	err = RevokeColumnConstraintsWithCheck(transaction, t2Identifier.ProjectID, alice, columnNotExistPrivID)
	r.Error(err)
	r.Equal("RevokeColumnConstraintsWithCheck: unknown column name column_not_exist in table t1", err.Error())

	errTableMeta := tableMeta
	errTableMeta.Table.ProjectID = "not_exist_project"
	err = AddTableWithCheck(transaction, "not_exist_project", errTableMeta.Table.Owner, errTableMeta)
	r.Error(err)
	r.Equal("AddTableWithCheck: party bob is not in project not_exist_project", err.Error())

	exist, err := DropTableWithCheck(transaction, tableMeta.Table.ProjectID, tableMeta.Table.Owner, tableMeta.Table.TableIdentifier)
	r.NoError(err)
	r.True(exist)
	// drop table duplicated
	exist, err = DropTableWithCheck(transaction, tableMeta.Table.ProjectID, tableMeta.Table.Owner, tableMeta.Table.TableIdentifier)
	r.NoError(err)
	r.False(exist)
}

func TestFeedResponseStatus(t *testing.T) {
	r := require.New(t)
	// test error is nil/response.Status is nil
	response := &pb.InviteToProjectResponse{}
	testCtx, _ := gin.CreateTestContext(nil)
	feedResponseStatus(testCtx, response, nil)
	r.Equal(pb.Code_OK.String(), testCtx.GetString(prom.ResponseStatusKey))
	r.Nil(response.Status)
	// test error is not nil/response.Status is nil
	response = &pb.InviteToProjectResponse{}
	feedResponseStatus(testCtx, response, fmt.Errorf("mock error"))
	r.NotNil(response.Status)
	r.Equal(int32(pb.Code_INTERNAL), response.Status.Code)
	r.Equal(pb.Code_INTERNAL.String(), testCtx.GetString(prom.ResponseStatusKey))
	r.Equal("mock error", response.Status.Message)
	// test error is nil/response.Status is not nil
	response = &pb.InviteToProjectResponse{Status: &pb.Status{Code: int32(pb.Code_BAD_REQUEST), Message: "mock error"}}
	feedResponseStatus(testCtx, response, nil)
	r.NotNil(response.Status)
	r.Equal(int32(pb.Code_BAD_REQUEST), response.Status.Code)
	r.Equal("mock error", response.Status.Message)
	// test error is not nil/response.Status is not nil
	response = &pb.InviteToProjectResponse{Status: &pb.Status{Code: int32(pb.Code_INTERNAL), Message: "from status"}}
	feedResponseStatus(testCtx, response, status.New(pb.Code_BAD_REQUEST, "from error"))
	r.NotNil(response.Status)
	r.Equal(int32(pb.Code_BAD_REQUEST), response.Status.Code)
	r.Equal("from error", response.Status.Message)
	// test status wrapped with other errors
	wrapErr := fmt.Errorf("wrap error: %w", status.New(pb.Code_OK, "origin error"))
	feedResponseStatus(testCtx, response, wrapErr)
	r.Equal(int32(pb.Code_OK), response.Status.Code)
	r.Equal("origin error", response.Status.Message)
}
