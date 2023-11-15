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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/storage"
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
}
