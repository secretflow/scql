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

package grm

import (
	grmproto "github.com/secretflow/scql/pkg/proto-gen/grm"
)

type DatabaseType = grmproto.DataSourceKind

const (
	DBUnknown  DatabaseType = grmproto.DataSourceKind_UNKNOWN
	DBMySQL    DatabaseType = grmproto.DataSourceKind_MYSQL
	DBSQLite   DatabaseType = grmproto.DataSourceKind_SQLITE
	DBPostgres DatabaseType = grmproto.DataSourceKind_POSTGRESQL
	DBCSV      DatabaseType = grmproto.DataSourceKind_CSVDB
)

type ColumnDesc struct {
	Name        string
	Type        string
	Description string
}

type TableSchema struct {
	DbName    string
	TableName string
	Columns   []*ColumnDesc
	DBType    DatabaseType
}

type EngineInfo struct {
	Endpoints  []string
	Credential []string
}

type Grm interface {
	GetTableMeta(tid, requestParty, token string) (*TableSchema, error)
	GetEngines(partyCodes []string, token string) ([]*EngineInfo, error)
	VerifyTableOwnership(tid, token string) (bool, error)
}
