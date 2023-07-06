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

package core

import (
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/proto-gen/grm"
)

var (
	_ Dialect = &MySQLDialect{}
	_ Dialect = &PostgresDialect{}
)

var (
	DBDialectMap = map[grm.DataSourceKind]Dialect{
		grm.DataSourceKind_UNKNOWN:    NewMySQLDialect(),
		grm.DataSourceKind_CSVDB:      NewPostgresDialect(),
		grm.DataSourceKind_MYSQL:      NewMySQLDialect(),
		grm.DataSourceKind_POSTGRESQL: NewPostgresDialect(),
		grm.DataSourceKind_SQLITE:     NewMySQLDialect(),
	}
)

type Dialect interface {
	GetRestoreFlags() format.RestoreFlags
	GetFormatDialect() format.Dialect
	IsSupportAnyValue() bool
}

type MySQLDialect struct {
	flags         format.RestoreFlags
	formatDialect format.Dialect
}

func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{
		flags:         format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase,
		formatDialect: format.NewMySQLDialect(),
	}
}

func (d *MySQLDialect) GetRestoreFlags() format.RestoreFlags {
	return d.flags
}

func (d *MySQLDialect) GetFormatDialect() format.Dialect {
	return d.formatDialect
}

func (d *MySQLDialect) IsSupportAnyValue() bool {
	return true
}

type PostgresDialect struct {
	flags         format.RestoreFlags
	formatDialect format.Dialect
}

func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{
		flags:         format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase,
		formatDialect: format.NewPostgresDialect(),
	}
}

func (d *PostgresDialect) GetRestoreFlags() format.RestoreFlags {
	return d.flags
}

func (d *PostgresDialect) GetFormatDialect() format.Dialect {
	return d.formatDialect
}

func (d *PostgresDialect) IsSupportAnyValue() bool {
	return false
}
