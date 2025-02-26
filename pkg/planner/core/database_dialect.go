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
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/parser/format"
)

type DBType int

const (
	DBTypeUnknown DBType = iota
	DBTypeMySQL
	DBTypeSQLite
	DBTypePostgres
	DBTypeCSVDB
	DBTypeODPS
)

var dbTypeMap = map[string]DBType{
	"mysql":      DBTypeMySQL,
	"sqlite":     DBTypeSQLite,
	"postgresql": DBTypePostgres,
	"csvdb":      DBTypeCSVDB,
	"odps":       DBTypeODPS,
}

var dbTypeNameMap = map[DBType]string{
	DBTypeUnknown:  "unknown",
	DBTypeMySQL:    "mysql",
	DBTypeSQLite:   "sqlite",
	DBTypePostgres: "postgresql",
	DBTypeCSVDB:    "csvdb",
	DBTypeODPS:     "odps",
}

func (t DBType) String() string {
	if name, exists := dbTypeNameMap[t]; exists {
		return name
	}
	return "unknown"
}

func ParseDBType(tp string) (DBType, error) {
	if v, ok := dbTypeMap[strings.ToLower(tp)]; ok {
		return v, nil
	}
	return DBTypeUnknown, fmt.Errorf("unknown db type: %s", tp)
}

var (
	_ Dialect = &MySQLDialect{}
	_ Dialect = &PostgresDialect{}
	_ Dialect = &CVSDBDialect{}
	_ Dialect = &OdpsDialect{}
)

var (
	DBDialectMap = map[DBType]Dialect{
		DBTypeUnknown:  NewMySQLDialect(),
		DBTypeCSVDB:    NewCVSDBDialect(),
		DBTypeMySQL:    NewMySQLDialect(),
		DBTypePostgres: NewPostgresDialect(),
		DBTypeSQLite:   NewMySQLDialect(),
		DBTypeODPS:     NewOdpsDialect(),
	}
)

type Dialect interface {
	GetRestoreFlags() format.RestoreFlags
	GetFormatDialect() format.Dialect
	SupportAnyValue() bool
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

func (d *MySQLDialect) SupportAnyValue() bool {
	return true
}

type PostgresDialect struct {
	MySQLDialect
}

func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{
		MySQLDialect{flags: format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase, formatDialect: format.NewPostgresDialect()},
	}
}

func (d *PostgresDialect) GetRestoreFlags() format.RestoreFlags {
	return d.flags
}

func (d *PostgresDialect) GetFormatDialect() format.Dialect {
	return d.formatDialect
}

func (d *PostgresDialect) SupportAnyValue() bool {
	return false
}

type CVSDBDialect struct {
	MySQLDialect
}

func NewCVSDBDialect() *CVSDBDialect {
	return &CVSDBDialect{
		MySQLDialect{flags: format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameDoubleQuotes, formatDialect: format.NewCVSDBDialect()},
	}
}

func (d *CVSDBDialect) GetRestoreFlags() format.RestoreFlags {
	return d.flags
}

func (d *CVSDBDialect) GetFormatDialect() format.Dialect {
	return d.formatDialect
}

func (d *CVSDBDialect) SupportAnyValue() bool {
	return false
}

type OdpsDialect struct {
	MySQLDialect
}

func NewOdpsDialect() *OdpsDialect {
	return &OdpsDialect{
		MySQLDialect{flags: format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase, formatDialect: format.NewOdpsDialect()},
	}
}

func (d *OdpsDialect) GetRestoreFlags() format.RestoreFlags {
	return d.flags
}

func (d *OdpsDialect) GetFormatDialect() format.Dialect {
	return d.formatDialect
}

func (d *OdpsDialect) SupportAnyValue() bool {
	return false
}
