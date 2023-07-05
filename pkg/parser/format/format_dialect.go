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

package format

import (
	"strings"
)

var (
	_ Dialect = &MySQLDialect{}
	_ Dialect = &TiDBDialect{}
	_ Dialect = &PostgresDialect{}
)

type Dialect interface {
	GetCastFieldType(string) string
	NeedShowSchema() bool
	GetOffsetKeyWord() string
	// special means functions which have different function names in different backends
	// If the function has been renamed inside the function, return the renamed name; otherwise,
	// return the original name. If you are sure that the function name is the same in all backends,
	// please use the original name directly; otherwise, please call this function.
	// Of course, you can also override the differences in function names of different backend databases within this function.
	GetSpecialFuncName(string) string
}

func NewMySQLDialect() Dialect {
	return &MySQLDialect{}
}

type MySQLDialect struct {
}

func (d *MySQLDialect) NeedShowSchema() bool {
	return false
}

func (d *MySQLDialect) GetCastFieldType(dtype string) string {
	switch strings.ToUpper(dtype) {
	case "FLOAT", "DOUBLE":
		return "DECIMAL(64,30)"
	}
	return dtype
}

func (d *MySQLDialect) GetOffsetKeyWord() string {
	return ","
}

func (d *MySQLDialect) GetSpecialFuncName(originName string) string {
	return originName
}

type TiDBDialect struct {
	MySQLDialect
}

func NewTiDBDialect() Dialect {
	return &TiDBDialect{}
}

func (d *TiDBDialect) GetCastFieldType(dtype string) string {
	return dtype
}

func (d *TiDBDialect) NeedShowSchema() bool {
	return true
}

type PostgresDialect struct {
	MySQLDialect
	FuncNameMap map[string]string
}

func NewPostgresDialect() Dialect {
	return &PostgresDialect{
		FuncNameMap: map[string]string{
			// don't use package ast here, may cause circle dependency
			"ifnull": "coalesce",
		},
	}
}

func (d *PostgresDialect) GetCastFieldType(dtype string) string {
	switch strings.ToUpper(dtype) {
	case "FLOAT", "DOUBLE", "DECIMAL":
		return "numeric"
	}
	return dtype
}

func (d *PostgresDialect) NeedShowSchema() bool {
	return false
}

func (d *PostgresDialect) GetOffsetKeyWord() string {
	return "OFFSET "
}

func (d *PostgresDialect) GetSpecialFuncName(originName string) string {
	if res, ok := d.FuncNameMap[originName]; ok {
		return res
	}
	return originName
}
