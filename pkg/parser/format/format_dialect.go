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
	"fmt"

	"github.com/secretflow/scql/pkg/parser/mysql"
)

const maxCSVDecimalWidth = 38

var (
	_ Dialect = &MySQLDialect{}
	_ Dialect = &TiDBDialect{}
	_ Dialect = &PostgresDialect{}
	_ Dialect = &CVSDBDialect{}
	_ Dialect = &OdpsDialect{}
)

type Dialect interface {
	ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error)
	SkipSchemaInColName() bool
	// special means functions which have different function names in different backends
	// If the function has been renamed inside the function, return the renamed name; otherwise,
	// return the original name. If you are sure that the function name is the same in all backends,
	// please use the original name directly; otherwise, please call this function.
	// Of course, you can also override the differences in function names of different backend databases within this function.
	GetSpecialFuncName(string) string
	GetOperator(string) string
	// need parentheses or not for continuous comparison operator
	// such as: a = b = c
	// for mysql a = b = c
	// for pg (a = b) = c
	NeedParenthesesForCmpOperand() bool
}

func NewMySQLDialect() Dialect {
	return &MySQLDialect{}
}

type MySQLDialect struct {
}

func (d *MySQLDialect) SkipSchemaInColName() bool {
	return true
}

func (d *MySQLDialect) ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error) {
	unspecifiedLength := -1
	switch asType {
	case mysql.TypeVarString, mysql.TypeVarchar:
		keyword = "CHAR"
		if flen != unspecifiedLength {
			plainWord = fmt.Sprintf("(%d)", flen)
		}
	case mysql.TypeDate:
		keyword = "DATE"
	case mysql.TypeDatetime:
		keyword = "DATETIME"
		if decimal > 0 {
			plainWord = fmt.Sprintf("(%d)", decimal)
		}
	case mysql.TypeNewDecimal:
		keyword = "DECIMAL"
		if flen > 0 && decimal > 0 {
			plainWord = fmt.Sprintf("(%d, %d)", flen, decimal)
		} else if flen > 0 {
			plainWord = fmt.Sprintf("(%d)", flen)

		}
	case mysql.TypeDuration:
		keyword = "TIME"
		if decimal > 0 {
			plainWord = fmt.Sprintf("(%d)", decimal)
		}
	case mysql.TypeLonglong:
		if flag&mysql.UnsignedFlag != 0 {
			keyword = "UNSIGNED"
		} else {
			keyword = "SIGNED"
		}
	case mysql.TypeJSON:
		keyword = "JSON"
	case mysql.TypeDouble:
		keyword = "DECIMAL"
		plainWord = "(64,30)"
	case mysql.TypeFloat:
		keyword = "DECIMAL"
		plainWord = "(64,30)"
	default:
		err = fmt.Errorf("unsupported cast as data type: %+v", asType)
	}
	return
}

func (d *MySQLDialect) GetSpecialFuncName(originName string) string {
	return originName
}

func (d *MySQLDialect) GetOperator(op string) string {
	return op
}

func (d *MySQLDialect) NeedParenthesesForCmpOperand() bool {
	return false
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

func (d *TiDBDialect) SkipSchemaInColName() bool {
	return false
}

func (d *TiDBDialect) ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error) {
	unspecifiedLength := -1
	switch asType {
	case mysql.TypeVarString, mysql.TypeVarchar:
		keyword = "CHAR"
		if flen != unspecifiedLength {
			plainWord = fmt.Sprintf("(%d)", flen)
		}
	case mysql.TypeDate:
		keyword = "DATE"
	case mysql.TypeDatetime:
		keyword = "DATETIME"
		if decimal > 0 {
			plainWord = fmt.Sprintf("(%d)", decimal)
		}
	case mysql.TypeNewDecimal:
		keyword = "DECIMAL"
		if flen > 0 && decimal > 0 {
			plainWord = fmt.Sprintf("(%d, %d)", flen, decimal)
		} else if flen > 0 {
			plainWord = fmt.Sprintf("(%d)", flen)

		}
	case mysql.TypeDuration:
		keyword = "TIME"
		if decimal > 0 {
			plainWord = fmt.Sprintf("(%d)", decimal)
		}
	case mysql.TypeLonglong:
		if flag&mysql.UnsignedFlag != 0 {
			keyword = "UNSIGNED"
		} else {
			keyword = "SIGNED"
		}
	case mysql.TypeJSON:
		keyword = "JSON"
	case mysql.TypeDouble:
		keyword = "DOUBLE"
	case mysql.TypeFloat:
		keyword = "FLOAT"
	default:
		err = fmt.Errorf("unsupported cast as data type: %+v", asType)
	}
	return
}

type PostgresDialect struct {
	MySQLDialect
	funcNameMap map[string]string
}

func NewPostgresDialect() Dialect {
	return &PostgresDialect{
		funcNameMap: map[string]string{
			// don't use package ast here, may cause circle dependency
			"ifnull":   "coalesce",
			"truncate": "trunc",
			"curdate":  "current_date",
			"adddate":  "+",
			"subdate":  "-",
			"datediff": "-",
			"intdiv":   "div",
		},
	}
}

func (d *PostgresDialect) ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error) {
	switch asType {
	case mysql.TypeNewDecimal:
		keyword = "NUMERIC"
		if flen > 0 && decimal > 0 {
			plainWord = fmt.Sprintf("(%d, %d)", flen, decimal)
		} else if flen > 0 {
			plainWord = fmt.Sprintf("(%d)", flen)

		}
	case mysql.TypeDouble, mysql.TypeFloat:
		keyword = "DOUBLE PRECISION"
	case mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeShort:
		if flag&mysql.UnsignedFlag != 0 {
			err = fmt.Errorf("unsupported cast as data type %+v", asType)
			return
		} else {
			keyword = "INTEGER"
		}
	case mysql.TypeVarString, mysql.TypeVarchar:
		keyword = "VARCHAR"
	case mysql.TypeDate:
		keyword = "DATE"
	// there is no datetime in pg
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		keyword = "TIMESTAMP"
	case mysql.TypeDuration:
		keyword = "INTERVAL"
	case mysql.TypeTiny:
		keyword = "BOOLEAN"
	default:
		err = fmt.Errorf("unsupported cast as data type: %+v", asType)
	}
	return
}

func (d *PostgresDialect) SkipSchemaInColName() bool {
	return true
}

func (d *PostgresDialect) GetSpecialFuncName(originName string) string {
	if res, ok := d.funcNameMap[originName]; ok {
		return res
	}
	return originName
}

func (d *PostgresDialect) NeedParenthesesForCmpOperand() bool {
	return true
}

type CVSDBDialect struct {
	PostgresDialect
	funcNameMap map[string]string
	operatorMap map[string]string
}

func NewCVSDBDialect() Dialect {
	return &CVSDBDialect{
		funcNameMap: map[string]string{
			// don't use package ast here, may cause circle dependency
			"ifnull":   "coalesce",
			"truncate": "trunc",
			"now":      "now",
			"curdate":  "current_date",
			"adddate":  "+",
			"subdate":  "-",
			"datediff": "-",
		},
		operatorMap: map[string]string{
			// ref to https://duckdb.org/docs/archive/0.9/sql/functions/numeric
			" DIV ": " // ",
		},
	}
}

func (d *CVSDBDialect) ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error) {
	switch asType {
	case mysql.TypeNewDecimal:
		keyword = "NUMERIC"
		// ref: https://github.com/duckdb/duckdb/blob/v0.9.2/src/parser/transform/helpers/transform_typename.cpp#L167
		if flen <= maxCSVDecimalWidth {
			if flen > 0 && decimal > 0 {
				plainWord = fmt.Sprintf("(%d, %d)", flen, decimal)
			} else if flen > 0 {
				plainWord = fmt.Sprintf("(%d)", flen)
			}
		}
	default:
		return d.PostgresDialect.ConvertCastTypeToString(asType, flen, decimal, flag)
	}
	return
}

func (d *CVSDBDialect) SkipSchemaInColName() bool {
	return true
}

func (d *CVSDBDialect) GetSpecialFuncName(originName string) string {
	if res, ok := d.funcNameMap[originName]; ok {
		return res
	}
	return originName
}

func (d *CVSDBDialect) GetOperator(originName string) string {
	if res, ok := d.operatorMap[originName]; ok {
		return res
	}
	return originName
}

type OdpsDialect struct {
	MySQLDialect
	funcNameMap map[string]string
}

func NewOdpsDialect() Dialect {
	return &OdpsDialect{
		funcNameMap: map[string]string{
			"truncate": "trunc",
			"ifnull":   "nvl",
		},
	}
}

func (d *OdpsDialect) SkipSchemaInColName() bool {
	return true
}

func (d *OdpsDialect) GetSpecialFuncName(originName string) string {
	if res, ok := d.funcNameMap[originName]; ok {
		return res
	}
	return originName
}

func (d *OdpsDialect) ConvertCastTypeToString(asType byte, flen int, decimal int, flag uint) (keyword string, plainWord string, err error) {
	switch asType {
	case mysql.TypeVarString, mysql.TypeVarchar:
		keyword = "STRING"
	case mysql.TypeNewDecimal:
		// odps don't support decimal, so we use double replace decimal(xx,xx)
		keyword = "BIGINT"
		if flen > 0 && decimal > 0 {
			keyword = "DECIMAL"
		}
	case mysql.TypeLonglong:
		if flag&mysql.UnsignedFlag != 0 {
			err = fmt.Errorf("unsupported cast as data type %+v", asType)
			return
		} else {
			keyword = "BIGINT"
		}
	case mysql.TypeFloat:
		keyword = "DOUBLE"
	case mysql.TypeDouble:
		keyword = "DOUBLE"
	default:
		return d.MySQLDialect.ConvertCastTypeToString(asType, flen, decimal, flag)
	}
	return
}

func (d *OdpsDialect) NeedParenthesesForCmpOperand() bool {
	return true
}
