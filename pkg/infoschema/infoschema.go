// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023

package infoschema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/table"
	"github.com/secretflow/scql/pkg/types"
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	TableByName(schema, table model.CIStr) (table.Table, error)
}

type sortedTables []table.Table

func (s sortedTables) Len() int {
	return len(s)
}

func (s sortedTables) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedTables) Less(i, j int) bool {
	return s[i].Meta().ID < s[j].Meta().ID
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]table.Table
}

const bucketCount = 512

type infoSchema struct {
	schemaMap map[string]*schemaTables

	// sortedTablesBuckets is a slice of sortedTables, a table's bucket index is (tableID % bucketCount).
	sortedTablesBuckets []sortedTables
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	if tbNames, ok := is.schemaMap[schema.O]; ok {
		if t, ok = tbNames.tables[table.O]; ok {
			return
		}
	}
	return nil, fmt.Errorf("TableByName: Table '%s.%s' doesn't exist", schema, table)
}

// MockInfoSchema only serves for test.
func MockInfoSchema(dbTableList map[string][]*model.TableInfo) InfoSchema {
	result := &infoSchema{}
	result.schemaMap = make(map[string]*schemaTables)
	result.sortedTablesBuckets = make([]sortedTables, bucketCount)
	for dbName, tbList := range dbTableList {
		dbInfo := &model.DBInfo{
			ID:     0,
			Name:   model.NewCIStr(dbName),
			Tables: tbList,
		}
		tableNames := &schemaTables{
			dbInfo: dbInfo,
			tables: make(map[string]table.Table),
		}
		result.schemaMap[dbName] = tableNames
		for _, tb := range tbList {
			tbl := table.MockTableFromMeta(tb)
			tableNames.tables[tb.Name.O] = tbl
			bucketIdx := tableBucketIdx(tb.ID)
			result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
		}
	}
	for i := range result.sortedTablesBuckets {
		sort.Sort(result.sortedTablesBuckets[i])
	}

	return result
}

var _ InfoSchema = (*infoSchema)(nil)

func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	tableNames, ok := is.schemaMap[schema.O]
	if !ok {
		return
	}
	return tableNames.dbInfo, true
}

func FromTableSchema(tableSchema []*TableSchema) (InfoSchema, error) {
	result := &infoSchema{
		schemaMap:           make(map[string]*schemaTables), // dbname -> []tables
		sortedTablesBuckets: make([]sortedTables, len(tableSchema)),
	}
	for i, tbl := range tableSchema {
		dbName, tableName := tbl.DbName, tbl.TableName
		tblInfo := &model.TableInfo{
			ID:          int64(i),
			TableId:     fmt.Sprint(i),
			Name:        model.NewCIStr(tableName),
			Columns:     []*model.ColumnInfo{},
			Indices:     []*model.IndexInfo{},
			ForeignKeys: []*model.FKInfo{},
			State:       model.StatePublic,
			PKIsHandle:  false,
		}
		for i, col := range tbl.Columns {
			colTyp := strings.ToLower(col.Type)
			defaultVal, err := TypeDefaultValue(colTyp)
			if err != nil {
				return nil, err
			}
			fieldTp, err := TypeConversion(colTyp)
			if err != nil {
				return nil, err
			}
			colInfo := &model.ColumnInfo{
				ID:                 int64(i),
				Name:               model.NewCIStr(col.Name),
				Offset:             i,
				OriginDefaultValue: defaultVal,
				DefaultValue:       defaultVal,
				DefaultValueBit:    []byte{},
				Dependences:        map[string]struct{}{},
				FieldType:          fieldTp,
				State:              model.StatePublic,
				Comment:            col.Description,
			}
			tblInfo.Columns = append(tblInfo.Columns, colInfo)
		}
		if _, ok := result.schemaMap[dbName]; ok {
			result.schemaMap[dbName].dbInfo.Tables = append(
				result.schemaMap[dbName].dbInfo.Tables, tblInfo)
		} else {
			dbInfo := &model.DBInfo{
				ID:     int64(len(result.schemaMap)),
				Name:   model.NewCIStr(dbName),
				Tables: []*model.TableInfo{tblInfo},
			}
			result.schemaMap[dbName] = &schemaTables{
				dbInfo: dbInfo,
				tables: map[string]table.Table{},
			}
		}
	}
	// fill schema map: tableName -> table
	for dbName := range result.schemaMap {
		for _, tblInfo := range result.schemaMap[dbName].dbInfo.Tables {
			tb := table.MockTableFromMeta(tblInfo)
			result.schemaMap[dbName].tables[tblInfo.Name.O] = tb
		}
	}
	return result, nil
}

func TypeConversion(tp string) (types.FieldType, error) {
	switch {
	case constant.StringTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeString)), nil
	case constant.IntegerTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeLong)), nil
	case constant.FloatTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeFloat)), nil
	case constant.DoubleTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeDouble)), nil
	case constant.DateTimeTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeDatetime)), nil
	case constant.TimeStampTypeAlias[tp]:
		return *(types.NewFieldType(mysql.TypeTimestamp)), nil
	default:
		return *(types.NewFieldType(mysql.TypeString)),
			fmt.Errorf("unknown type in schema: %s", tp)
	}
}

// FieldTypeString converts mysql FieldType to scdb type string
func FieldTypeString(tp types.FieldType) (string, error) {
	switch tp.EvalType() {
	case types.ETString:
		return "string", nil
	case types.ETInt:
		return "int", nil
	case types.ETDatetime:
		return "datetime", nil
	case types.ETTimestamp:
		return "timestamp", nil
	case types.ETReal:
		return "double", nil
	default:
		return "", fmt.Errorf("unknown type: %v", tp)
	}
}

func TypeDefaultValue(tp string) (interface{}, error) {
	switch {
	case constant.StringTypeAlias[tp]:
		return "", nil
	case constant.IntegerTypeAlias[tp]:
		return 0, nil
	case constant.FloatTypeAlias[tp]:
		return 0.0, nil
	case constant.DoubleTypeAlias[tp]:
		return 0.0, nil
	case constant.DateTimeTypeAlias[tp]:
		// in mysql default value is current time, this place we set to min datetime
		return "1000-01-01 00:00:00", nil
	case constant.TimeStampTypeAlias[tp]:
		// 1970-01-01 00:00:01.000000
		return 0, nil
	default:
		return nil, fmt.Errorf("unknown type in schema: %s", tp)
	}
}
