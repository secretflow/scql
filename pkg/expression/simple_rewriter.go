// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/types"
)

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, errNonUniq.GenWithStackByArgs(name.String(), "field list")
			}
		}
	}
	return idx, nil
}

// FindFieldNameIdxByColName finds the index of corresponding name in the given slice. -1 for not found.
func FindFieldNameIdxByColName(names []*types.FieldName, colName string) int {
	for i, name := range names {
		if name.ColName.L == colName {
			return i
		}
	}
	return -1
}
