// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package tables

import (
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/table"
)

// TableCommon is shared by both Table and partition.
type TableCommon struct {
	tableID int64
	Columns []*table.Column
	meta    *model.TableInfo
}

// Cols implements table.Table Cols interface.
func (t *TableCommon) Cols() []*table.Column {
	return t.Columns
}

// Meta implements table.Table Meta interface.
func (t *TableCommon) Meta() *model.TableInfo {
	return t.meta
}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tblInfo *model.TableInfo) table.Table {
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns)
	return &t
}

// initTableCommon initializes a TableCommon struct.
func initTableCommon(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column) {
	t.tableID = tblInfo.ID
	t.meta = tblInfo
	t.Columns = cols
}

func init() {
	table.MockTableFromMeta = MockTableFromMeta
}
