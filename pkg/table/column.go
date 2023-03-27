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

package table

import (
	"github.com/secretflow/scql/pkg/parser/model"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
}

// ToColumn converts a *model.ColumnInfo to *Column.
func ToColumn(col *model.ColumnInfo) *Column {
	return &Column{
		col,
	}
}

func (c *Column) ToInfo() *model.ColumnInfo {
	return c.ColumnInfo
}

// ColDescFieldNames returns the fields name in result set for desc and show columns.
func ColDescFieldNames(full bool) []string {
	if full {
		return []string{"Field", "Type", "Comment"}
	}
	return []string{"Field", "Type"}
}
