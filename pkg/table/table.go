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

// Table is used to retrieve and modify rows in table.
type Table interface {
	// Cols returns the columns of the table which is used in select, including hidden columns.
	Cols() []*Column
	// Meta returns TableInfo.
	Meta() *model.TableInfo
}

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table
