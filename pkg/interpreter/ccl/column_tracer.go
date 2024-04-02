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

package ccl

import (
	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type Context struct {
	GroupByThreshold uint64
}

// ColumnTracer trace column source info
type ColumnTracer struct {
	colToSourceParties map[int64][]string
}

func (t *ColumnTracer) FindSourceParties(uniqueID int64) []string {
	return t.colToSourceParties[uniqueID]
}

func (t *ColumnTracer) AddSourceParties(uniqueID int64, parties []string) {
	if _, exist := t.colToSourceParties[uniqueID]; !exist {
		t.colToSourceParties[uniqueID] = []string{}
	}
	t.colToSourceParties[uniqueID] = append(t.colToSourceParties[uniqueID], parties...)
	t.colToSourceParties[uniqueID] = sliceutil.SliceDeDup(t.colToSourceParties[uniqueID])
}

func (t *ColumnTracer) SetSourcePartiesFromExpr(uniqueID int64, expr expression.Expression) {
	var dataSourceParties []string
	for _, col := range expression.ExtractColumns(expr) {
		parties := t.FindSourceParties(col.UniqueID)
		dataSourceParties = append(dataSourceParties, parties...)
	}
	t.AddSourceParties(uniqueID, dataSourceParties)
}

func NewColumnTracer() *ColumnTracer {
	return &ColumnTracer{
		colToSourceParties: make(map[int64][]string),
	}
}
