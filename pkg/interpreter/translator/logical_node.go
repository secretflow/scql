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

package translator

import (
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/types"
)

// logicalNode is a wrapper on planner/core:LogicalPlan.
// It adds secure computation info such as visible parties, etc.
type logicalNode interface {
	// Methods of core.LogicalPlan
	ID() int
	Type() string
	LP() core.LogicalPlan
	Children() []logicalNode
	Schema() *expression.Schema
	OutputNames() types.NameSlice
	String() string
	IntoOpt() *ast.SelectIntoOption
	InsertTableOpt() *core.InsertTableOption
	// buildCCL builds the column control list of logical node's table
	buildCCL(*ccl.Context, *ccl.ColumnTracer) error
	// CCL returns the column control list of the logical node's table
	CCL() map[int64]*ccl.CCL
	// VisibleParty returns a list of parties that can see the logical node's table
	VisibleParty() []string
	// DataSourceParty returns a list of parties that logical node's table comes from
	DataSourceParty() []string
	SetResultTableWithDTypeCheck([]*graph.Tensor) error
	ResultTable() []*graph.Tensor

	FindTensorByColumnId(int64) (*graph.Tensor, error)
}

type baseNode struct {
	lp                core.LogicalPlan
	children          []logicalNode
	rt                []*graph.Tensor
	ccl               map[int64]*ccl.CCL
	dataSourceParties []string
}

func (n *baseNode) String() string {
	return core.ToString(n.lp)
}

func (n *baseNode) ID() int {
	return n.lp.ID()
}

func (n *baseNode) Type() string {
	return n.lp.TP()
}

func (n *baseNode) Schema() *expression.Schema {
	return n.lp.Schema()
}

func (n *baseNode) OutputNames() types.NameSlice {
	return n.lp.OutputNames()
}

func (n *baseNode) Children() []logicalNode {
	return n.children
}

func (n *baseNode) IntoOpt() *ast.SelectIntoOption {
	return n.lp.IntoOpt()
}

func (n *baseNode) InsertTableOpt() *core.InsertTableOption {
	return n.lp.InsertTableOpt()
}

func (n *baseNode) VisibleParty() []string {
	var ccls []*ccl.CCL
	for _, cc := range n.CCL() {
		ccls = append(ccls, cc)
	}
	partyCodes := ccl.ExtractPartyCodes(ccls)
	var visibleParties []string
	for _, party := range partyCodes {
		visible := true
		for _, cc := range n.CCL() {
			if !cc.IsVisibleFor(party) {
				visible = false
				break
			}
		}
		if visible {
			visibleParties = append(visibleParties, party)
		}
	}
	return visibleParties
}

func (n *baseNode) DataSourceParty() []string {
	return n.dataSourceParties
}

func (n *baseNode) SetResultTableWithDTypeCheck(rt []*graph.Tensor) error {
	// check tensor date type matches the logical node's column data type
	for i, t := range rt {
		col := n.Schema().Columns[i]
		expectTp, err := convertDataType(col.RetType)
		if err != nil {
			return fmt.Errorf("fail to set result table: %v", err)
		}
		if !t.SkipDTypeCheck && t.DType != expectTp {
			return fmt.Errorf("fail to set result table: tensor %v type(=%v) doesn't match scheme type(=%v) in node %v ",
				t.UniqueName(), t.DType, expectTp, n.Type())
		}
		t.CC = n.ccl[col.UniqueID]
	}

	n.rt = rt
	return nil
}

func (n *baseNode) ResultTable() []*graph.Tensor {
	return n.rt
}

func (n *baseNode) CCL() map[int64]*ccl.CCL {
	return n.ccl
}

func (n *baseNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("please implement this function in child")
}

func (n *baseNode) FindTensorByColumnId(id int64) (*graph.Tensor, error) {
	if n.rt == nil || len(n.rt) != len(n.Schema().Columns) {
		return nil, fmt.Errorf("findTensorByColumnId: invalid result table %v", n.rt)
	}
	for i, col := range n.Schema().Columns {
		if col.UniqueID == id {
			return n.rt[i], nil
		}
	}
	return nil, fmt.Errorf("findTensorByColumnId: column id %v doesn't exists", id)
}

func (n *baseNode) LP() core.LogicalPlan {
	return n.lp
}

type JoinNode struct {
	baseNode
	// record child data source party
	childDataSourceParties [][]string
}

type SelectionNode struct {
	baseNode
	condVis *ccl.CCL
}

type DataSourceNode struct {
	baseNode
	originCCL map[string]*ccl.CCL
}

type ProjectionNode struct {
	baseNode
}

type ApplyNode struct {
	baseNode
}

type AggregationNode struct {
	baseNode
}

type UnionAllNode struct {
	baseNode
}

type LimitNode struct {
	baseNode
}

type SortNode struct {
	baseNode
}

type WindowNode struct {
	baseNode
}
