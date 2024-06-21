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
	"strings"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
)

type logicalNodeBuilder struct {
	issuerPartyCode  string
	numOfParties     int
	enginesInfo      *graph.EnginesInfo
	columnToParty    map[int64][]string
	originCCL        map[string]*ccl.CCL
	columnTracer     *ccl.ColumnTracer
	groupByThreshold uint64
}

func newLogicalNodeBuilder(issuer string, info *graph.EnginesInfo, ccls map[string]*ccl.CCL, groupByThreshold uint64) (*logicalNodeBuilder, error) {
	return &logicalNodeBuilder{
		issuerPartyCode:  issuer,
		numOfParties:     len(info.GetParties()),
		enginesInfo:      info,
		columnToParty:    map[int64][]string{},
		originCCL:        ccls,
		columnTracer:     ccl.NewColumnTracer(),
		groupByThreshold: groupByThreshold,
	}, nil
}

func (b *logicalNodeBuilder) buildLogicalNode(lp core.LogicalPlan) (logicalNode, error) {
	ln, err := b.build(lp)
	if err != nil {
		return nil, err
	}
	if b.originCCL != nil {
		if err := ln.buildCCL(&ccl.Context{GroupByThreshold: b.groupByThreshold}, b.columnTracer); err != nil {
			return nil, status.New(pb.Code_CCL_CHECK_FAILED, err.Error())
		}
	}
	return ln, nil
}

func (b *logicalNodeBuilder) build(lp core.LogicalPlan) (logicalNode, error) {
	var children []logicalNode
	for _, p := range lp.Children() {
		ln, err := b.build(p)
		if err != nil {
			return nil, err
		}
		children = append(children, ln)
	}
	var ln logicalNode
	// if expect child number is -1, don't check child number
	expectChildNum := -1
	switch x := lp.(type) {
	case *core.DataSource:
		expectChildNum = 0
		dt, err := core.NewDbTableFromString(strings.Join([]string{x.DBName.O, x.TableInfo().Name.O}, "."))
		if err != nil {
			return nil, err
		}
		sourceParty := b.enginesInfo.GetPartyByTable(dt)
		if sourceParty == "" {
			return nil, fmt.Errorf("fail to get party by table: %s", dt.String())
		}
		ln = &DataSourceNode{baseNode{lp: x, children: children, dataSourceParties: []string{sourceParty}}, b.originCCL}
	case *core.LogicalSelection:
		expectChildNum = 1
		condVis := ccl.NewCCL()
		ln = &SelectionNode{baseNode{lp: x, children: children}, condVis}
	case *core.LogicalJoin:
		expectChildNum = 2
		dataSourceParties := make([][]string, 2)
		ln = &JoinNode{baseNode{lp: x, children: children}, dataSourceParties}
	case *core.LogicalApply:
		expectChildNum = 2
		ln = &ApplyNode{baseNode{lp: x, children: children}}
	case *core.LogicalAggregation:
		expectChildNum = 1
		ln = &AggregationNode{baseNode{lp: x, children: children}}
	case *core.LogicalProjection:
		expectChildNum = 1
		ln = &ProjectionNode{baseNode: baseNode{lp: x, children: children}}
	case *core.LogicalUnionAll:
		expectChildNum = -1
		ln = &UnionAllNode{baseNode: baseNode{lp: x, children: children}}
	case *core.LogicalLimit:
		expectChildNum = 1
		ln = &LimitNode{baseNode: baseNode{lp: x, children: children}}
	case *core.LogicalSort:
		expectChildNum = 1
		ln = &SortNode{baseNode: baseNode{lp: x, children: children}}
	case *core.LogicalWindow:
		expectChildNum = 1
		ln = &WindowNode{baseNode: baseNode{lp: x, children: children}}
	default:
		return nil, fmt.Errorf("logicalNodeBuilder.build: unsupported logical plan type %T", lp)
	}
	if expectChildNum == -1 || len(children) == expectChildNum {
		return ln, nil
	}
	return nil, fmt.Errorf("%s has child num %d expected %d", lp.TP(), len(children), expectChildNum)
}
