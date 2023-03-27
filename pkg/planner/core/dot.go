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

package core

import (
	"fmt"
	"sort"
	"strings"
)

// DrawLogicalPlan draws a logical plan in Graphviz format.
// One can visualize the returned string at http://www.webgraphviz.com/
func DrawLogicalPlan(lp LogicalPlan) string {
	nodeDeps := map[int][]int{}   // node# -> {node#, node#, ...}
	nodeLabel := map[int]string{} // node# -> node_info

	traverseLogicalPlan(lp, nodeDeps, nodeLabel)

	var allNodeIds []int
	for id := range nodeLabel {
		allNodeIds = append(allNodeIds, id)
	}
	sort.Ints(allNodeIds)

	var builder strings.Builder

	fmt.Fprintln(&builder, "digraph G {")
	for _, id := range allNodeIds {
		fmt.Fprintf(&builder, "%d [label=\"%s\"]\n", id, nodeLabel[id])
	}
	for _, id := range allNodeIds {
		if cs, ok := nodeDeps[id]; ok {
			for _, c := range cs {
				fmt.Fprintf(&builder, "%d -> %d\n", id, c)
			}
		}
	}
	fmt.Fprint(&builder, "}")

	return builder.String()
}

func traverseLogicalPlan(lp LogicalPlan, nodeDeps map[int][]int, nodeLabel map[int]string) {
	if _, exist := nodeDeps[lp.ID()]; !exist && len(lp.Children()) > 0 {
		var cid []int
		for _, c := range lp.Children() {
			cid = append(cid, c.ID())
		}
		nodeDeps[lp.ID()] = cid
	}

	if _, exist := nodeLabel[lp.ID()]; !exist {
		var str string
		switch x := lp.(type) {
		case *LogicalApply:
			str = x.JoinType.String()
			var conds []string
			if len(x.EqualConditions) > 0 {
				conds = append(conds, fmt.Sprintf("%s,", x.EqualConditions))
			}
			if len(x.LeftConditions) > 0 {
				conds = append(conds, fmt.Sprintf("l%s,", x.LeftConditions))
			}
			if len(x.RightConditions) > 0 {
				conds = append(conds, fmt.Sprintf("r%s,", x.RightConditions))
			}
			if len(x.OtherConditions) > 0 {
				conds = append(conds, fmt.Sprintf("o%s,", x.OtherConditions))
			}
			if len(conds) > 0 {
				str += "(" + strings.Join(conds, ",") + ")"
			}
		case *LogicalJoin:
			str = x.JoinType.String()
			var conds []string
			if len(x.EqualConditions) > 0 {
				conds = append(conds, fmt.Sprintf("%s,", x.EqualConditions))
			}
			if len(x.LeftConditions) > 0 {
				conds = append(conds, fmt.Sprintf("l%s,", x.LeftConditions))
			}
			if len(x.RightConditions) > 0 {
				conds = append(conds, fmt.Sprintf("r%s,", x.RightConditions))
			}
			if len(x.OtherConditions) > 0 {
				conds = append(conds, fmt.Sprintf("o%s,", x.OtherConditions))
			}
			if len(conds) > 0 {
				str += "(" + strings.Join(conds, ",") + ")"
			}
		case *DataSource:
			str = ToString(x)
		case *LogicalProjection:
			str = fmt.Sprintf("Projection(%s)", x.Exprs)
		case *LogicalSelection:
			str = fmt.Sprintf("Sel(%s)", x.Conditions)
		case *LogicalAggregation:
			str = fmt.Sprintf("Agg(%s)", x.AggFuncs)
		case *LogicalWindow:
			str = fmt.Sprintf("Window(%s)", x.Schema().Columns)
		default:
			str = lp.TP()
		}
		nodeLabel[lp.ID()] = str
	}

	for _, child := range lp.Children() {
		traverseLogicalPlan(child, nodeDeps, nodeLabel)
	}
}
