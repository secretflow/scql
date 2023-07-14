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

	"github.com/secretflow/scql/pkg/planner/core"
)

// ToString explains a Plan, returns description string.
func ToString(p logicalNode, skipDataSource bool) string {
	strs, _ := toString(p, []string{}, []int{}, skipDataSource)
	return strings.Join(strs, "->")
}

func toString(in logicalNode, strs []string, idxs []int, skipDataSource bool) ([]string, []int) {
	if len(in.Children()) > 1 {
		idxs = append(idxs, len(strs))
	}

	for _, c := range in.Children() {
		strs, idxs = toString(c, strs, idxs, skipDataSource)
	}

	var str string
	switch x := in.(type) {
	case *JoinNode:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "; ") + "}"
		idxs = idxs[:last]

		var conds []string
		join, ok := x.LP().(*core.LogicalJoin)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		if len(join.EqualConditions) > 0 {
			conds = append(conds, fmt.Sprintf("%s,", join.EqualConditions))
		}
		if len(join.LeftConditions) > 0 {
			conds = append(conds, fmt.Sprintf("l%s,", join.LeftConditions))
		}
		if len(join.RightConditions) > 0 {
			conds = append(conds, fmt.Sprintf("r%s,", join.RightConditions))
		}
		if len(join.OtherConditions) > 0 {
			conds = append(conds, fmt.Sprintf("o%s,", join.OtherConditions))
		}
		if len(conds) > 0 {
			str += "(" + strings.Join(conds, ",") + ")"
		}
	case *UnionAllNode:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "UnionAll{" + strings.Join(children, "; ") + "}"
		idxs = idxs[:last]
	case *DataSourceNode:
		source, ok := x.LP().(*core.DataSource)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		if source.TableAsName != nil && source.TableAsName.L != "" {
			str = fmt.Sprintf("DataScan(%s)", source.TableAsName)
		} else {
			str = fmt.Sprintf("DataScan(%s)", source.TableInfo().Name)
		}
	case *ProjectionNode:
		projection, ok := x.LP().(*core.LogicalProjection)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		str = fmt.Sprintf("Projection(%s)", projection.Exprs)
	case *SelectionNode:
		selection, ok := x.LP().(*core.LogicalSelection)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		str = fmt.Sprintf("Sel(%s)", selection.Conditions)
		str += fmt.Sprintf("(cond: %s)", x.condVis.String())
	case *AggregationNode:
		agg, ok := x.LP().(*core.LogicalAggregation)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		str = "Aggr("
		for i, aggFunc := range agg.AggFuncs {
			str += aggFunc.String()
			if i != len(agg.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *ApplyNode:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
		apply, ok := x.LP().(*core.LogicalApply)
		if !ok {
			str = fmt.Sprintf("%T", in)
			break
		}
		var conds []string
		if len(apply.EqualConditions) > 0 {
			conds = append(conds, fmt.Sprintf("%s", apply.EqualConditions))
		}
		if len(apply.LeftConditions) > 0 {
			conds = append(conds, fmt.Sprintf("l%s", apply.LeftConditions))
		}
		if len(apply.RightConditions) > 0 {
			conds = append(conds, fmt.Sprintf("r%s", apply.RightConditions))
		}
		if len(apply.OtherConditions) > 0 {
			conds = append(conds, fmt.Sprintf("o%s", apply.OtherConditions))
		}
		if len(conds) > 0 {
			str += "(" + strings.Join(conds, ",") + ")"
		}
	default:
		str = fmt.Sprintf("%T", in)
	}
	if _, ok := in.LP().(*core.DataSource); !ok || !skipDataSource {
		// add ccl string for columns
		str += "("
		ccls := in.CCL()
		for _, col := range in.Schema().Columns {
			str += fmt.Sprintf("%s:%s,", col.String(), ccls[col.UniqueID].String())
		}
		str += ")"
	}

	strs = append(strs, str)
	return strs, idxs
}
