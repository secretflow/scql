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

package core

import (
	"bytes"
	"fmt"
	"strings"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	switch x := in.(type) {
	case LogicalPlan:
		if len(x.Children()) > 1 {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	}

	var str string
	switch x := in.(type) {
	case *LogicalJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]

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
	case *LogicalUnionAll:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "UnionAll{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *DataSource:
		if x.TableAsName != nil && x.TableAsName.L != "" {
			str = fmt.Sprintf("DataScan(%s)", x.TableAsName)
		} else {
			str = fmt.Sprintf("DataScan(%s)", x.tableInfo.Name)
		}
	case *LogicalProjection:
		str = fmt.Sprintf("Projection(%s)", x.Exprs)
	case *LogicalSelection:
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *LogicalSort:
		str = "Sort("
		for i, byItem := range x.ByItems {
			str += byItem.String()
			if i != len(x.ByItems)-1 {
				str += ","
			}
		}
		str += ")"
	case *LogicalLimit:
		str = "Limit"
	case *LogicalAggregation:
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			str += aggFunc.String()
			if i != len(x.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *LogicalApply:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"

		var conds []string
		if len(x.EqualConditions) > 0 {
			conds = append(conds, fmt.Sprintf("%s", x.EqualConditions))
		}
		if len(x.LeftConditions) > 0 {
			conds = append(conds, fmt.Sprintf("l%s", x.LeftConditions))
		}
		if len(x.RightConditions) > 0 {
			conds = append(conds, fmt.Sprintf("r%s", x.RightConditions))
		}
		if len(x.OtherConditions) > 0 {
			conds = append(conds, fmt.Sprintf("o%s", x.OtherConditions))
		}
		if len(conds) > 0 {
			str += "(" + strings.Join(conds, ",") + ")"
		}
	case *LogicalWindow:
		buffer := bytes.NewBufferString("")
		formatWindowFuncDescs(buffer, x.WindowFuncDescs, x.schema)
		str = fmt.Sprintf("Window(%s)", buffer.String())
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
