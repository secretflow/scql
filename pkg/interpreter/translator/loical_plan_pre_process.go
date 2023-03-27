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
	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/planner/core"
)

type LpPrePocessor struct{}

func (m *LpPrePocessor) process(lp core.LogicalPlan) error {
	if err := m.addThreshold(nil, lp, 0); err != nil {
		return err
	}
	err := m.mergeSelection(lp)
	return err
}

// addThreshold add selection between lp and flp(father of lp)
func (m *LpPrePocessor) addThreshold(flp core.LogicalPlan, lp core.LogicalPlan, index int) error {
	for i, childLp := range lp.Children() {
		if err := m.addThreshold(lp, childLp, i); err != nil {
			return err
		}
	}
	if flp == nil {
		return nil
	}
	if agg, ok := lp.(*core.LogicalAggregation); ok {
		for i, f := range agg.AggFuncs {
			if f.UseAsThreshold {
				col := agg.Schema().Columns[i]
				col.UseAsThreshold = true
				core.AddSelection(flp, lp, []expression.Expression{col}, index)
			}
		}
	}
	return nil
}

// mergeSelection merges lp and it's father when they both are selection
func (m *LpPrePocessor) mergeSelection(lp core.LogicalPlan) error {
	if selection, ok := lp.(*core.LogicalSelection); ok && len(selection.Children()) == 1 {
		if childSelection, ok := lp.Children()[0].(*core.LogicalSelection); ok {
			selection.Conditions = append(selection.Conditions, childSelection.Conditions...)
			selection.Children()[0] = childSelection.Children()[0]
		}
	}
	for _, childLp := range lp.Children() {
		if err := m.mergeSelection(childLp); err != nil {
			return err
		}
	}
	return nil
}
