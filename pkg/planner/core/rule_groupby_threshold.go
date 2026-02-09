// Copyright 2025 Ant Group Co., Ltd.
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
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

type groupbyThresholdApplier struct {
}

func (g *groupbyThresholdApplier) name() string {
	return "add_groupby_threshold"
}

func getThreshold(p LogicalPlan) int {
	sctx := p.SCtx()
	threshold := sctx.GetSessionVars().GroupByThreshold
	if threshold == 0 {
		logrus.Warn("group by threshold is 0, use default value 4")
		return 4
	}
	return int(threshold)
}

func (g *groupbyThresholdApplier) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	threshold := getThreshold(p)

	if threshold == 1 {
		return p, nil
	}

	if err := g.markAggCountFunc(p); err != nil {
		return p, err
	}

	if err := g.addThreshold(nil, p, 0); err != nil {
		return p, err
	}
	return p, nil
}

// markAggCountFunc mark aggregation function which can be used as threshold.
// If no count aggregation function found, add a count aggregation function.
func (g *groupbyThresholdApplier) markAggCountFunc(lp LogicalPlan) error {
	for _, child := range lp.Children() {
		err := g.markAggCountFunc(child)
		if err != nil {
			return err
		}
	}

	agg, ok := lp.(*LogicalAggregation)
	if !ok {
		return nil
	}
	// By pass "reduce aggregation" and "group aggregation produced by distinct".
	if len(agg.GroupByItems) == 0 || agg.ProducedByDistinct {
		return nil
	}

	hasCount := false
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Name == ast.AggFuncCount && !aggFunc.HasDistinct {
			aggFunc.UseAsThreshold = true
			hasCount = true
			break
		}
	}
	if !hasCount {
		countFunc, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncCount, []expression.Expression{expression.One}, false)
		if err != nil {
			return err
		}
		countFunc.UseAsThreshold = true
		agg.AggFuncs = append(agg.AggFuncs, countFunc)
		agg.schema.Append(&expression.Column{
			UniqueID: agg.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  countFunc.RetTp,
		})
	}
	return nil
}

// addThreshold add selection between lp and flp(father of lp)
// index indicate the index of lp in flp's children
func (g *groupbyThresholdApplier) addThreshold(flp LogicalPlan, lp LogicalPlan, index int) error {
	for i, childLp := range lp.Children() {
		if err := g.addThreshold(lp, childLp, i); err != nil {
			return err
		}
	}

	if flp == nil {
		return nil
	}

	if agg, ok := lp.(*LogicalAggregation); ok {
		for i, f := range agg.AggFuncs {
			if f.UseAsThreshold {
				countCol := agg.Schema().Columns[i]
				countCol.UseAsThreshold = true

				thresholdConstant := &expression.Constant{
					Value:   types.NewDatum(getThreshold(lp)),
					RetType: types.NewFieldType(mysql.TypeTiny),
				}
				args := []expression.Expression{countCol, thresholdConstant}
				condExpr, err := expression.NewFunction(sessionctx.NewContext(), ast.GE, types.NewFieldType(mysql.TypeTiny), args...)
				if err != nil {
					return err
				}

				AddSelection(flp, lp, []expression.Expression{condExpr}, index)

				_, ok := flp.Children()[index].(*LogicalSelection)
				if !ok {
					return fmt.Errorf("flp's child must be LogicalSelection after AddSelection")
				}
				lp.SCtx().GetSessionVars().AffectedByGroupThreshold = true
			}
		}
	}

	return nil
}
