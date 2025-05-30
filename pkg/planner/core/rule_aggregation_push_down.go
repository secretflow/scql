// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023

package core

import (
	"context"
	"fmt"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

type aggregationPushDownSolver struct {
	aggregationEliminateChecker
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// It's easy to see that max, min, first row is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (a *aggregationPushDownSolver) isDecomposable(fun *aggregation.AggFuncDesc) bool {
	switch fun.Name {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncVarPop:
		// TODO: Support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		return !fun.HasDistinct
	default:
		return false
	}
}

// getAggFuncChildIdx gets which children it belongs to, 0 stands for left, 1 stands for right, -1 stands for both.
func (a *aggregationPushDownSolver) getAggFuncChildIdx(aggFunc *aggregation.AggFuncDesc, schema *expression.Schema) int {
	fromLeft, fromRight := false, false
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, aggFunc.Args, nil)
	for _, col := range cols {
		if schema.Contains(col) {
			fromLeft = true
		} else {
			fromRight = true
		}
	}
	if fromLeft && fromRight {
		return -1
	} else if fromLeft {
		return 0
	}
	return 1
}

// collectAggFuncs collects all aggregate functions and splits them into two parts: "leftAggFuncs" and "rightAggFuncs" whose
// arguments are all from left child or right child separately. If some aggregate functions have the arguments that have
// columns both from left and right children, the whole aggregation is forbidden to push down.
func (a *aggregationPushDownSolver) collectAggFuncs(agg *LogicalAggregation, join *LogicalJoin) (valid bool, leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc) {
	valid = true
	leftChild := join.children[0]
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposable(aggFunc) {
			return false, nil, nil
		}
		index := a.getAggFuncChildIdx(aggFunc, leftChild.Schema())
		switch index {
		case 0:
			leftAggFuncs = append(leftAggFuncs, aggFunc)
		case 1:
			rightAggFuncs = append(rightAggFuncs, aggFunc)
		default:
			return false, nil, nil
		}
	}
	return
}

// collectGbyCols collects all columns from gby-items and join-conditions and splits them into two parts: "leftGbyCols" and
// "rightGbyCols". e.g. For query "SELECT SUM(B.id) FROM A, B WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3" , the optimized
// query should be "SELECT SUM(B.agg) FROM A, (SELECT SUM(id) as agg, c1, c2, c3 FROM B GROUP BY id, c1, c2, c3) as B
// WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3". As you see, all the columns appearing in join-conditions should be
// treated as group by columns in join subquery.
func (a *aggregationPushDownSolver) collectGbyCols(agg *LogicalAggregation, join *LogicalJoin) (leftGbyCols, rightGbyCols []*expression.Column) {
	leftChild := join.children[0]
	ctx := agg.ctx
	for _, gbyExpr := range agg.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		for _, col := range cols {
			if leftChild.Schema().Contains(col) {
				leftGbyCols = append(leftGbyCols, col)
			} else {
				rightGbyCols = append(rightGbyCols, col)
			}
		}
	}
	// extract equal conditions
	for _, eqFunc := range join.EqualConditions {
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, eqFunc.GetArgs()[0].(*expression.Column))
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, eqFunc.GetArgs()[1].(*expression.Column))
	}
	for _, leftCond := range join.LeftConditions {
		cols := expression.ExtractColumns(leftCond)
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, cols...)
	}
	for _, rightCond := range join.RightConditions {
		cols := expression.ExtractColumns(rightCond)
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, cols...)
	}
	for _, otherCond := range join.OtherConditions {
		cols := expression.ExtractColumns(otherCond)
		for _, col := range cols {
			if leftChild.Schema().Contains(col) {
				leftGbyCols = a.addGbyCol(ctx, leftGbyCols, col)
			} else {
				rightGbyCols = a.addGbyCol(ctx, rightGbyCols, col)
			}
		}
	}
	return
}

func (a *aggregationPushDownSolver) splitAggFuncsAndGbyCols(agg *LogicalAggregation, join *LogicalJoin) (valid bool,
	leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc,
	leftGbyCols, rightGbyCols []*expression.Column) {
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyCols, rightGbyCols = a.collectGbyCols(agg, join)
	return
}

// addGbyCol adds a column to gbyCols. If a group by column has existed, it will not be added repeatedly.
func (a *aggregationPushDownSolver) addGbyCol(ctx sessionctx.Context, gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
	for _, c := range cols {
		duplicate := false
		for _, gbyCol := range gbyCols {
			if c.Equal(ctx, gbyCol) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			gbyCols = append(gbyCols, c)
		}
	}
	return gbyCols
}

// checkValidJoin checks if this join should be pushed across.
func (a *aggregationPushDownSolver) checkValidJoin(join *LogicalJoin) bool {
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (a *aggregationPushDownSolver) decompose(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc, schema *expression.Schema) ([]*aggregation.AggFuncDesc, *expression.Schema) {
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []*aggregation.AggFuncDesc{aggFunc.Clone()}
	for _, aggFunc := range result {
		schema.Append(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  aggFunc.RetTp,
		})
	}
	aggFunc.Args = expression.Column2Exprs(schema.Columns[schema.Len()-len(result):])
	aggFunc.Mode = aggregation.FinalMode
	return result, schema
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first row, we won't
// process it temporarily. If not, We will add additional group by columns and first row functions. We make a new aggregation operator.
// If the pushed aggregation is grouped by unique key, it's no need to push it down.
func (a *aggregationPushDownSolver) tryToPushDownAgg(aggFuncs []*aggregation.AggFuncDesc, gbyCols []*expression.Column, join *LogicalJoin, childIdx int, aggHints aggHintInfo, blockOffset int) (_ LogicalPlan, err error) {
	child := join.children[childIdx]
	if aggregation.IsAllFirstRow(aggFuncs) {
		return child, nil
	}
	// If the join is multiway-join, we forbid pushing down.
	if _, ok := join.children[childIdx].(*LogicalJoin); ok {
		return child, nil
	}
	tmpSchema := expression.NewSchema(gbyCols...)
	for _, key := range child.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil {
			return child, nil
		}
	}
	agg, err := a.makeNewAgg(join.ctx, aggFuncs, gbyCols, aggHints, blockOffset)
	if err != nil {
		return nil, err
	}
	agg.SetChildren(child)
	// If agg has no group-by item, it will return a default value, which may cause some bugs.
	// So here we add a group-by item forcely.
	if len(agg.GroupByItems) == 0 {
		agg.GroupByItems = []expression.Expression{&expression.Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeLong)}}
	}
	if (childIdx == 0 && join.JoinType == RightOuterJoin) || (childIdx == 1 && join.JoinType == LeftOuterJoin) {
		var existsDefaultValues bool
		join.DefaultValues, existsDefaultValues = a.getDefaultValues(agg)
		_, existsDefaultValues = a.getDefaultValues(agg)
		if !existsDefaultValues {
			return child, nil
		}
	}
	return agg, nil
}

func (a *aggregationPushDownSolver) getDefaultValues(agg *LogicalAggregation) ([]types.Datum, bool) {
	defaultValues := make([]types.Datum, 0, agg.Schema().Len())
	for _, aggFunc := range agg.AggFuncs {
		value, existsDefaultValue := aggFunc.EvalNullValueInOuterJoin(agg.ctx, agg.children[0].Schema())
		if !existsDefaultValue {
			return nil, false
		}
		defaultValues = append(defaultValues, value)
	}
	return defaultValues, true
}

func (a *aggregationPushDownSolver) checkAnyCountAndSum(aggFuncs []*aggregation.AggFuncDesc) bool {
	for _, fun := range aggFuncs {
		if fun.Name == ast.AggFuncSum || fun.Name == ast.AggFuncCount {
			return true
		}
	}
	return false
}

func (a *aggregationPushDownSolver) makeNewAgg(ctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, gbyCols []*expression.Column, aggHints aggHintInfo, blockOffset int) (*LogicalAggregation, error) {
	newAgg := LogicalAggregation{
		GroupByItems: expression.Column2Exprs(gbyCols),
		groupByCols:  gbyCols,
		aggHints:     aggHints,
	}.Init(ctx, blockOffset)
	aggLen := len(aggFuncs) + len(gbyCols)
	newAggFuncDescs := make([]*aggregation.AggFuncDesc, 0, aggLen)
	newSchema := expression.NewSchema(make([]*expression.Column, 0, aggLen)...)
	for _, aggFunc := range aggFuncs {
		var newFuncs []*aggregation.AggFuncDesc
		newFuncs, newSchema = a.decompose(ctx, aggFunc, newSchema)
		newAggFuncDescs = append(newAggFuncDescs, newFuncs...)
	}
	for i, gbyCol := range gbyCols {
		firstRow, err := aggregation.NewAggFuncDesc(newAgg.ctx, ast.AggFuncFirstRow, []expression.Expression{gbyCol}, false)
		if err != nil {
			return nil, err
		}
		newCol, ok := gbyCol.Clone().(*expression.Column)
		if !ok {
			return nil, fmt.Errorf("makeNewAgg: clone column err")
		}
		newCol.RetType = firstRow.RetTp

		duplicateFirstRow := false
		for j, aggFunc := range newAggFuncDescs {
			if firstRow.Equal(ctx, aggFunc) {
				duplicateFirstRow = true
				gbyCols[i].UniqueID = newSchema.Columns[j].UniqueID
			}
		}
		if !duplicateFirstRow {
			newAggFuncDescs = append(newAggFuncDescs, firstRow)
			newSchema.Append(newCol)
		}
	}
	newAgg.AggFuncs = newAggFuncDescs
	newAgg.SetSchema(newSchema)
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// agg.buildProjectionIfNecessary()
	return newAgg, nil
}

// pushAggCrossUnion will try to push the agg down to the union. If the new aggregation's group-by columns doesn't contain unique key.
// We will return the new aggregation. Otherwise we will transform the aggregation to projection.
// TODO(yang.y): merge duplicated first_row
func (a *aggregationPushDownSolver) pushAggCrossUnion(agg *LogicalAggregation, unionSchema *expression.Schema, unionChild LogicalPlan) LogicalPlan {
	ctx := agg.ctx
	newAgg := LogicalAggregation{
		AggFuncs:           make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs)),
		GroupByItems:       make([]expression.Expression, 0, len(agg.GroupByItems)),
		aggHints:           agg.aggHints,
		ProducedByDistinct: agg.ProducedByDistinct,
	}.Init(ctx, agg.blockOffset)
	newAgg.SetSchema(agg.schema.Clone())
	for _, aggFunc := range agg.AggFuncs {
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.Args))
		for _, arg := range newAggFunc.Args {
			newArgs = append(newArgs, expression.ColumnSubstitute(arg, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns)))
		}
		newAggFunc.Args = newArgs
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	for _, gbyExpr := range agg.GroupByItems {
		newExpr := expression.ColumnSubstitute(gbyExpr, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
	}
	newAgg.collectGroupByColumns()
	tmpSchema := expression.NewSchema(newAgg.groupByCols...)
	// e.g. Union distinct will add a aggregation like `select join_agg_0, join_agg_1, join_agg_2 from t group by a, b, c` above UnionAll.
	// And the pushed agg will be something like `select a, b, c, a, b, c from t group by a, b, c`. So if we just return child as join does,
	// this will cause error during executor phase.
	for _, key := range unionChild.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil {
			proj := a.convertAggToProj(newAgg)
			proj.SetChildren(unionChild)
			return proj
		}
	}
	newAgg.SetChildren(unionChild)
	return newAgg
}

func (a *aggregationPushDownSolver) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	//if !p.SCtx().GetSessionVars().AllowAggPushDown {
	//	return p, nil
	//}
	if err := a.MarkAggCountFunc(p); err != nil {
		return nil, err
	}
	if err := CheckAggFuncArgInGroupByItems(p); err != nil {
		return nil, err
	}
	return a.aggPushDown(p)
}

func (a *aggregationPushDownSolver) tryAggPushDownForUnion(union *LogicalUnionAll, agg *LogicalAggregation) error {
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposable(aggFunc) {
			return nil
		}
	}
	var gbyCols []*expression.Column
	gbyCols = expression.ExtractColumnsFromExpressions(gbyCols, agg.GroupByItems, nil)
	pushedAgg, err := a.makeNewAgg(agg.ctx, agg.AggFuncs, gbyCols, agg.aggHints, agg.blockOffset)
	if err != nil {
		return err
	}
	pushedAgg.ProducedByDistinct = agg.ProducedByDistinct
	newChildren := make([]LogicalPlan, 0, len(union.children))
	for _, child := range union.children {
		newChild := a.pushAggCrossUnion(pushedAgg, union.Schema(), child)
		newChildren = append(newChildren, newChild)
	}
	union.SetSchema(expression.NewSchema(newChildren[0].Schema().Columns...))
	union.SetChildren(newChildren...)
	return nil
}

func CheckAggFuncArgInGroupByItems(p LogicalPlan) error {
	for _, child := range p.Children() {
		err := CheckAggFuncArgInGroupByItems(child)
		if err != nil {
			return err
		}
	}
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return nil
	}
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			IsArgInGroupKeys := false
			if _, ok := aggFunc.Args[0].(*expression.Constant); ok {
				IsArgInGroupKeys = true
			}
			if !IsArgInGroupKeys {
				for _, item := range agg.GroupByItems {
					if ok := aggFunc.Args[0].Equal(agg.ctx, item); ok {
						IsArgInGroupKeys = true
						break
					}
				}
			}
			if !IsArgInGroupKeys {
				return fmt.Errorf("failed to check aggregate function: select column (%+v) is not in group by items %+v", aggFunc.Args[0], agg.GroupByItems)
			}
		}
	}
	return nil
}

func (a *aggregationPushDownSolver) MarkAggCountFunc(p LogicalPlan) error {
	for _, child := range p.Children() {
		err := a.MarkAggCountFunc(child)
		if err != nil {
			return err
		}
	}
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return nil
	}
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

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggregationPushDownSolver) aggPushDown(p LogicalPlan) (_ LogicalPlan, err error) {
	if agg, ok := p.(*LogicalAggregation); ok {
		proj := a.tryToEliminateAggregation(agg)
		if proj != nil {
			p = proj
		} else {
			child := agg.children[0]
			if join, ok1 := child.(*LogicalJoin); ok1 && a.checkValidJoin(join) {
				// NOTE(yang.y): disable push down for join for now. Join node's child usually contains one data source,
				// so it is likely translated to RunSQL nodes. The translation from sub logical plan to SQL string hasn't
				// support agg node yet.
				//
				//if valid, leftAggFuncs, rightAggFuncs, leftGbyCols, rightGbyCols := a.splitAggFuncsAndGbyCols(agg, join); valid {
				//	var lChild, rChild LogicalPlan
				//	// If there exist count or sum functions in left join path, we can't push any
				//	// aggregate function into right join path.
				//	rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
				//	leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
				//	if rightInvalid {
				//		rChild = join.children[1]
				//	} else {
				//		rChild, err = a.tryToPushDownAgg(rightAggFuncs, rightGbyCols, join, 1, agg.aggHints, agg.blockOffset)
				//		if err != nil {
				//			return nil, err
				//		}
				//	}
				//	if leftInvalid {
				//		lChild = join.children[0]
				//	} else {
				//		lChild, err = a.tryToPushDownAgg(leftAggFuncs, leftGbyCols, join, 0, agg.aggHints, agg.blockOffset)
				//		if err != nil {
				//			return nil, err
				//		}
				//	}
				//	join.SetChildren(lChild, rChild)
				//	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
				//	buildKeyInfo(join)
				//	proj := a.tryToEliminateAggregation(agg)
				//	if proj != nil {
				//		p = proj
				//	}
				//}
			} else if proj, ok1 := child.(*LogicalProjection); ok1 {
				/// WARNING: this optimization is disable when projection exprs contains scalar function. Because it does not make the engine plan more efficient.
				/// On the contrary, it will bring more computation cost. e.g.: compute the same expression more than once.
				// TODO: This optimization is not always reasonable. We have not supported pushing projection to kv layer yet,
				// so we must do this optimization.
				if canProjectionBeEliminatedLoose(proj) {
					for i, gbyItem := range agg.GroupByItems {
						agg.GroupByItems[i] = expression.ColumnSubstitute(gbyItem, proj.schema, proj.Exprs)
					}
					agg.collectGroupByColumns()
					for _, aggFunc := range agg.AggFuncs {
						newArgs := make([]expression.Expression, 0, len(aggFunc.Args))
						for _, arg := range aggFunc.Args {
							newArgs = append(newArgs, expression.ColumnSubstitute(arg, proj.schema, proj.Exprs))
						}
						aggFunc.Args = newArgs
					}
					projChild := proj.children[0]
					agg.SetChildren(projChild)
				}
			} else if union, ok1 := child.(*LogicalUnionAll); ok1 {
				if err := a.tryAggPushDownForUnion(union, agg); err != nil {
					return nil, err
				}
			}
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.aggPushDown(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*aggregationPushDownSolver) name() string {
	return "aggregation_push_down"
}
