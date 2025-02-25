// Copyright 2016 PingCAP, Inc.
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
	"context"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(lp.Schema().Columns)
	return lp, err
}

func (*columnPruner) name() string {
	return "column_prune"
}

// ExprsHasSideEffects checks if any of the expressions has side effects.
func ExprsHasSideEffects(exprs []expression.Expression) bool {
	for _, expr := range exprs {
		if exprHasSetVarOrSleep(expr) {
			return true
		}
	}
	return false
}

// exprHasSetVarOrSleep checks if the expression has SetVar function or Sleep function.
func exprHasSetVarOrSleep(expr expression.Expression) bool {
	scalaFunc, isScalaFunc := expr.(*expression.ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar || scalaFunc.FuncName.L == ast.Sleep {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if exprHasSetVarOrSleep(arg) {
			return true
		}
	}
	return false
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	used := GetUsedList(parentUsedCols, p.schema)
	p.schema.Columns = sliceutil.Take(p.schema.Columns, used)
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	err := child.PruneColumns(parentUsedCols)
	if err != nil {
		return err
	}
	if len(p.schema.Columns) == 0 {
		p.schema.Columns = []*expression.Column{child.Schema().Columns[0]}
	}
	return nil
}

func (p *LogicalJoin) extractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	lChild := p.children[0]
	rChild := p.children[1]
	for _, col := range parentUsedCols {
		if lChild.Schema().Contains(col) {
			leftCols = append(leftCols, col)
		} else if rChild.Schema().Contains(col) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

func (p *LogicalJoin) mergeSchema(parentUsedCols []*expression.Column) {
	p.schema = buildLogicalJoinSchema(p.JoinType, p, parentUsedCols)
}

func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) error {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	err := p.children[0].PruneColumns(leftCols)
	if err != nil {
		return err
	}

	err = p.children[1].PruneColumns(rightCols)
	if err != nil {
		return err
	}

	p.mergeSchema(parentUsedCols)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	used := GetUsedList(parentUsedCols, p.schema)

	p.schema.Columns = sliceutil.Take(p.schema.Columns, used)
	p.Exprs = sliceutil.Take(p.Exprs, used)

	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column) error {
	var selfUsedCols []*expression.Column
	windowColumns := p.GetWindowResultColumns()

	for _, col := range parentUsedCols {
		used := false
		for _, windowColumn := range windowColumns {
			if windowColumn.Equal(nil, col) {
				used = true
				break
			}
		}
		if !used {
			selfUsedCols = append(selfUsedCols, col)
		}
	}

	selfUsedCols = p.extractUsedCols(selfUsedCols)
	err := p.children[0].PruneColumns(selfUsedCols)
	if err != nil {
		return err
	}

	used := GetUsedList(parentUsedCols, p.schema)
	p.schema.Columns = sliceutil.Take(p.schema.Columns, used)

	return nil
}

func (p *LogicalWindow) extractUsedCols(usedCols []*expression.Column) []*expression.Column {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			usedCols = append(usedCols, expression.ExtractColumns(arg)...)
		}
	}
	for _, by := range p.PartitionBy {
		usedCols = append(usedCols, by.Col)
	}
	for _, by := range p.OrderBy {
		usedCols = append(usedCols, by.Col)
	}
	return usedCols
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) error {
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)

	err := la.children[1].PruneColumns(rightCols)
	if err != nil {
		return err
	}

	la.CorCols = extractCorColumnsBySchema(la.children[1], la.children[0].Schema())
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	err = la.children[0].PruneColumns(leftCols)
	if err != nil {
		return err
	}

	la.mergeSchema(parentUsedCols)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column) error {
	used := GetUsedList(parentUsedCols, ds.schema)

	var (
		handleCol     *expression.Column
		handleColInfo *model.ColumnInfo
	)
	originSchemaColumns := ds.schema.Columns
	originColumns := ds.Columns
	ds.schema.Columns = sliceutil.Take(ds.schema.Columns, used)
	ds.Columns = sliceutil.Take(ds.Columns, used)

	// For SQL like `select 1 from t`, we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 {
		if len(originColumns) > 0 {
			// use the first line.
			handleCol = originSchemaColumns[0]
			handleColInfo = originColumns[0]
		} else {
			handleCol = ds.newExtraHandleSchemaCol()
			handleColInfo = model.NewExtraHandleColInfo()
		}
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column) error {
	child := la.children[0]
	used := GetUsedList(parentUsedCols, la.Schema())

	la.schema.Columns = sliceutil.Take(la.schema.Columns, used)
	la.AggFuncs = sliceutil.Take(la.AggFuncs, used)
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range la.AggFuncs {
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.Args, nil)
	}
	if len(la.AggFuncs) == 0 {
		// If all the aggregate functions are pruned, we should add an aggregate function to keep the correctness.
		one, err := aggregation.NewAggFuncDesc(la.ctx, ast.AggFuncFirstRow, []expression.Expression{expression.One}, false)
		if err != nil {
			return err
		}
		la.AggFuncs = []*aggregation.AggFuncDesc{one}
		col := &expression.Column{
			UniqueID: la.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  types.NewFieldType(mysql.TypeTiny),
		}
		la.schema.Columns = []*expression.Column{col}
	}

	if len(la.GroupByItems) > 0 {
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			cols := expression.ExtractColumns(la.GroupByItems[i])
			if len(cols) == 0 {
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				selfUsedCols = append(selfUsedCols, cols...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(la.GroupByItems) == 0 {
			la.GroupByItems = []expression.Expression{expression.One}
		}
	}
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) error {
	child := ls.children[0]
	for i := len(ls.ByItems) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(ls.ByItems[i].Expr)
		if len(cols) == 0 {
			if expression.IsMutableEffectsExpr(ls.ByItems[i].Expr) {
				continue
			}
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else if ls.ByItems[i].Expr.GetType().Tp == mysql.TypeNull {
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else {
			parentUsedCols = append(parentUsedCols, cols...)
		}
	}
	return child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) error {
	used := GetUsedList(parentUsedCols, p.schema)

	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
		if hasBeenUsed {
			break
		}
	}
	if !hasBeenUsed {
		parentUsedCols = make([]*expression.Column, len(p.schema.Columns))
		copy(parentUsedCols, p.schema.Columns)
	} else {
		// Issue 10341: p.schema.Columns might contain table name (AsName), but p.Children()0].Schema().Columns does not.
		p.schema.Columns = sliceutil.Take(p.schema.Columns, used)
	}
	for _, child := range p.Children() {
		err := child.PruneColumns(parentUsedCols)
		if err != nil {
			return err
		}
	}
	return nil
}
