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

package core

import (
	"context"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"

	"github.com/secretflow/scql/pkg/expression"
)

type ppdSolver struct{}

func (s *ppdSolver) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	_, p := lp.PredicatePushDown(nil)
	return p, nil
}

func (*ppdSolver) name() string {
	return "predicate_push_down"
}

func addSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, chIdx int) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.SelectBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
}

func AddSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, chIdx int) {
	addSelection(p, child, conditions, chIdx)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	var condsToPush []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByColumns := expression.NewSchema(la.groupByCols...)
	for _, cond := range predicates {
		switch cond.(type) {
		case *expression.Constant:
			condsToPush = append(condsToPush, cond)
			// Consider SQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
			// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
			// with value 0 rather than an empty query result.
			ret = append(ret, cond)
		case *expression.ScalarFunction:
			extractedCols := expression.ExtractColumns(cond)
			ok := true
			for _, col := range extractedCols {
				if !groupByColumns.Contains(col) {
					ok = false
					break
				}
			}
			if ok {
				newFunc := expression.ColumnSubstitute(cond, la.Schema(), exprsOriginal)
				condsToPush = append(condsToPush, newFunc)
			} else {
				ret = append(ret, cond)
			}
		default:
			ret = append(ret, cond)
		}
	}
	la.baseLogicalPlan.PredicatePushDown(condsToPush)
	return ret, la
}

// PredicatePushDown implements LogicalPlan interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	if len(p.children) == 0 {
		return predicates, p.self
	}
	child := p.children[0]
	rest, newChild := child.PredicatePushDown(predicates)
	addSelection(p.self, newChild, rest, 0)
	return nil, p.self
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	retConditions, child := p.children[0].PredicatePushDown(append(p.Conditions, predicates...))
	if len(retConditions) > 0 {
		p.Conditions = retConditions
		return nil, p
	}
	return nil, child
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	return predicates, ds
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(p, false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(p, true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.ctx, tempCond)
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case AntiSemiJoin:
		// `predicates` should only contain left conditions or constant filters.
		_, leftPushCond, rightPushCond, _ = p.extractOnCondition(predicates, true, true)
		// Do not derive `is not null` for anti join, since it may cause wrong results.
		// For example:
		// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
		// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
		// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
		leftCond = leftPushCond
		rightCond = append(p.RightConditions, rightPushCond...)
		p.RightConditions = nil
	}
	leftCond = expression.RemoveDupExprs(p.ctx, leftCond)
	rightCond = expression.RemoveDupExprs(p.ctx, rightCond)
	leftRet, lCh := p.children[0].PredicatePushDown(leftCond)
	rightRet, rCh := p.children[1].PredicatePushDown(rightCond)
	addSelection(p, lCh, leftRet, 0)
	addSelection(p, rCh, rightRet, 1)
	p.updateEQCond()
	for _, eqCond := range p.EqualConditions {
		p.LeftJoinKeys = append(p.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		p.RightJoinKeys = append(p.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	p.mergeSchema()
	buildKeyInfo(p)
	// except semi
	switch p.JoinType {
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		nilOtherCond := p.OtherConditions == nil || len(p.OtherConditions) == 0
		nilLeftCond := p.LeftConditions == nil || len(p.LeftConditions) == 0
		nilRightCond := p.RightConditions == nil || len(p.RightConditions) == 0
		conditonsNeedPushToSelection := expression.CNFExprs{}
		if !nilOtherCond {
			conditonsNeedPushToSelection = append(conditonsNeedPushToSelection, p.OtherConditions...)
			p.OtherConditions = nil
		}
		if !nilLeftCond {
			conditonsNeedPushToSelection = append(conditonsNeedPushToSelection, p.LeftConditions...)
			p.LeftConditions = nil
		}
		if !nilRightCond {
			conditonsNeedPushToSelection = append(conditonsNeedPushToSelection, p.RightConditions...)
			p.RightConditions = nil
		}

		if !nilLeftCond || !nilOtherCond || !nilRightCond {
			selection := LogicalSelection{Conditions: conditonsNeedPushToSelection}.Init(p.SCtx(), 0)
			selection.SetChildren(p)
			return ret, selection.self
		}
	}

	return ret, p.self
}

// updateEQCond will extract the arguments of a equal condition that connect two expressions.
func (p *LogicalJoin) updateEQCond() {
	lChild, rChild := p.children[0], p.children[1]
	var lKeys, rKeys []expression.Expression
	for i := len(p.OtherConditions) - 1; i >= 0; i-- {
		need2Remove := false
		if eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction); ok && eqCond.FuncName.L == ast.EQ {
			// If it is a column equal condition converted from `[not] in (subq)`, do not move it
			// to EqualConditions, and keep it in OtherConditions. Reference comments in `extractOnCondition`
			// for detailed reasons.
			if expression.IsEQCondFromIn(eqCond) {
				continue
			}
			lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
			if expression.ExprFromSchema(lExpr, lChild.Schema()) && expression.ExprFromSchema(rExpr, rChild.Schema()) {
				lKeys = append(lKeys, lExpr)
				rKeys = append(rKeys, rExpr)
				need2Remove = true
			} else if expression.ExprFromSchema(lExpr, rChild.Schema()) && expression.ExprFromSchema(rExpr, lChild.Schema()) {
				lKeys = append(lKeys, rExpr)
				rKeys = append(rKeys, lExpr)
				need2Remove = true
			}
		}
		if need2Remove {
			p.OtherConditions = append(p.OtherConditions[:i], p.OtherConditions[i+1:]...)
		}
	}
	if len(lKeys) > 0 {
		needLProj, needRProj := false, false
		for i := range lKeys {
			_, lOk := lKeys[i].(*expression.Column)
			_, rOk := rKeys[i].(*expression.Column)
			needLProj = needLProj || !lOk
			needRProj = needRProj || !rOk
		}

		var lProj, rProj *LogicalProjection
		if needLProj {
			lProj = p.getProj(0)
		}
		if needRProj {
			rProj = p.getProj(1)
		}
		for i := range lKeys {
			lKey, rKey := lKeys[i], rKeys[i]
			if lProj != nil {
				lKey = lProj.appendExpr(lKey)
			}
			if rProj != nil {
				rKey = rProj.appendExpr(rKey)
			}
			eqCond := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lKey, rKey)
			p.EqualConditions = append(p.EqualConditions, eqCond.(*expression.ScalarFunction))
		}
	}
}

func (p *LogicalProjection) appendExpr(expr expression.Expression) *expression.Column {
	if col, ok := expr.(*expression.Column); ok {
		return col
	}
	expr = expression.ColumnSubstitute(expr, p.schema, p.Exprs)
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
	}
	p.schema.Append(col)
	return col
}

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.children[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.Init(p.ctx, child.SelectBlockOffset())
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.children[idx] = proj
	return proj
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	for _, cond := range predicates {
		canBePushed = append(canBePushed, expression.ColumnSubstitute(cond, p.Schema(), p.Exprs))
	}
	return p.baseLogicalPlan.PredicatePushDown(canBePushed)
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(p *LogicalJoin, deriveLeft bool, deriveRight bool) (leftCond []expression.Expression,
	rightCond []expression.Expression) {
	// NOTE(yang.y): further simplification on DNF is skipped
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	for i, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild := proj.PredicatePushDown(newExprs)
		addSelection(p, newChild, retCond, i)
	}
	return nil, p
}

func (p *LogicalWindow) GetPartitionByCols() []*expression.Column {
	partitionCols := make([]*expression.Column, 0, len(p.PartitionBy))
	for _, partitionItem := range p.PartitionBy {
		partitionCols = append(partitionCols, partitionItem.Col)
	}
	return partitionCols
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	partitionCols := expression.NewSchema(p.GetPartitionByCols()...)
	for _, cond := range predicates {
		// We can push predicate beneath Window, only if all of the
		// extractedCols are part of partitionBy columns.
		if expression.ExprFromSchema(cond, partitionCols) {
			canBePushed = append(canBePushed, cond)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	p.baseLogicalPlan.PredicatePushDown(canBePushed)
	return canNotBePushed, p
}
