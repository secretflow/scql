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
	"github.com/secretflow/scql/pkg/types"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if
//
//	every expression is a single column
//	and
//	projection has the same number of columns as its children.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	if p.Schema().Len() != p.Children()[0].Schema().Len() {
		return false
	}
	return true
}

//// canProjectionBeEliminatedStrict checks whether a projection can be
//// eliminated, returns true if the projection just copy its child's output.
//func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
//	// If this projection is specially added for `DO`, we keep it.
//	if p.CalculateNoDelay {
//		return false
//	}
//	if p.Schema().Len() == 0 {
//		return true
//	}
//	child := p.Children()[0]
//	if p.Schema().Len() != child.Schema().Len() {
//		return false
//	}
//	for i, expr := range p.Exprs {
//		col, ok := expr.(*expression.Column)
//		if !ok || !col.Equal(nil, child.Schema().Columns[i]) {
//			return false
//		}
//	}
//	return true
//}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) {
	dst := replace[string(origin.HashCode(nil))]
	// different from original code
	// use for to loop until dst is nil rather than a if statement
	// TODO: refine this
	for dst != nil {
		retType, inOperand := origin.RetType, origin.InOperand
		*origin = *dst
		origin.RetType, origin.InOperand = retType, inOperand

		dst = replace[string(origin.HashCode(nil))]
		if dst != nil && dst.UniqueID == origin.UniqueID {
			// avoid infinite loop
			dst = nil
		}
	}
}

// ResolveExprAndReplace replaces columns fields of expressions by children logical plans.
func ResolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	switch expr := origin.(type) {
	case *expression.Column:
		resolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		resolveColumnAndReplace(&expr.Column, replace)
	case *expression.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			ResolveExprAndReplace(arg, replace)
		}
	}
}

//func doPhysicalProjectionElimination(p PhysicalPlan) PhysicalPlan {
//	for i, child := range p.Children() {
//		p.Children()[i] = doPhysicalProjectionElimination(child)
//	}
//
//	proj, isProj := p.(*PhysicalProjection)
//	if !isProj || !canProjectionBeEliminatedStrict(proj) {
//		return p
//	}
//	child := p.Children()[0]
//	return child
//}

//// eliminatePhysicalProjection should be called after physical optimization to
//// eliminate the redundant projection left after logical projection elimination.
//func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
//	oldSchema := p.Schema()
//	newRoot := doPhysicalProjectionElimination(p)
//	newCols := newRoot.Schema().Columns
//	for i, oldCol := range oldSchema.Columns {
//		oldCol.Index = newCols[i].Index
//		oldCol.ID = newCols[i].ID
//		oldCol.UniqueID = newCols[i].UniqueID
//		oldCol.VirtualExpr = newCols[i].VirtualExpr
//		newRoot.Schema().Columns[i] = oldCol
//	}
//	return newRoot
//}

type projectionEliminator struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminator) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminator) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool) LogicalPlan {
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
		childFlag = true
	}
	for i, child := range p.Children() {
		p.Children()[i] = pe.eliminate(child, replace, childFlag)
	}

	switch x := p.(type) {
	case *LogicalJoin:
		x.schema = buildLogicalJoinSchema(x.JoinType, x, x.Schema().Columns)
	case *LogicalApply:
		x.schema = buildLogicalJoinSchema(x.JoinType, x, x.Schema().Columns)
	default:
		for _, dst := range p.Schema().Columns {
			resolveColumnAndReplace(dst, replace)
		}
	}
	p.replaceExprColumns(replace)
	if isProj {
		if child, ok := p.Children()[0].(*LogicalProjection); ok && !ExprsHasSideEffects(child.Exprs) {
			for i := range proj.Exprs {
				proj.Exprs[i] = ReplaceColumnOfExpr(proj.Exprs[i], child, child.Schema())
			}
			p.Children()[0] = child.Children()[0]
		}
	}

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode(nil))] = exprs[i].(*expression.Column)
	}
	return p.Children()[0]
}

// ReplaceColumnOfExpr replaces column of expression by another LogicalProjection.
func ReplaceColumnOfExpr(expr expression.Expression, proj *LogicalProjection, schema *expression.Schema) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		idx := schema.ColumnIndex(v)
		if idx != -1 && idx < len(proj.Exprs) {
			return proj.Exprs[idx]
		}
	case *expression.ScalarFunction:
		for i := range v.GetArgs() {
			v.GetArgs()[i] = ReplaceColumnOfExpr(v.GetArgs()[i], proj, schema)
		}
	}
	return expr
}

func (p *LogicalJoin) replaceExprColumns(replace map[string]*expression.Column) {
	for _, equalExpr := range p.EqualConditions {
		ResolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		ResolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		ResolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		ResolveExprAndReplace(otherExpr, replace)
	}
}

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			ResolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ResolveExprAndReplace(gbyItem, replace)
	}
	la.collectGroupByColumns()
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	la.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range la.CorCols {
		dst := replace[string(coCol.Column.HashCode(nil))]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range ls.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (*projectionEliminator) name() string {
	return "projection_eliminate"
}

// buildQuantifierPlan adds extra condition for any / all subquery.
func (er *expressionRewriter) buildQuantifierPlan(plan4Agg *LogicalAggregation, cond, lexpr, rexpr expression.Expression, all bool) {
	// innerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), rexpr)
	//outerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), lexpr)

	// funcSum, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncSum, []expression.Expression{innerIsNull}, false)
	// if err != nil {
	// 	er.err = err
	// 	return
	// }
	// colSum := &expression.Column{
	// 	UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
	// 	RetType:  funcSum.RetTp,
	// }
	// plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcSum)
	// plan4Agg.schema.Append(colSum)
	// innerHasNull := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colSum, expression.Zero)

	// Build `count(1)` aggregation to check if subquery is empty.
	// funcCount, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{expression.One}, false)
	// if err != nil {
	// 	er.err = err
	// 	return
	// }
	// colCount := &expression.Column{
	// 	UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
	// 	RetType:  funcCount.RetTp,
	// }
	// plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcCount)
	// plan4Agg.schema.Append(colCount)

	// if all {
	// 	// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
	// 	// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) != 0, null, true).
	// 	innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.Null, expression.One)
	// 	cond = expression.ComposeCNFCondition(er.sctx, cond, innerNullChecker)
	// 	// If the subquery is empty, it should always return true.
	// 	emptyChecker := expression.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), colCount, expression.Zero)
	// 	// If outer key is null, and subquery is not empty, it should always return null, even when it is `null = all (1, 2)`.
	// 	outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.Null, expression.Zero)
	// 	cond = expression.ComposeDNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	// } else {
	// 	// For "any" expression, if the subquery has null and the cond returns false, the result should be NULL.
	// 	// Specifically, `t.id < any (select s.id from s)` would be rewrote to `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`
	// 	innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.Null, expression.Zero)
	// 	cond = expression.ComposeDNFCondition(er.sctx, cond, innerNullChecker)
	// 	// If the subquery is empty, it should always return false.
	// 	emptyChecker := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colCount, expression.Zero)
	// 	// If outer key is null, and subquery is not empty, it should return null.
	// 	outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.Null, expression.One)
	// 	cond = expression.ComposeCNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	// }

	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// plan4Agg.buildProjectionIfNecessary()
	// if !er.asScalar {
	// 	// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
	// 	er.p, er.err = er.b.buildSemiApply(er.p, plan4Agg, []expression.Expression{cond}, false, false)
	// 	return
	// }
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, plan4Agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(joinSchema.Clone().Columns[:outerSchemaLen]),
	}.Init(er.sctx, er.b.getSelectOffset())
	proj.names = make([]*types.FieldName, outerSchemaLen, outerSchemaLen+1)
	copy(proj.names, er.p.OutputNames())
	proj.SetSchema(expression.NewSchema(joinSchema.Clone().Columns[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schema.Append(&expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  cond.GetType(),
	})
	proj.names = append(proj.names, types.EmptyName)
	proj.SetChildren(er.p)
	er.p = proj

	//scql change
	er.asScalar = true
}
