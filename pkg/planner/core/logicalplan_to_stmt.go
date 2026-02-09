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

	"slices"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
)

func (p *DataSource) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c := NewRunSqlCtx()
	tableName := ast.TableName{Schema: p.DBName, Name: p.tableInfo.Name}
	from := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: &tableName, AsName: *p.TableAsName}}}
	tableAsName := *p.TableAsName
	err := c.updateExprNodeFromColumns(d, p.logicalSchemaProducer.Schema().Columns)
	if err != nil {
		return nil, err
	}
	c.updateTableRefs([]string{fmt.Sprintf("%s.%s", p.DBName, p.tableInfo.Name)})
	c.updateTableAsName(tableAsName)
	c.setFrom(from)
	return c, err
}

func (p *LogicalProjection) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	newCtx, err := c.addClause(ClauseProjection)
	if err != nil {
		return nil, err
	}
	_, err = newCtx.updateExprNodeFromExpressions(d, p.Exprs, p.logicalSchemaProducer.Schema().Columns)
	return newCtx, err
}

func (p *LogicalSelection) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	// empty selection, due to group by threshold is 1
	if len(p.Conditions) == 0 {
		return c, nil
	}
	if slices.Contains(c.clauses, ClauseAggregate) {
		c, err = c.addClause(ClauseHaving)
	} else {
		c, err = c.addClause(ClauseWhere)
	}
	if err != nil {
		return nil, err
	}
	condExprs, err := c.updateExprNodeFromExpressions(d, p.Conditions, nil)
	if err != nil {
		return nil, err
	}
	CNFExpr := composeCNFCondition(condExprs)
	if slices.Contains(c.clauses, ClauseAggregate) {
		c.setHaving(CNFExpr)
	} else {
		c.setWhere(CNFExpr)
	}
	return c, nil
}

func (p *LogicalJoin) SqlStmt(d Dialect) (*runSqlCtx, error) {
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin, LeftOuterSemiJoin:
		return SemiJoinToSqlStmt(p, d)
	case LeftOuterJoin, RightOuterJoin, InnerJoin:
		return JoinSqlStmt(p, d)
	}
	return nil, fmt.Errorf("unsupported join type %d", p.JoinType)
}

// SemiJoinToSqlStmt converts a LogicalJoin of type
// SemiJoin/AntiSemiJoin/AntiLeftOuterSemiJoin/LeftOuterSemiJoin to a SQL statement.
func SemiJoinToSqlStmt(p *LogicalJoin, d Dialect) (*runSqlCtx, error) {
	var result []*runSqlCtx
	for _, child := range p.Children() {
		c, err := BuildChildCtx(d, child)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	ctx := result[0]
	ctx.updateTableRefs(result[1].GetTableRefs())
	columns := p.logicalSchemaProducer.Schema().Columns
	err := ctx.updateExprNodeFromColumns(d, columns[:len(columns)-1])
	if err != nil {
		return nil, err
	}
	// If it's generated from an in operator, translate to in expression
	// In other cases, translate to ExistsSubqueryExpr
	inScalarFunc := []*expression.ScalarFunction{}
	otherExprs := []expression.Expression{}

	splitExpr := func(cond expression.Expression) {
		if sfunc, ok := cond.(*expression.ScalarFunction); ok {
			// If it's an eq function and both arguments are in operand columns, translate to in expression
			if sfunc.FuncName.L == ast.EQ {
				c1, ok1 := sfunc.GetArgs()[0].(*expression.Column)
				c2, ok2 := sfunc.GetArgs()[1].(*expression.Column)
				if ok1 && ok2 && (c1.InOperand || c2.InOperand) {
					inScalarFunc = append(inScalarFunc, sfunc)
					return
				}
			}
		}
		otherExprs = append(otherExprs, cond)
	}

	for _, condition := range p.EqualConditions {
		splitExpr(condition)
	}
	for _, condition := range p.OtherConditions {
		splitExpr(condition)
	}
	inExprNodes := []ast.ExprNode{}
	for _, sfunc := range inScalarFunc {
		args, err := ctx.updateExprNodeFromExpressions(d, []expression.Expression{sfunc.GetArgs()[0]}, nil)
		if err != nil {
			return nil, err
		}
		ss, err := result[1].GetSQLStmt()
		if err != nil {
			return nil, err
		}
		inExpr := &ast.PatternInExpr{Expr: args[0], Sel: &ast.SubqueryExpr{Query: ss}, Not: p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin}
		inExprNodes = append(inExprNodes, inExpr)
	}
	if len(otherExprs) > 0 {
		result[1].mergeFieldsName(result[0])
		otherExprNodes, err := result[1].updateExprNodeFromExpressions(d, otherExprs, nil)
		if err != nil {
			return nil, err
		}
		condExpr := composeCNFCondition(otherExprNodes)
		if slices.Contains(result[1].clauses, ClauseWhere) {
			result[1].setWhere(composeCNFCondition([]ast.ExprNode{result[1].selectStmt.Where, condExpr}))
		} else {
			result[1].setWhere(condExpr)
		}
		ss, err := result[1].GetSQLStmt()
		if err != nil {
			return nil, err
		}
		condExpr = &ast.ExistsSubqueryExpr{Sel: &ast.SubqueryExpr{Query: ss}, Not: p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin}
		inExprNodes = append(inExprNodes, condExpr)
	}
	condExpr := composeCNFCondition(inExprNodes)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin:
		if slices.Contains(ctx.clauses, ClauseWhere) {
			ctx.selectStmt.Where = composeCNFCondition([]ast.ExprNode{ctx.selectStmt.Where, condExpr})
		} else {
			ctx.setWhere(condExpr)
		}
	case AntiLeftOuterSemiJoin, LeftOuterSemiJoin:
		ctx.colIdToExprNode[columns[len(columns)-1].UniqueID] = condExpr
	}
	return ctx, nil
}

func JoinSqlStmt(p *LogicalJoin, d Dialect) (*runSqlCtx, error) {
	joinStmt := &ast.Join{}
	switch p.JoinType {
	case InnerJoin:
		joinStmt.Tp = ast.CrossJoin
	case LeftOuterJoin:
		joinStmt.Tp = ast.LeftJoin
	case RightOuterJoin:
		joinStmt.Tp = ast.RightJoin
	default:
		// for now only support join/left join/right join
		return nil, fmt.Errorf("unsupported join type %d", p.JoinType)
	}

	newCtx := NewRunSqlCtx()
	var results []ast.ResultSetNode
	for i, child := range p.Children() {
		c, err := BuildChildCtx(d, child)
		if err != nil {
			return nil, err
		}
		newCtx.updateTableRefs(c.tableRefs)
		if t, ok := stripAsTbl(c); ok {
			results = append(results, t)
			for id, expr := range c.colIdToExprNode {
				newCtx.colIdToExprNode[id] = expr
			}
			continue
		}
		ss, err := c.GetSQLStmt()
		if err != nil {
			return nil, err
		}
		subTable := ast.TableSource{Source: ss, AsName: createAsTable(&p.logicalSchemaProducer, i)}
		c.tableAsName = subTable.AsName
		newCtx.UpdateFieldsName(c)
		results = append(results, &subTable)
	}
	joinStmt.Left, joinStmt.Right = results[0], results[1]
	// update on conditions
	var conditions []expression.Expression
	for _, condition := range p.EqualConditions {
		conditions = append(conditions, condition)
	}
	conds, err := newCtx.updateExprNodeFromExpressions(d, conditions, nil)
	if err != nil {
		return nil, err
	}
	// size of conds is zero means it's cross join
	if len(conds) > 0 {
		joinStmt.On = &ast.OnCondition{Expr: composeCNFCondition(conds)}
	}
	newCtx.setFrom(&ast.TableRefsClause{TableRefs: joinStmt})
	return newCtx, nil
}

func (p *LogicalUnionAll) SqlStmt(d Dialect) (*runSqlCtx, error) {
	unionStmt := ast.UnionStmt{SelectList: &ast.UnionSelectList{Selects: make([]*ast.SelectStmt, len(p.Children()))}}
	newCtx := NewRunSqlCtx()
	for i, child := range p.Children() {
		c, err := BuildChildCtx(d, child)
		if err != nil {
			return nil, err
		}
		newCtx.updateTableRefs(c.tableRefs)
		ss, err := c.GetSQLStmt()
		if err != nil {
			return nil, err
		}
		ss.IsInBraces = true
		unionStmt.SelectList.Selects[i] = ss
		if i == 0 {
			newCtx.UpdateFieldsName(c)
		}
	}
	tableAsName := createAsTable(&p.logicalSchemaProducer, 0)
	newCtx.updateTableAsName(tableAsName)
	newCtx.setFrom(&ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: &unionStmt, AsName: tableAsName}}})
	return newCtx, nil
}

func (p *LogicalLimit) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	c, err = c.addClause(ClauseLimit)
	if err != nil {
		return nil, err
	}
	c.setLimit(p.Count, p.Offset)
	return c, nil
}

func (p *LogicalSort) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	c, err = c.addClause(ClauseOrderby)
	if err != nil {
		return nil, err
	}
	var expressions []expression.Expression
	for _, byItem := range p.ByItems {
		expressions = append(expressions, byItem.Expr)
	}
	items, err := c.updateExprNodeFromExpressions(d, expressions, nil)
	if err != nil {
		return nil, err
	}
	var byItems []*ast.ByItem
	for i, item := range items {
		byItems = append(byItems, &ast.ByItem{Expr: item, Desc: p.ByItems[i].Desc})
	}
	c.setOrderBy(byItems)
	return c, nil
}
func (p *LogicalWindow) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	c, err = c.addClause(ClauseWindow)
	if err != nil {
		return nil, err
	}
	var partitionByCols []expression.Expression
	for _, item := range p.PartitionBy {
		partitionByCols = append(partitionByCols, item.Col)
	}
	updatedPartitionItems, err := c.updateExprNodeFromExpressions(d, partitionByCols, nil)
	if err != nil {
		return nil, err
	}
	var partitionByItems []*ast.ByItem
	for _, item := range updatedPartitionItems {
		partitionByItems = append(partitionByItems, &ast.ByItem{Expr: item})
	}
	var orderByCols []expression.Expression
	var desc []bool
	for _, item := range p.OrderBy {
		orderByCols = append(orderByCols, item.Col)
		desc = append(desc, item.Desc)
	}
	updatedOrderByItems, err := c.updateExprNodeFromExpressions(d, orderByCols, nil)
	if err != nil {
		return nil, err
	}
	var orderByItems []*ast.ByItem
	for i, item := range updatedOrderByItems {
		orderByItems = append(orderByItems, &ast.ByItem{Expr: item, Desc: desc[i]})
	}
	if len(p.WindowFuncDescs) != 1 {
		return nil, fmt.Errorf("expect 1 window spec but get %v", len(p.WindowFuncDescs))
	}
	var spec ast.WindowSpec
	spec.OrderBy = &ast.OrderByClause{
		Items: orderByItems,
	}
	spec.PartitionBy = &ast.PartitionByClause{
		Items: partitionByItems,
	}
	c.convertWindowFunc(p, spec)
	return c, nil
}

func (p *LogicalAggregation) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	if err != nil {
		return nil, err
	}
	c, err = c.addClause(ClauseAggregate)
	if err != nil {
		return nil, err
	}
	items, err := c.updateExprNodeFromExpressions(d, p.GroupByItems, nil)
	if err != nil {
		return nil, err
	}
	var byItems []*ast.ByItem
	for _, item := range items {
		byItems = append(byItems, &ast.ByItem{Expr: item})
	}
	c.setGroupBY(byItems)
	err = c.convertAggregateFunc(d, p)
	return c, err
}

func (p *LogicalMaxOneRow) SqlStmt(d Dialect) (*runSqlCtx, error) {
	c, err := BuildChildCtx(d, p.Children()[0])
	return c, err
}
