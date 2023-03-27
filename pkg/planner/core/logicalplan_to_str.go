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
	"reflect"
	"strings"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

const AggFuncPrefix = "agg_expr_"

type RunSqlContext struct {
	ExprConverter          *expression.ExprConverter
	ColIdToQualifiedName   map[int64]string // column name :table.columnName
	ColIdToUnqualifiedName map[int64]string // field name:columnName without table
	ColAsName              map[int64]string // store as name for column
	TableRefs              []string
	TableAsName            string
	SelectFiledList        []string // keep ordinal of field
}

func NewRunSqlContext() *RunSqlContext {
	return &RunSqlContext{
		ExprConverter:          &expression.ExprConverter{},
		ColIdToQualifiedName:   make(map[int64]string),
		ColAsName:              make(map[int64]string),
		ColIdToUnqualifiedName: make(map[int64]string),
	}
}

// set col as name when subquery was created
func (ctx *RunSqlContext) updateFieldName() {
	for key, value := range ctx.ColAsName {
		if _, exist := ctx.ColIdToUnqualifiedName[key]; !exist {
			ctx.ColIdToUnqualifiedName[key] = value
		}
	}
}

// don't use in logical window, its columns order is not same as names
func (ctx *RunSqlContext) updateFieldAsName(p *logicalSchemaProducer) {
	namesLen := len(p.names)
	for i, column := range p.schema.Columns {
		if column.IsHidden {
			continue
		}
		if i >= namesLen {
			return
		}
		name := p.names[i]
		if name.ColName.String() != "" &&
			name.OrigColName.O != name.ColName.O {
			ctx.ColAsName[column.UniqueID] = name.ColName.O
		}
	}
}

// if expressions are not in select field then sch is nil
func (ctx *RunSqlContext) convertExpressionsToStrings(expressions []expression.Expression, sch *expression.Schema, colToString map[int64]string, qualified bool) ([]string, error) {
	var cols []string
	for i, col := range expressions {
		var column *expression.Column
		if sch != nil {
			column = sch.Columns[i]
		}
		s, err := ctx.convertExpressionToString(col, column, colToString, qualified)
		if err != nil {
			return nil, err
		}
		cols = append(cols, s)
	}
	return cols, nil
}

func IsColumnNeedAsName(expr expression.Expression) bool {
	// if expression need as name return true, otherwise return false
	switch expr.(type) {
	case *expression.Column, *expression.Constant:
		return false
	default:
		return true
	}
}

func (ctx *RunSqlContext) convertExpressionToString(expr expression.Expression, column *expression.Column, colToString map[int64]string, qualified bool) (string, error) {
	s, err := ctx.ExprConverter.ExprToStrWithColString(expr, colToString, qualified)
	if err != nil {
		return "", err
	}
	if column != nil {
		ctx.ColIdToQualifiedName[column.UniqueID] = s

		if IsColumnNeedAsName(expr) && ctx.ColAsName[column.UniqueID] == "" {
			ctx.ColAsName[column.UniqueID] = fmt.Sprintf("expr_%d_%s", column.UniqueID, column.OrigName)
		}

	}
	return s, nil
}

func (ctx *RunSqlContext) convertColumnsToStr(columns []*expression.Column, colToString map[int64]string, qualified bool) ([]string, error) {
	var cols []string
	fieldToString := map[int64]string{}
	for _, col := range columns {
		if col.IsHidden {
			continue
		}
		s, err := ctx.ExprConverter.ExprToStrWithColString(col, colToString, qualified)
		if err != nil {
			return nil, err
		}
		cols = append(cols, s)
		fieldToString[col.UniqueID] = s
	}
	// rename duplicate name with suffix
	duplicateNameCount := map[string]int64{}
	for _, fname := range fieldToString {
		duplicateNameCount[fname]++
	}
	for id, fname := range fieldToString {
		if duplicateNameCount[fname] > 1 {
			ctx.ColAsName[id] = fieldToString[id] + fmt.Sprintf("_%d", id)
		}
	}
	ctx.ColIdToUnqualifiedName = fieldToString
	return cols, nil
}

// get table name
func GetOriginName(p *logicalSchemaProducer) string {
	tblName := ""
	for i := range p.names {
		if p.names[i].TblName.O != "" {
			tblName = p.names[i].TblName.O
		}
	}
	return tblName
}

// get table as name
func GetOriginAsName(p *logicalSchemaProducer) string {
	subQueryAsTblName := ""
	for i := range p.names {
		if p.names[i].TblName.O != "" &&
			p.names[i].TblName.O != p.names[i].OrigTblName.O {
			subQueryAsTblName = p.names[i].TblName.O
		}
	}
	return subQueryAsTblName
}

// sometimes table need as name, if the table don't rename in logical plan, use this function creating table as name
func createAsTable(plan LogicalPlan, childIndex int) string {
	tableNameLists := strings.Split(reflect.TypeOf(plan).String(), ".")
	if childIndex == 0 {
		return fmt.Sprintf("t_%s", tableNameLists[len(tableNameLists)-1])
	} else {
		return fmt.Sprintf("t_%s_%d", tableNameLists[len(tableNameLists)-1], childIndex)
	}
}

// if a plan has more than one child use this func merge children's ctx
func (c *RunSqlContext) mergeCtxs(ctxs []*RunSqlContext) {
	for _, ctx := range ctxs {
		for columnId, Column := range ctx.ColIdToQualifiedName {
			c.ColIdToQualifiedName[columnId] = Column
		}
		for columnId, asName := range ctx.ColAsName {
			c.ColAsName[columnId] = asName
		}
		for columnId, field := range ctx.ColIdToUnqualifiedName {
			c.ColIdToUnqualifiedName[columnId] = field
		}
		c.TableRefs = append(c.TableRefs, ctx.TableRefs...)
	}
}

func (ctx *RunSqlContext) replaceTableAsNameInColumn(tableAsName string) {
	if tableAsName == "" {
		return
	}
	for id := range ctx.ColIdToUnqualifiedName {
		ctx.ColIdToQualifiedName[id] = fmt.Sprintf("%s.%s", tableAsName, ctx.ColIdToUnqualifiedName[id])
	}
}

func (p *DataSource) RunSqlString(ctx *RunSqlContext) (string, error) {
	columns, err := ctx.convertColumnsToStr(p.Schema().Columns, ctx.ColIdToQualifiedName, false)
	if err != nil {
		return "", err
	}
	tableName := fmt.Sprintf("%s.%s", p.DBName.String(), p.TableInfo().Name.String())
	for i, column := range p.Schema().Columns {
		ctx.ColIdToUnqualifiedName[column.UniqueID] = columns[i]
		ctx.ColIdToQualifiedName[column.UniqueID] = fmt.Sprintf("%s.%s", tableName, columns[i])
	}
	fromString := fmt.Sprintf("%s.%s",
		p.DBName.String(), p.TableInfo().Name.String())
	ctx.SelectFiledList = columns
	ctx.TableRefs = []string{tableName}
	return fromString, nil
}

func (p *LogicalSelection) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalSection.RunSQLString: invalid children number %v != 1", len(p.Children()))
	}
	child := p.Children()[0]
	sql, err := buildChildRunSQL(ctx, p, 0)
	if err != nil {
		return "", err
	}

	for id := range ctx.ColIdToUnqualifiedName {
		ctx.ColIdToQualifiedName[id] = ctx.ColIdToUnqualifiedName[id]
	}

	conditions, err := ctx.convertExpressionsToStrings(p.Conditions, nil, ctx.getColNameAsName(), true)
	if err != nil {
		return "", err
	}

	var selectionClause string
	// if the type of LogicalSelection's child is LogicalAggregation, replace where by having
	if _, ok := child.(*LogicalAggregation); ok {
		selectionClause = fmt.Sprintf("having %s", strings.Join(conditions, " and "))
	} else {
		selectionClause = fmt.Sprintf("where %s", strings.Join(conditions, " and "))
	}

	s := fmt.Sprintf("%s %s", sql, selectionClause)
	return s, err
}

func (p *LogicalJoin) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 2 {
		return "", fmt.Errorf("logicalJoin.RunSQLString: invalid children number %d != 2", len(p.Children()))
	}
	if len(p.OtherConditions) != 0 || len(p.LeftConditions) != 0 || len(p.RightConditions) != 0 {
		return "", fmt.Errorf("only support equal conditions for join but get LeftConditions: %v, RightConditions: %v, OtherConditions: %v", p.LeftConditions, p.RightConditions, p.OtherConditions)
	}
	var joinStr string
	switch p.JoinType {
	case InnerJoin:
		joinStr = "join"
	case LeftOuterJoin:
		joinStr = "left join"
	case RightOuterJoin:
		joinStr = "right join"
	default:
		// for now only support join/left join/right join
		return "", fmt.Errorf("unsupported join type %d", p.JoinType)
	}
	ctx.updateFieldAsName(&p.logicalSchemaProducer)
	var subTable []string
	ctxs := []*RunSqlContext{}
	for i := range p.children {
		newCtx := NewRunSqlContext()
		newCtx.ColAsName = ctx.ColAsName
		if sql, err := buildChildRunSQL(newCtx, p, i); err != nil {
			return "", err
		} else {
			subTable = append(subTable, sql)
		}
		ctxs = append(ctxs, newCtx)
	}
	ctx.mergeCtxs(ctxs)
	columns, err := ctx.convertColumnsToStr(p.Schema().Columns, ctx.ColIdToQualifiedName, true)
	if err != nil {
		return "", err
	}

	// if duplicate name exists, then add as name for duplicate col
	var cols []string
	for i, column := range columns {
		if ctx.ColAsName[p.schema.Columns[i].UniqueID] != "" {
			ctx.ColIdToQualifiedName[p.schema.Columns[i].UniqueID] = column
			column = fmt.Sprintf("%s as %s", column, ctx.ColAsName[p.schema.Columns[i].UniqueID])
			cols = append(cols, column)
			continue
		}
		ctx.ColIdToQualifiedName[p.schema.Columns[i].UniqueID] = column
		cols = append(cols, column)
	}

	var conditions []expression.Expression
	for _, condition := range p.EqualConditions {
		conditions = append(conditions, condition)
	}
	conditionStrs, err := ctx.convertExpressionsToStrings(conditions, nil, ctx.getColNameAsName(), true)
	if err != nil {
		return "", err
	}
	joinTableStr := strings.Join(subTable, fmt.Sprintf(" %s ", joinStr))
	if len(conditions) != 0 {
		joinTableStr += " on " + strings.Join(conditionStrs, " and ")
	}
	ctx.SelectFiledList = cols
	return joinTableStr, nil
}

func (p *LogicalApply) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 2 {
		return "", fmt.Errorf("logicalApply.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	if len(p.OtherConditions)+len(p.EqualConditions) != 1 {
		return "", fmt.Errorf("logicalApply.RunSQLString: failed to check conditions: unsupported condition other:%s, equal:%s", p.OtherConditions, p.EqualConditions)
	}
	ctx.updateFieldAsName(&p.logicalSchemaProducer)
	var subTable []string
	ctxs := []*RunSqlContext{}
	for i := range p.children {
		newCtx := NewRunSqlContext()
		newCtx.ColAsName = ctx.ColAsName
		if sql, err := buildChildRunSQL(newCtx, p, i); err != nil {
			return "", err
		} else {
			subTable = append(subTable, sql)
		}
		ctxs = append(ctxs, newCtx)
		ctx.TableRefs = append(ctx.TableRefs, newCtx.TableRefs...)
	}
	var sFunc *expression.ScalarFunction
	if len(p.OtherConditions) > 0 {
		conditions := p.OtherConditions
		ok := false
		sFunc, ok = p.OtherConditions[0].(*expression.ScalarFunction)
		if !ok || sFunc.FuncName.L != ast.EQ {
			return "", fmt.Errorf("logicalApply.RunSQLString: failed to check conditions %s", conditions)
		}
	} else if len(p.EqualConditions) > 0 {
		conditions := p.EqualConditions
		sFunc = conditions[0]
		if sFunc.FuncName.L != ast.EQ {
			return "", fmt.Errorf("logicalApply.RunSQLString: failed to check conditions %s", conditions)
		}
	}
	arg0, err := ctx.convertExpressionToString(sFunc.GetArgs()[0], nil, ctx.ColIdToQualifiedName, true)
	if err != nil {
		return "", err
	}
	var sFunColumn string
	switch p.JoinType {
	case AntiLeftOuterSemiJoin, AntiSemiJoin:
		sFunColumn = fmt.Sprintf("%s not in (select %s from %s)", arg0, strings.Join(ctxs[1].SelectFiledList, ", "), subTable[1])
	case LeftOuterSemiJoin, SemiJoin:
		sFunColumn = fmt.Sprintf("%s in (select %s from %s)", arg0, strings.Join(ctxs[1].SelectFiledList, ", "), subTable[1])
	default:
		return "", fmt.Errorf("logicalApply.RunSQLString: failed to check join type %s", p.JoinType)
	}

	switch p.JoinType {
	case SemiJoin, AntiSemiJoin:
		return fmt.Sprintf("%s where (%s)", subTable[0], sFunColumn), nil
	case AntiLeftOuterSemiJoin, LeftOuterSemiJoin:
		columns, err := ctx.convertColumnsToStr(p.Schema().Columns[:len(p.Schema().Columns)-1], ctx.ColIdToQualifiedName, false)
		if err != nil {
			return "", err
		}
		columns = append(columns, sFunColumn)
		var cols []string
		for i, column := range columns {
			if ctx.ColAsName[p.schema.Columns[i].UniqueID] != "" {
				ctx.ColIdToQualifiedName[p.schema.Columns[i].UniqueID] = column
				column = fmt.Sprintf("%s as %s", column, ctx.ColAsName[p.schema.Columns[i].UniqueID])
				cols = append(cols, column)
				continue
			}
			ctx.ColIdToQualifiedName[p.schema.Columns[i].UniqueID] = column
			cols = append(cols, column)
		}
		ctx.SelectFiledList = cols
		return subTable[0], nil
	}
	return "", fmt.Errorf("logicalApply.RunSQLString: failed to check join type %s", p.JoinType)
}

func (p *LogicalUnionAll) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) < 2 {
		return "", fmt.Errorf("logicalUnionAll.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	ctx.updateFieldAsName(&p.logicalSchemaProducer)
	var subTable []string
	for i := range p.children {
		fromString, err := buildChildRunSQL(ctx, p, i)
		if err != nil {
			return "", err
		}
		subTable = append(subTable, fromString)
	}
	ctx.SelectFiledList = []string{}
	return strings.Join(subTable, " union all "), nil
}

func extractAggregation(agg *LogicalAggregation, ctx *RunSqlContext) ([]string /* gbyItems */, []string /* aggExprs */, error) {
	gbyItems, err := ctx.convertExpressionsToStrings(agg.GroupByItems, nil, ctx.getColNameAsName(), false)
	if err != nil {
		return nil, nil, err
	}

	var aggExprs []string
	for i, column := range agg.Schema().Columns {
		c := agg.AggFuncs[i]
		var aggFuncStr string
		switch c.Name {
		case ast.AggFuncFirstRow:
			if len(c.Args) > 1 {
				return nil, nil, fmt.Errorf("logicalAggregation: unsupported function %v", c.String())
			}

			aggExpr, err := ctx.convertExpressionToString(c.Args[0], column, ctx.ColIdToQualifiedName, true)
			if err != nil {
				return nil, nil, err
			}
			if ctx.ColAsName[column.UniqueID] == "" {
				ctx.ColAsName[column.UniqueID] = fmt.Sprintf("%s%d", AggFuncPrefix, column.UniqueID)
			}
			aggFuncStr = fmt.Sprintf("any_value(%s)", aggExpr)
		case ast.AggFuncGroupConcat:
			colname, err := ctx.convertExpressionToString(c.Args[0], column, ctx.ColIdToQualifiedName, true)
			if err != nil {
				return nil, nil, err
			}
			if c.HasDistinct {
				colname = fmt.Sprintf("distinct(%s)", colname)
			}
			separator, err := ctx.convertExpressionToString(c.Args[1], column, ctx.ColIdToQualifiedName, true)
			if err != nil {
				return nil, nil, err
			}
			if ctx.ColAsName[column.UniqueID] == "" {
				ctx.ColAsName[column.UniqueID] = fmt.Sprintf("%s%d_%s", AggFuncPrefix, column.UniqueID, column.OrigName)
			}
			aggFuncStr = fmt.Sprintf("%s(%s separator %s)", c.Name, colname, separator)
		default:
			funcName := c.Name
			if c.Name == ast.AggFuncCount && c.Mode == aggregation.FinalMode {
				funcName = ast.AggFuncSum
			}
			if c.HasDistinct {
				colname, err := ctx.convertExpressionToString(c.Args[0], column, ctx.ColIdToQualifiedName, true)
				if err != nil {
					return nil, nil, err
				}
				aggFuncStr = fmt.Sprintf("%s(distinct(%s))", funcName, colname)
				ctx.ColIdToQualifiedName[column.UniqueID] = aggFuncStr
			} else {
				var funcStrs []string
				for _, arg := range c.Args {
					str, err := ctx.convertExpressionToString(arg, column, ctx.ColIdToQualifiedName, true)
					if err != nil {
						return nil, nil, err
					}
					funcStrs = append(funcStrs, str)
				}
				aggFuncStr = fmt.Sprintf("%s(%s)", funcName, strings.Join(funcStrs, ", "))
				ctx.ColIdToQualifiedName[column.UniqueID] = aggFuncStr
			}
			if ctx.ColAsName[column.UniqueID] == "" {
				ctx.ColAsName[column.UniqueID] = fmt.Sprintf("%s%d_%s", AggFuncPrefix, column.UniqueID, column.OrigName)
			}
		}
		ctx.ColIdToQualifiedName[column.UniqueID] = aggFuncStr
		aggExprs = append(aggExprs, fmt.Sprintf("%s as %s", aggFuncStr, ctx.ColAsName[column.UniqueID]))
	}
	return gbyItems, aggExprs, nil
}

func (p *LogicalAggregation) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalAggregation.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	fromString, err := buildChildRunSQL(ctx, p, 0)
	if err != nil {
		return "", err
	}

	gbyItems, aggExprs, err := extractAggregation(p, ctx)
	if err != nil {
		return "", fmt.Errorf("logicalAggregation: %v", err)
	}

	if len(gbyItems) != 0 {
		byStr := fmt.Sprintf(" group by %s", strings.Join(gbyItems, ", "))
		fromString += byStr
	}

	ctx.SelectFiledList = aggExprs
	return fromString, nil
}

func (p *LogicalLimit) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalLimit.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	s := fmt.Sprintf(" limit %d", p.Count)
	if p.Offset != 0 {
		s += fmt.Sprintf(" offset %d", p.Offset)
	}

	sql, err := p.Children()[0].RunSqlString(ctx)
	if err != nil {
		return "", err
	}
	return sql + s, err
}

func (p *LogicalSort) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalSort.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}

	sql, err := p.Children()[0].RunSqlString(ctx)
	if err != nil {
		return "", err
	}
	var expressions []expression.Expression
	for _, byItem := range p.ByItems {
		expressions = append(expressions, byItem.Expr)
	}

	for id := range ctx.ColIdToUnqualifiedName {
		ctx.ColIdToQualifiedName[id] = ctx.ColIdToUnqualifiedName[id]
	}
	orderBys, err := ctx.convertExpressionsToStrings(expressions, nil, ctx.getColNameAsName(), false)
	if err != nil {
		return "", err
	}
	for i, order := range orderBys {
		if p.ByItems[i].Desc {
			orderBys[i] = order + " desc"
		}
	}

	s := fmt.Sprintf("%s order by %s", sql, strings.Join(orderBys, " , "))
	return s, nil
}

func (p *LogicalProjection) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalProjection.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	ctx.updateFieldAsName(&p.logicalSchemaProducer)
	fromString, err := buildChildRunSQL(ctx, p, 0)
	if err != nil {
		return "", err
	}
	columns, err := ctx.convertExpressionsToStrings(p.Exprs, p.Schema(), ctx.ColIdToQualifiedName, false)
	if err != nil {
		return "", err
	}
	var cols []string
	for i, column := range columns {
		if ctx.ColAsName[p.schema.Columns[i].UniqueID] != "" {
			// if column's name is same as asname, just use asname.
			subStrs := strings.Split(column, ".")
			if ctx.ColAsName[p.schema.Columns[i].UniqueID] == subStrs[len(subStrs)-1] {
				cols = append(cols, ctx.ColAsName[p.schema.Columns[i].UniqueID])
				continue
			}
			cols = append(cols, fmt.Sprintf("%s as %s", column, ctx.ColAsName[p.schema.Columns[i].UniqueID]))
			continue
		}
		cols = append(cols, column)
	}
	ctx.SelectFiledList = cols
	return fromString, err
}

func (p *LogicalWindow) RunSqlString(ctx *RunSqlContext) (string, error) {
	if len(p.Children()) != 1 {
		return "", fmt.Errorf("logicalWindow.RunSQLString: invalid children number %d != 1", len(p.Children()))
	}
	if len(p.WindowFuncDescs) != 1 {
		return "", fmt.Errorf("logicalWindow.RunSQLString: unsupported windowFuncDescs length 1(expected) != %d(actural)", len(p.WindowFuncDescs))
	}
	fromString, err := buildChildRunSQL(ctx, p, 0)
	if err != nil {
		return "", err
	}
	columns, err := ctx.convertColumnsToStr(p.schema.Columns[:p.schema.Len()-len(p.WindowFuncDescs)], ctx.ColIdToQualifiedName, false)
	if err != nil {
		return "", err
	}
	colStatement := fmt.Sprintf("%s()", p.WindowFuncDescs[0].Name)
	flagPartitionBy := false
	if p.PartitionBy != nil {
		flagPartitionBy = true
		colStatement += " over (partition by"
		var partitionColList = []string{}
		for _, item := range p.PartitionBy {
			s, err := ctx.ExprConverter.ExprToStrWithColString(item.Col, ctx.ColIdToQualifiedName, false)
			if err != nil {
				return "", err
			}
			partitionColList = append(partitionColList, s)
		}
		colStatement += fmt.Sprintf(" %s", strings.Join(partitionColList, ", "))
	}

	if p.OrderBy != nil {
		if !flagPartitionBy {
			colStatement += " over ("
		}
		colStatement += " order by"
		var orderColList = []string{}
		for _, item := range p.OrderBy {
			s, err := ctx.ExprConverter.ExprToStrWithColString(item.Col, ctx.ColIdToQualifiedName, true)
			if err != nil {
				return "", err
			}
			if item.Desc {
				s = s + " desc"
			}
			orderColList = append(orderColList, s)
		}
		colStatement += fmt.Sprintf(" %s", strings.Join(orderColList, ", "))
	}
	colStatement += ")"
	asName := fmt.Sprintf("expr_%d_%s", 0, p.WindowFuncDescs[0].Name)
	windowFuncColId := p.GetWindowResultColumns()[0].UniqueID
	ctx.ColIdToQualifiedName[windowFuncColId] = colStatement
	ctx.ColAsName[windowFuncColId] = asName
	columns[len(columns)-1] = colStatement + fmt.Sprintf(" as %s", asName)
	ctx.SelectFiledList = columns
	return fromString, err
}

func (p *baseLogicalPlan) RunSqlString(ctx *RunSqlContext) (string, error) {
	return "", fmt.Errorf("unsupported logical plan baseLogicalPlan")
}

func buildChildRunSQL(ctx *RunSqlContext, lp LogicalPlan, index int) (fromString string, err error) {
	// set ctx.TableAsName as nil so child plan will not affect by father plan
	ctx.TableAsName = ""
	// if table as name is not nil or
	// lp is logical aggregation and lp.child[index] is logical aggregation or
	// lp.child[index] is projection
	// make lp.child[index] as subquery
	tableAsName := GetChildTblAsName(lp, index)
	if tableAsName == "" {
		switch x := lp.Children()[index].(type) {
		case *LogicalProjection, *LogicalUnionAll:
			tableAsName = createAsTable(x, index)
		case *LogicalAggregation:
			if _, ok := lp.(*LogicalAggregation); ok {
				tableAsName = createAsTable(x, index)
			}
		}
		switch lp.(type) {
		case *LogicalJoin:
			if _, ok := lp.Children()[index].(*DataSource); !ok {
				tableAsName = createAsTable(lp.Children()[index], index)
			}
		}
	}

	fromString, err = lp.Children()[index].RunSqlString(ctx)
	if err != nil {
		return
	}
	switch lp.(type) {
	case *LogicalUnionAll:
		ctx.TableAsName = tableAsName
		ctx.updateFieldName()
		ctx.replaceTableAsNameInColumn(ctx.TableAsName)
		if len(ctx.SelectFiledList) != 0 {
			fromString = addFromToFromString(fromString)
			fromString = fmt.Sprintf("(select %s %s)", strings.Join(skipDuplicateCol(ctx.SelectFiledList), ", "), fromString)
		}
	default:
		// update field name
		if tableAsName != "" {
			ctx.TableAsName = tableAsName
			ctx.updateFieldName()
			ctx.replaceTableAsNameInColumn(ctx.TableAsName)
			if _, ok := lp.Children()[index].(*DataSource); ok {
				return fmt.Sprintf("%s as %s", fromString, ctx.TableAsName), err
			}
			if len(ctx.SelectFiledList) != 0 {
				fromString = addFromToFromString(fromString)
				fromString = fmt.Sprintf("(select %s %s) as %s", strings.Join(skipDuplicateCol(ctx.SelectFiledList), ", "), fromString, ctx.TableAsName)
			} else {
				fromString = fmt.Sprintf("from (%s) as %s", fromString, ctx.TableAsName)
			}
		}
	}

	return fromString, err
}

func ToSqlString(lp LogicalPlan) (string, []string, error) {
	ctx := NewRunSqlContext()
	var fromString string
	fromString, err := lp.RunSqlString(ctx)
	if err != nil {
		return "", nil, err
	}
	switch lp.(type) {
	case *LogicalUnionAll:
		return fromString, ctx.TableRefs, nil
	}
	fromString = addFromToFromString(fromString)
	if len(ctx.SelectFiledList) == 0 {
		return fmt.Sprintf("select * %s", fromString), ctx.TableRefs, nil
	}
	tableRefs := sliceutil.SliceDeDup(ctx.TableRefs)
	return fmt.Sprintf("select %s %s", strings.Join(ctx.SelectFiledList, ", "), fromString), tableRefs, nil
}

// logical plan don't keep table as name, except DataSource
// so we get table as name from field name
// if field's origin table name is not equal to table name
// return filed's table name
func GetChildTblAsName(plan LogicalPlan, index int) string {
	child := plan.Children()[index]
	switch x := child.(type) {
	case *DataSource:
		return x.TableAsName.String()
	}
	switch x := plan.(type) {
	case *LogicalAggregation:
		return GetOriginAsName(&x.logicalSchemaProducer)
	case *LogicalUnionAll:
		return GetOriginAsName(&x.logicalSchemaProducer)
	case *LogicalProjection:
		switch child.(type) {
		case *LogicalUnionAll:
			return GetOriginAsName(&x.logicalSchemaProducer)
		}
	}
	return ""
}

func addFromToFromString(fromString string) string {
	if strings.HasPrefix(fromString, "from") {
		return fromString
	}
	return fmt.Sprintf("from %s", fromString)
}

func (ctx *RunSqlContext) getColNameAsName() map[int64]string {
	nameWithAs := make(map[int64]string)
	for key, value := range ctx.ColIdToQualifiedName {
		nameWithAs[key] = value
	}
	for id, col := range ctx.ColAsName {
		// for query select tbl_1.plain_float_0 from alice.tbl_1 join alice.tbl_2 on tbl_1.plain_long_0 = tbl_2.plain_long_0 group by tbl_1.plain_float_0 having count(*) > 0
		// logical plan looks like Join{DataScan(tbl_1)->DataScan(tbl_2)}([eq(alice.tbl_1.plain_long_0, alice.tbl_2.plain_long_0)],)->Aggr(count(1),firstrow(alice.tbl_1.plain_float_0))->Sel([gt(Column#91, 0)])->Projection([alice.tbl_1.plain_float_0])
		// selection node looks like Sel([gt(Column#91, 0)]), column#91 means count(1) > 0

		// ctx.ColIdToQualifiedName keeps uniqueID 91 -> count(1)
		// but rename as expr_91_
		// we want uniqueID 91 -> count(1) but uniqueID 91 -> expr_91_ in map
		if !strings.HasPrefix(col, AggFuncPrefix) {
			nameWithAs[id] = col
		}
	}
	return nameWithAs
}

// remove duplicate cols because of dup cols in origin sql or having clauses
func skipDuplicateCol(cols []string) []string {
	var ret []string
	for _, col := range cols {
		isDuplicate := false
		for _, retCol := range ret {
			if retCol == col {
				isDuplicate = true
				break
			}
		}
		if !isDuplicate {
			ret = append(ret, col)
		}
	}
	return ret
}
