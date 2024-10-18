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

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/opcode"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type ClauseType int

const (
	// table source includes join, union, table
	ClauseTableSource ClauseType = iota
	ClauseWhere
	ClauseAggregate
	ClauseHaving
	ClauseWindow
	ClauseProjection
	ClauseOrderby
	ClauseLimit
)

type runSqlCtx struct {
	exprConverter   *expression.ExprConverter
	colIdToExprNode map[int64]ast.ExprNode
	selectStmt      *ast.SelectStmt
	schemaProducer  *logicalSchemaProducer
	storedClause    map[ClauseType]LogicalPlan
	clauses         []ClauseType
	tableAsName     model.CIStr
	tableRefs       []string
}

func NewRunSqlCtx() *runSqlCtx {
	return &runSqlCtx{
		exprConverter:   &expression.ExprConverter{},
		colIdToExprNode: make(map[int64]ast.ExprNode),
		selectStmt:      &ast.SelectStmt{},
		storedClause:    make(map[ClauseType]LogicalPlan),
	}
}

func BuildChildCtx(d Dialect, in LogicalPlan) (*runSqlCtx, error) {
	expectChildNumber := 1
	switch in.(type) {
	case *LogicalJoin, *LogicalApply:
		expectChildNumber = 2
	case *DataSource:
		expectChildNumber = 0
	case *LogicalUnionAll:
		expectChildNumber = -1
	}
	if expectChildNumber != -1 && len(in.Children()) != expectChildNumber {
		return nil, fmt.Errorf("%T: invalid children number %d != %d", in, len(in.Children()), expectChildNumber)
	}
	ctx, err := in.SqlStmt(d)
	if err != nil {
		return nil, err
	}
	if producer := getSchemaProducer(in); producer != nil {
		ctx.schemaProducer = producer
	}
	return ctx, nil
}

func getSchemaProducer(in LogicalPlan) *logicalSchemaProducer {
	switch x := in.(type) {
	case *LogicalJoin:
		return &x.logicalSchemaProducer
	case *LogicalUnionAll:
		return &x.logicalSchemaProducer
	case *DataSource:
		return &x.logicalSchemaProducer
	case *LogicalAggregation:
		return &x.logicalSchemaProducer
	case *LogicalWindow:
		return &x.logicalSchemaProducer
	case *LogicalProjection:
		return &x.logicalSchemaProducer
	}
	return nil
}

func (ctx *runSqlCtx) updateTableAsNameIfNeed(p *logicalSchemaProducer) {
	if ctx.tableAsName.String() == "" {
		ctx.tableAsName = createAsTable(p, 0)
		ctx.updateTableAsName(ctx.tableAsName)
	}
}

func (c *runSqlCtx) GetSQLStmt() (*ast.SelectStmt, error) {
	if c.schemaProducer == nil {
		return nil, fmt.Errorf("failed to find schema producer")
	}
	p := c.schemaProducer
	namesLen := len(p.names)
	fields := &ast.FieldList{Fields: make([]*ast.SelectField, len(p.Schema().Columns))}
	for i, col := range p.Schema().Columns {
		if col.IsHidden {
			return nil, fmt.Errorf("col(%+v) is hidden", col)
		}
		f := &ast.SelectField{}
		if expr, ok := c.colIdToExprNode[col.UniqueID]; !ok {
			return nil, fmt.Errorf("failed to find expr for column unique id %d", col.UniqueID)
		} else {
			f.Expr = expr
		}
		fields.Fields[i] = f
		if i >= namesLen {
			addAsNameForNonColumnName(f, col.UniqueID)
			continue
		}
		name := p.names[i]
		// For logical union all, name.OrigColName may be nil, we should catch this case
		if colName, ok := f.Expr.(*ast.ColumnNameExpr); ok {
			// skip to avoid 'a' as 'a'
			if name.OrigColName.String() == "" && colName.Name.Name.String() == name.ColName.String() {
				continue
			}
		}
		if name.ColName.String() != "" &&
			name.OrigColName.String() != name.ColName.String() {
			f.AsName = name.ColName
			continue
		}
		addAsNameForNonColumnName(f, col.UniqueID)
	}
	c.selectStmt.Fields = fields
	return c.selectStmt, nil
}

func (c *runSqlCtx) GetTableRefs() []string {
	return c.tableRefs
}

func (c *runSqlCtx) setFrom(from *ast.TableRefsClause) {
	c.clauses = append(c.clauses, ClauseTableSource)
	c.selectStmt.From = from
}

func (c *runSqlCtx) setWhere(where ast.ExprNode) {
	c.selectStmt.Where = where
}

func (c *runSqlCtx) setGroupBY(byItems []*ast.ByItem) {
	if len(byItems) > 0 {
		c.selectStmt.GroupBy = &ast.GroupByClause{Items: byItems}
	}
}

func (c *runSqlCtx) setHaving(having ast.ExprNode) {
	c.selectStmt.Having = &ast.HavingClause{Expr: having}
}

func (c *runSqlCtx) setLimit(count uint64, offset uint64) {
	if offset == 0 {
		c.selectStmt.Limit = &ast.Limit{Count: ast.NewValueExpr(count)}
	} else {
		c.selectStmt.Limit = &ast.Limit{Count: ast.NewValueExpr(count), Offset: ast.NewValueExpr(offset)}
	}
}

func (c *runSqlCtx) setOrderBy(byItems []*ast.ByItem) {
	c.selectStmt.OrderBy = &ast.OrderByClause{Items: byItems}
}

func (c *runSqlCtx) updateExprNodeFromColumns(d Dialect, columns []*expression.Column) error {
	for _, col := range columns {
		if col.IsHidden {
			continue
		}
		expr, err := c.exprConverter.ConvertExpressionToExprNode(d.GetFormatDialect(), col, 0, c.colIdToExprNode)
		if err != nil {
			return err
		}
		c.colIdToExprNode[col.UniqueID] = expr
	}
	return nil
}

// cases for sub query
// 1. replicated clauses
// 2. when current clause type < any clause type in stored clauses
func (ctx *runSqlCtx) needAsSub(c ClauseType) bool {
	for _, t := range ctx.clauses {
		if c == ClauseTableSource && len(ctx.clauses) == 1 && ctx.clauses[0] == ClauseTableSource {
			return true
		}
		if c <= t {
			return true
		}
	}
	return false
}

func (ctx *runSqlCtx) addClause(c ClauseType) (newCtx *runSqlCtx, err error) {
	newCtx = ctx
	if ctx.needAsSub(c) {
		newCtx, err = ctx.WorkAsSub()
		if err != nil {
			return
		}
		newCtx.clauses = append(newCtx.clauses, ClauseTableSource)
		newCtx.updateTableRefs(ctx.tableRefs)
	}
	newCtx.clauses = append(newCtx.clauses, c)
	newCtx.clauses = sliceutil.SliceDeDup(newCtx.clauses)
	return
}

func (c *runSqlCtx) updateTableRefs(refs []string) {
	c.tableRefs = append(c.tableRefs, refs...)
	c.tableRefs = sliceutil.SliceDeDup(c.tableRefs)
}

func (c *runSqlCtx) UpdateFieldsName(oldCtx *runSqlCtx) {
	for i, col := range oldCtx.schemaProducer.Schema().Columns {
		field := oldCtx.selectStmt.Fields.Fields[i]
		if field.AsName.String() != "" {
			c.colIdToExprNode[col.UniqueID] = &ast.ColumnNameExpr{Name: &ast.ColumnName{Table: oldCtx.tableAsName, Name: field.AsName}}
			continue
		}
		if colName, ok := field.Expr.(*ast.ColumnNameExpr); ok {
			c.colIdToExprNode[col.UniqueID] = &ast.ColumnNameExpr{Name: &ast.ColumnName{Table: oldCtx.tableAsName, Name: colName.Name.Name}}
		}
	}
}

func (c *runSqlCtx) WorkAsSub() (*runSqlCtx, error) {
	ss, err := c.GetSQLStmt()
	if err != nil {
		return nil, err
	}
	// create table as name after creating sub query
	c.updateTableAsNameIfNeed(nil)
	newCtx := NewRunSqlCtx()
	newCtx.selectStmt.From = &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: ss, AsName: c.tableAsName}}}
	newCtx.addClause(ClauseTableSource)
	newCtx.UpdateFieldsName(c)
	return newCtx, nil
}

func (c *runSqlCtx) updateExprNodeFromExpressions(d Dialect, exprs []expression.Expression, columns []*expression.Column) ([]ast.ExprNode, error) {
	var exprStmts []ast.ExprNode
	for i, expr := range exprs {
		exprStmt, err := c.exprConverter.ConvertExpressionToExprNode(d.GetFormatDialect(), expr, 0, c.colIdToExprNode)
		if err != nil {
			return nil, err
		}
		if columns != nil {
			c.colIdToExprNode[columns[i].UniqueID] = exprStmt
		}
		exprStmts = append(exprStmts, exprStmt)
	}
	return exprStmts, nil
}

func (c *runSqlCtx) convertWindowFunc(window *LogicalWindow, spec ast.WindowSpec) {
	columns := window.Schema().Columns
	lastCol := columns[len(columns)-1]
	windowDesc := window.WindowFuncDescs[0]
	c.colIdToExprNode[lastCol.UniqueID] = &ast.WindowFuncExpr{
		F:    windowDesc.Name,
		Spec: spec,
	}
}

func (c *runSqlCtx) convertAggregateFunc(d Dialect, agg *LogicalAggregation) error {
	for i, col := range agg.Schema().Columns {
		f := agg.AggFuncs[i]
		argExpr, err := c.updateExprNodeFromExpressions(d, f.Args, nil)
		if err != nil {
			return err
		}
		if f.Name == ast.AggFuncFirstRow {
			if d.SupportAnyValue() {
				c.colIdToExprNode[col.UniqueID] = &ast.AggregateFuncExpr{Args: argExpr, F: "any_value", Distinct: f.HasDistinct}
			} else {
				c.colIdToExprNode[col.UniqueID] = argExpr[0]
			}
			continue
		}
		c.colIdToExprNode[col.UniqueID] = &ast.AggregateFuncExpr{Args: argExpr, F: f.Name, Distinct: f.HasDistinct}
	}
	return nil
}

func (c *runSqlCtx) updateTableAsName(tableAsName model.CIStr) {
	if tableAsName.String() == "" {
		return
	}
	for id, expr := range c.colIdToExprNode {
		if col, ok := expr.(*ast.ColumnNameExpr); ok {
			c.colIdToExprNode[id] = &ast.ColumnNameExpr{Name: &ast.ColumnName{Table: tableAsName, Name: col.Name.Name}}
		}
	}
}

func stripAsTbl(ctx *runSqlCtx) (*ast.TableSource, bool) {
	if len(ctx.clauses) != 1 || ctx.clauses[0] != ClauseTableSource {
		return nil, false
	}
	from := ctx.selectStmt.From
	if from == nil {
		return nil, false
	}
	if source, ok := from.TableRefs.Left.(*ast.TableSource); !ok {
		return nil, false
	} else {
		if _, ok := source.Source.(*ast.TableName); !ok {
			return nil, false
		} else {
			return source, true
		}
	}
}

// func StripFieldAsName(ss *ast.SelectStmt) {
// 	for _, field := range ss.Fields.Fields {
// 		field.AsName = model.NewCIStr("")
// 	}
// }

func composeCNFCondition(conds []ast.ExprNode) ast.ExprNode {
	var result ast.ExprNode
	for i, expr := range conds {
		if i == 0 {
			result = conds[0]
			continue
		}
		result = &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: &ast.ParenthesesExpr{Expr: result}, R: &ast.ParenthesesExpr{Expr: expr}}
	}
	return result
}

// get table as name
func getOriginAsName(p *logicalSchemaProducer, colIndex int) (model.CIStr, bool) {
	if p == nil {
		return model.CIStr{}, false
	}
	if p.names[colIndex].TblName.O != "" &&
		p.names[colIndex].TblName.O != p.names[colIndex].OrigTblName.O {
		return p.names[colIndex].TblName, true
	}

	return model.CIStr{}, false
}

// common case for subquery
// sometimes table need as name, if the table don't rename in logical plan, use this function creating table as name
// special case for join
// join plan has two children, columns from left children locate in left half of p.names, columns from right children locate in right half of p.names
// Note: length of p.names may not equal to length of p.schema.columns due to column pruning
func createAsTable(p *logicalSchemaProducer, childIndex int) model.CIStr {
	// if childIndex != 0, means right child of logical join
	left := childIndex == 0

	if p != nil {
		colIndex := 0
		if !left {
			colIndex = len(p.names) - 1
		}
		tableAsName, found := getOriginAsName(p, colIndex)
		if found {
			return tableAsName
		}
	}
	return model.CIStr{L: fmt.Sprintf("t_%d", childIndex), O: fmt.Sprintf("t_%d", childIndex)}
}

func addAsNameForNonColumnName(f *ast.SelectField, uniqueID int64) {
	if _, ok := f.Expr.(*ast.ColumnNameExpr); !ok {
		f.AsName = model.NewCIStr(fmt.Sprintf("expr_%d", uniqueID))
	}
}
