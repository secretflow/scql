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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	driver "github.com/secretflow/scql/pkg/types/parser_driver"

	glog "github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/table"
	"github.com/secretflow/scql/pkg/types"
)

type visitInfo struct {
	privilege mysql.PrivilegeType
	db        string
	table     string
	column    string
	err       error
}

type aggHintInfo struct {
	preferAggType  uint
	preferAggToCop bool
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota + 1
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
}

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo []visitInfo
	// optFlag indicates the flags of the optimizer rules.
	optFlag uint64

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// selectOffset is the offsets of current processing select stmts.
	selectOffset []int

	windowSpecs map[string]*ast.WindowSpec
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	return b.visitInfo
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

func (b *PlanBuilder) getSelectOffset() int {
	if len(b.selectOffset) > 0 {
		return b.selectOffset[len(b.selectOffset)-1]
	}
	return -1
}

func (b *PlanBuilder) pushSelectOffset(offset int) {
	b.selectOffset = append(b.selectOffset, offset)
}

func (b *PlanBuilder) popSelectOffset() {
	b.selectOffset = b.selectOffset[:len(b.selectOffset)-1]
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	return &PlanBuilder{
		ctx:       sctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	b.optFlag = flagPrunColumns
	switch x := node.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.UnionStmt:
		return b.buildUnion(ctx, x)
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.GrantStmt, *ast.RevokeStmt, *ast.AlterUserStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DDLNode:
		return b.buildDDL(ctx, x)
	case *ast.ShowStmt:
		return b.buildShow(ctx, x)
	case *ast.ExplainStmt:
		return b.buildExplain(ctx, x)
	case *ast.SetStmt:
		return b.buildSet(ctx, x)
	case *ast.InsertStmt:
		return b.buildInsert(ctx, x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.FlushStmt, *ast.GrantRoleStmt, *ast.RevokeRoleStmt,
		*ast.KillStmt, *ast.UseStmt, *ast.ShutdownStmt:
		// TODO(yang.y): the above nodes may need to be ported
		return nil, fmt.Errorf("buildSimple: un-ported node %v", raw)
	case *ast.CreateUserStmt, *ast.DropUserStmt:
		// NOTE(shunde.csd): the CREATE USER privilege enables use of
		//   ALTER USER, CREATE USER, DROP USER, RENAME USER,
		//   and REVOKE ALL PRIVILEGES.
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		if b.ctx.GetSessionVars().CurrentDB == "" && raw.Level.DBName == "" {
			if raw.Level.Level == ast.GrantLevelTable {
				return nil, ErrNoDB
			}
		}
		b.visitInfo = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
	case *ast.RevokeStmt:
		b.visitInfo = collectVisitInfoFromRevokeStmt(b.ctx, b.visitInfo, raw)
	}

	return p, nil
}

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		if vars.IsGlobal {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Set use to set timezone, not support other value
				// Convert column name expression to string value expression.
				// char, col := b.ctx.GetSessionVars().GetCharsetInfo()
				// vars.Value = ast.NewValueExpr(cn.Name.Name.O, char, col)
				return nil, ErrUnsupportedType.GenWithStack("Unsupported set type")
			}
			mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
			var err error
			assign.Expr, _, err = b.rewrite(ctx, vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			return nil, ErrUnsupportedType.GenWithStack("Unsupported set type")
			//assign.ExtendValue = &expression.Constant{
			//	Value:   vars.ExtendValue.(*driver.ValueExpr).Datum,
			//	RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			// }
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

func (b *PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
	for _, f := range sel.Fields.Fields {
		if ast.HasWindowFlag(f.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	orderBy      bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p LogicalPlan) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	if idx < 0 {
		return -1, nil
	}
	col := p.Schema().Columns[idx]
	name := p.OutputNames()[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(c.Name, newColName) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			glog.Infof("havingWindowAndOrderbyExprResolver error: %s", a.err)
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    model.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.orderBy && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && !a.orderBy {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.orderBy {
					index, a.err = a.resolveFromPlan(v, a.p)
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p)
			_ = err
			if index == -1 && a.curClause != fieldList {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	extractor.curClause = fieldList
	for _, field := range sel.Fields.Fields {
		if !ast.HasWindowFlag(field.Expr) {
			continue
		}
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
		field.Expr = n.(ast.ExprNode)
	}
	for _, spec := range sel.WindowSpecs {
		_, ok := spec.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
	}
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if !ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, extractor.err
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

func (b *PlanBuilder) checkWindowFuncArgs(ctx context.Context, p LogicalPlan, windowFuncExprs []*ast.WindowFuncExpr, windowAggMap map[*ast.AggregateFuncExpr]int) error {
	for _, windowFuncExpr := range windowFuncExprs {
		args, err := b.buildArgs4WindowFunc(ctx, p, windowFuncExpr.Args, windowAggMap)
		if err != nil {
			return err
		}
		desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFuncExpr.F, args)
		if err != nil {
			return err
		}
		if desc == nil {
			return ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFuncExpr.F))
		}
	}
	return nil
}

func (b *PlanBuilder) buildArgs4WindowFunc(ctx context.Context, p LogicalPlan, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) ([]expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	newArgList := make([]expression.Expression, 0, len(args))
	// use below index for created a new col definition
	// it's okay here because we only want to return the args used in window function
	newColIndex := 0
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg)
			continue
		}
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(),
		}
		newColIndex += 1
		newArgList = append(newArgList, col)
	}
	return newArgList, nil
}

func collectVisitInfoFromRevokeStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.RevokeStmt) []visitInfo {
	// To use REVOKE, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are revoking.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	if dbName == "" {
		dbName = sctx.GetSessionVars().CurrentDB
	}
	vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)

	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		// NOTE(yang.y): skip visibility privilege here
		// it will be checked in revoke executor
		if _, ok := storage.VisibilityPrivColName[item.Priv]; ok {
			continue
		}

		if item.Priv == mysql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = storage.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = storage.AllDBPrivs
			case ast.GrantLevelTable:
				allPrivs = storage.AllTablePrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", nil)
	}

	return vi
}

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (Plan, error) {
	var authErr error
	switch v := node.(type) {
	case *ast.AlterDatabaseStmt, *ast.AlterTableStmt, *ast.CreateIndexStmt,
		*ast.CreateSequenceStmt, *ast.DropIndexStmt,
		*ast.TruncateTableStmt, *ast.RenameTableStmt, *ast.RecoverTableStmt,
		*ast.FlashBackTableStmt, *ast.LockTablesStmt, *ast.UnlockTablesStmt,
		*ast.CleanupTableLockStmt, *ast.RepairTableStmt:
		// TODO(yang.y): port the above cases when needed
		return nil, fmt.Errorf("planbuilder.buildDDL: unsupported ast %v", node)
	case *ast.CreateViewStmt:
		plan, err := b.Build(ctx, v.Select)
		if err != nil {
			return nil, err
		}
		schema := plan.Schema()
		names := plan.OutputNames()
		if v.Cols == nil {
			v.Cols = make([]model.CIStr, len(schema.Columns))
			for i, name := range names {
				v.Cols[i] = name.ColName
			}
		}
		if len(v.Cols) != schema.Len() {
			return nil, fmt.Errorf("create view must include all columns in the select clause")
		}
		if _, ok := plan.(LogicalPlan); ok {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE VIEW", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, v.ViewName.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateViewPriv, v.ViewName.Schema.L,
				v.ViewName.Name.L, "", authErr)
		}
		if v.Definer.CurrentUser && b.ctx.GetSessionVars().User != nil {
			v.Definer = b.ctx.GetSessionVars().User
		}
		if b.ctx.GetSessionVars().User != nil && v.Definer.String() != b.ctx.GetSessionVars().User.String() {
			err = ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "",
				"", "", err)
		}
	case *ast.CreateTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		if v.ReferTable != nil {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Username,
					b.ctx.GetSessionVars().User.Hostname, v.ReferTable.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.ReferTable.Schema.L,
				v.ReferTable.Name.L, "", authErr)
		}
	case *ast.CreateDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name,
			"", "", authErr)
	case *ast.DropDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Name,
			"", "", authErr)
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Username,
					b.ctx.GetSessionVars().User.Hostname, tableVal.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, tableVal.Schema.L,
				tableVal.Name.L, "", authErr)
		}

	}
	p := &DDL{Statement: node}
	return p, nil
}

func (b *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (Plan, error) {
	p := LogicalShow{
		ShowContents: ShowContents{
			Tp:          show.Tp,
			DBName:      show.DBName,
			Table:       show.Table,
			Column:      show.Column,
			IndexName:   show.IndexName,
			Flag:        show.Flag,
			User:        show.User,
			Roles:       show.Roles,
			Full:        show.Full,
			IfNotExists: show.IfNotExists,
			GlobalScope: show.GlobalScope,
			Extended:    show.Extended,
		},
	}.Init(b.ctx)
	isView := false
	switch show.Tp {
	case ast.ShowTables, ast.ShowTableStatus:
		if p.DBName == "" {
			return nil, ErrNoDB
		}
	case ast.ShowCreateTable, ast.ShowCreateView:
		return nil, fmt.Errorf("buildShow unsupported")
	}
	schema, names := buildShowSchema(show, isView)
	p.SetSchema(schema)
	p.names = names
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	var err error
	var np LogicalPlan
	np = p
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.OutputNames()[0].ColName},
		}
		np, err = b.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Where != nil {
		np, err = b.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if np != p {
		b.optFlag |= flagEliminateProjection
		fieldsLen := len(p.schema.Columns)
		proj := LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx, 0)
		schema := expression.NewSchema(make([]*expression.Column, 0, fieldsLen)...)
		for _, col := range p.schema.Columns {
			proj.Exprs = append(proj.Exprs, col)
			newCol := col.Clone().(*expression.Column)
			newCol.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
			schema.Append(newCol)
		}
		proj.SetSchema(schema)
		proj.SetChildren(np)
		proj.SetOutputNames(np.OutputNames())
		return proj, nil
	}
	return p, nil
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool) (schema *expression.Schema, outputNames []*types.FieldName) {
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowProcedureStatus, ast.ShowTriggers, ast.ShowEvents, ast.ShowWarnings, ast.ShowErrors,
		ast.ShowRegions, ast.ShowEngines, ast.ShowOpenTables, ast.ShowTableStatus,
		ast.ShowCharset, ast.ShowVariables, ast.ShowStatus, ast.ShowCollation, ast.ShowCreateTable,
		ast.ShowCreateUser, ast.ShowCreateView, ast.ShowCreateDatabase, ast.ShowDrainerStatus,
		ast.ShowIndex, ast.ShowPlugins, ast.ShowProcessList, ast.ShowPumpStatus, ast.ShowStatsMeta,
		ast.ShowStatsHistograms, ast.ShowStatsBuckets, ast.ShowStatsHealthy, ast.ShowProfiles,
		ast.ShowMasterStatus, ast.ShowPrivileges, ast.ShowBindings, ast.ShowAnalyzeStatus, ast.ShowBuiltins:
		return
	case ast.ShowDatabases:
		names = []string{"Database"}
	case ast.ShowTables:
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Table_type")
		}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowGrants:
		if s.User != nil {
			names = []string{fmt.Sprintf("Grants on %s for %s", s.DBName, s.User)}
		} else {
			// Don't know the name yet, so just say "user"
			names = []string{fmt.Sprintf("Grants on %s for User", s.DBName)}
		}
	}

	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: model.NewCIStr(names[i])})
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schema.Append(col)
	}
	return
}

func collectVisitInfoFromGrantStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	if dbName == "" {
		dbName = sctx.GetSessionVars().CurrentDB
	}
	vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)

	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		// NOTE(shunde.csd): skip visibility privilege here
		// it will be checked in grant executor
		if _, ok := storage.VisibilityPrivColName[item.Priv]; ok {
			continue
		}

		if item.Priv == mysql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = storage.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = storage.AllDBPrivs
			case ast.GrantLevelTable:
				allPrivs = storage.AllTablePrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", nil)
	}

	return vi
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	show, ok := explain.Stmt.(*ast.ShowStmt)
	if !ok {
		return nil, ErrUnsupportedType.GenWithStack("Unsupported explain stmt %T", explain.Stmt)
	}
	return b.buildShow(ctx, show)
}

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, fmt.Errorf("buildInsert: get table source failed")
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, fmt.Errorf("buildInsert: get table name failed")
	}
	fullTableName := tn.Name.String()
	if fullTableName == "" {
		return nil, fmt.Errorf("buildInsert: table name is empty")
	}
	if tn.Schema.String() != "" {
		fullTableName = tn.Schema.String() + "." + fullTableName
	}

	var plainColumnNames []string
	if len(insert.Columns) <= 0 {
		return nil, fmt.Errorf("buildInsert: get column failed")
	}
	for _, col := range insert.Columns {
		if col.Schema.String() != "" {
			return nil, fmt.Errorf("buildInsert: column name should not contain schema")
		}
		if col.Table.String() != "" && col.Table.String() != fullTableName {
			return nil, fmt.Errorf("buildInsert: table '%s' in column not equal to table '%s'", col.Table.String(), fullTableName)
		}
		plainColumnNames = append(plainColumnNames, col.Name.String())
	}

	if insert.Select == nil {
		return nil, fmt.Errorf("buildInsert: get select failed, only support 'INSERT INTO ... SELECT ... '")
	}

	ss, ok := insert.Select.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("buildInsert: get select stmt failed")
	}
	p, err := b.buildSelect(ctx, ss)
	if err != nil {
		return nil, fmt.Errorf("buildInsert: buildSelect failed: %v", err)
	}

	p.SetInsertTableOpt(&InsertTableOption{
		TableName: fullTableName,
		Columns:   plainColumnNames,
	})
	return p, nil
}

func getWindowName(name string) string {
	if name == "" {
		return "<unnamed window>"
	}
	return name
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	if ref.Frame != nil {
		return ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	if spec.PartitionBy != nil {
		return errors.Trace(ErrWindowNoChildPartitioning)
	}
	if ref.OrderBy != nil {
		if spec.OrderBy != nil {
			return ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		spec.OrderBy = ref.OrderBy
	}
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = model.NewCIStr("")
	return nil
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MySQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       which is the same as an empty frame.
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// For functions that operate on the entire partition, the frame clause will be ignored.
	if !needFrame && spec.Frame != nil {
		// specName := spec.Name.O
		// b.ctx.GetSessionVars().StmtCtx.AppendNote(ErrWindowFunctionIgnoresFrame.GenWithStackByArgs(windowFuncName, getWindowName(specName)))
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	return spec, false
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, error) {
	// updatedSpecMap is used to handle the specifications that have frame clause changed.
	updatedSpecMap := make(map[string]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	for _, windowFunc := range windowFuncs {
		if windowFunc.Spec.Name.L == "" {
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					return nil, err
				}
			}
			spec, _ = b.handleDefaultFrame(spec, windowFunc.F)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			continue
		}

		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		newSpec, updated := b.handleDefaultFrame(spec, windowFunc.F)
		if !updated {
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
		} else {
			if _, ok := updatedSpecMap[name]; !ok {
				updatedSpecMap[name] = newSpec
			}
			updatedSpec := updatedSpecMap[name]
			groupedWindow[updatedSpec] = append(groupedWindow[updatedSpec], windowFunc)
		}
	}
	return groupedWindow, nil
}
