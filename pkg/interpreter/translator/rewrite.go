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
	"bytes"
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/grm"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
)

type DbTable struct {
	dbName    string
	tableName string
	dbType    grm.DatabaseType // db type of table stored
}

func (dt *DbTable) String() string {
	return fmt.Sprintf("%s.%s", dt.dbName, dt.tableName)
}

func NewDbTable(db, table string) DbTable {
	return DbTable{dbName: db, tableName: table}
}

func newDbTable(dbTableName string) (DbTable, error) {
	ss := strings.Split(dbTableName, ".")
	if len(ss) != 2 {
		return DbTable{}, fmt.Errorf("newDbTable: invalid dbTableName %v", dbTableName)
	}
	return DbTable{dbName: strings.ToLower(ss[0]), tableName: strings.ToLower(ss[1])}, nil
}

func (dt *DbTable) SetDBType(dbType grm.DatabaseType) {
	dt.dbType = dbType
}

// rewriteTableRefsAndGetDBType rewrites the tableRefs with local db_dbname.table_name and get db type of
func rewriteTableRefsAndGetDBType(m map[DbTable]DbTable, tableRefs []string) ([]string, grm.DatabaseType, error) {
	dbType := grm.DBUnknown
	newTableRefs := make([]string, 0, len(tableRefs))
	for _, dbTableName := range tableRefs {
		DbTable, err := newDbTable(dbTableName)
		if err != nil {
			return nil, grm.DBUnknown, err
		}
		newDbTable, ok := m[DbTable]
		if !ok {
			return nil, grm.DBUnknown, fmt.Errorf("table %s not found", DbTable.String())
		}
		newTableRefs = append(newTableRefs, newDbTable.String())
		if newDbTable.dbType == grm.DBUnknown {
			continue
		}
		if dbType != grm.DBUnknown && newDbTable.dbType != dbType {
			return nil, grm.DBUnknown, fmt.Errorf("table %s has wrong db type %+v", DbTable.String(), newDbTable.dbType)
		}
		dbType = newDbTable.dbType
	}
	if dbType == grm.DBUnknown {
		dbType = grm.DBMySQL
	}
	return newTableRefs, dbType, nil
}

// runSQLString create sql string from lp with dialect
func runSQLString(lp core.LogicalPlan, enginesInfo *EnginesInfo, skipDb bool) (sql string, newTableRefs []string, err error) {
	var dialect core.Dialect
	dialect = core.NewMySQLDialect()
	// use MySQL as default dialect to get ref tables
	ctx, err := core.BuildChildCtx(dialect, lp)
	if err != nil {
		return "", nil, err
	}
	stmt, err := ctx.GetSQLStmt()
	if err != nil {
		return "", nil, err
	}
	tableRefs := ctx.GetTableRefs()
	dbType := grm.DBMySQL
	needRewrite := false
	for _, party := range enginesInfo.GetParties() {
		if len(enginesInfo.GetTablesByParty(party)) > 0 {
			needRewrite = true
		}
	}

	if needRewrite {
		m := enginesInfo.GetDBTableMap()
		newTableRefs, dbType, err = rewriteTableRefsAndGetDBType(m, tableRefs)
		if err != nil {
			return
		}
		if dbType != grm.DBMySQL {
			ok := true
			dialect, ok = core.DBDialectMap[dbType]
			if !ok {
				return "", nil, fmt.Errorf("failed to find dialect for db type %v", dbType)
			}
			ctx, err := core.BuildChildCtx(dialect, lp)
			if err != nil {
				return "", nil, err
			}
			stmt, err = ctx.GetSQLStmt()
			if err != nil {
				return "", nil, err
			}
		}

		r := newRewriter(m)
		stmt.Accept(r)
		if r.err != nil {
			err = r.err
			return
		}
	} else {
		newTableRefs = tableRefs
	}

	if skipDb {
		r := newDbNameRemover()
		stmt.Accept(r)
	}

	b := new(bytes.Buffer)
	if err := stmt.Restore(format.NewRestoreCtxWithDialect(dialect.GetRestoreFlags(), b, dialect.GetFormatDialect())); err != nil {
		return "", nil, err
	}

	return b.String(), newTableRefs, nil
}

// replace ref table name by local table name
type rewriter struct {
	err error
	m   map[DbTable]DbTable
}

func newRewriter(m map[DbTable]DbTable) *rewriter {
	return &rewriter{m: m}
}

func (r *rewriter) Enter(in ast.Node) (ast.Node, bool) {
	return in, r.err != nil
}

func (r *rewriter) Leave(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.TableSource:
		if t, ok := x.Source.(*ast.TableName); ok {
			r.rewriteDbTableName(t)
		}
	case *ast.ColumnNameExpr:
		r.rewriteDbTableName4Column(in.(*ast.ColumnNameExpr).Name)
	case *ast.ColumnName:
		r.rewriteDbTableName4Column(in.(*ast.ColumnName))
	}

	return in, r.err == nil
}

func (r *rewriter) rewriteDbTableName(t *ast.TableName) {
	for from, to := range r.m {
		if from.dbName == t.Schema.String() && from.tableName == t.Name.String() {
			t.Schema = model.NewCIStr(to.dbName)
			t.Name = model.NewCIStr(to.tableName)
		}
	}
}

func (r *rewriter) rewriteDbTableName4Column(t *ast.ColumnName) {
	for from, to := range r.m {
		if from.dbName == t.Schema.String() && from.tableName == t.Table.String() {
			t.Schema = model.NewCIStr(to.dbName)
			t.Table = model.NewCIStr(to.tableName)
		}
	}
}

// remove prefix db name in qualified table name
type dbNameRemover struct {
	err error
}

func newDbNameRemover() *dbNameRemover {
	return &dbNameRemover{}
}

func (r *dbNameRemover) Enter(in ast.Node) (ast.Node, bool) {
	return in, r.err != nil
}

func (r *dbNameRemover) Leave(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.ColumnNameExpr:
		x.Name.Schema = model.NewCIStr("")
	case *ast.ColumnName:
		x.Schema = model.NewCIStr("")
	}
	return in, r.err == nil
}
