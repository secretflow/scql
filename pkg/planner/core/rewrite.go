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
	"bytes"
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/parser/model"
)

type DbTable struct {
	dbName    string
	tableName string
	dbType    DBType // db type of table stored
}

func (dt *DbTable) String() string {
	if dt.dbName == "" {
		return dt.tableName
	}
	return fmt.Sprintf("%s.%s", dt.dbName, dt.tableName)
}

func NewDbTable(db, table string) DbTable {
	return DbTable{dbName: db, tableName: table}
}

func NewDbTableFromString(dbTableName string) (DbTable, error) {
	ss := strings.Split(dbTableName, ".")
	if len(ss) > 2 || len(ss) < 1 {
		return DbTable{}, fmt.Errorf("NewDbTableFromString: invalid dbTableName %v", dbTableName)
	}
	if len(ss) == 1 {
		return DbTable{tableName: ss[0]}, nil
	}
	return DbTable{dbName: ss[0], tableName: ss[1]}, nil
}

func (dt *DbTable) SetDBType(dbType DBType) {
	dt.dbType = dbType
}

func (dt *DbTable) GetTableName() string {
	return dt.tableName
}

func (dt *DbTable) GetDbName() string {
	return dt.dbName
}

// RewriteTableRefsAndGetDBType rewrites the tableRefs with local db_dbname.table_name and get db type of table
func RewriteTableRefsAndGetDBType(m map[DbTable]DbTable, tableRefs []string) ([]string, DBType, error) {
	dbType := DBTypeUnknown
	newTableRefs := make([]string, 0, len(tableRefs))
	for _, dbTableName := range tableRefs {
		DbTable, err := NewDbTableFromString(dbTableName)
		if err != nil {
			return nil, DBTypeUnknown, err
		}
		dbTable, ok := m[DbTable]
		if !ok {
			return nil, DBTypeUnknown, fmt.Errorf("table %s not found", DbTable.String())
		}
		newTableRefs = append(newTableRefs, dbTable.String())
		if dbTable.dbType == DBTypeUnknown {
			continue
		}
		if dbType != DBTypeUnknown && dbTable.dbType != dbType {
			return nil, DBTypeUnknown, fmt.Errorf("table %s has wrong db type %+v", DbTable.String(), dbTable.dbType)
		}
		dbType = dbTable.dbType
	}
	if dbType == DBTypeUnknown {
		dbType = DBTypeMySQL
	}
	return newTableRefs, dbType, nil
}

// replace ref table name by local table name
type DbTableRewriter struct {
	err error
	m   map[DbTable]DbTable
}

func NewDbTableRewriter(m map[DbTable]DbTable) *DbTableRewriter {
	return &DbTableRewriter{m: m}
}

func (r *DbTableRewriter) Enter(in ast.Node) (ast.Node, bool) {
	return in, r.err != nil
}

func (r *DbTableRewriter) Leave(in ast.Node) (ast.Node, bool) {
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

func (r *DbTableRewriter) rewriteDbTableName(t *ast.TableName) {
	for from, to := range r.m {
		if from.dbName == t.Schema.String() && from.tableName == t.Name.String() {
			t.Schema = model.NewCIStr(to.dbName)
			t.Name = model.NewCIStr(to.tableName)
		}
	}
}

func (r *DbTableRewriter) rewriteDbTableName4Column(t *ast.ColumnName) {
	for from, to := range r.m {
		if from.dbName == t.Schema.String() && from.tableName == t.Table.String() {
			t.Schema = model.NewCIStr(to.dbName)
			t.Table = model.NewCIStr(to.tableName)
		}
	}
}

func (r *DbTableRewriter) Error() error {
	return r.err
}

// replace ref table name by local table name
type DbTableVisitor struct {
	err    error
	tables []DbTable
}

func NewDbTableVisitor() *DbTableVisitor {
	return &DbTableVisitor{tables: make([]DbTable, 0)}
}

func (r *DbTableVisitor) Enter(in ast.Node) (ast.Node, bool) {
	return in, r.err != nil
}

func (r *DbTableVisitor) Leave(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.TableSource:
		if t, ok := x.Source.(*ast.TableName); ok {
			r.tables = append(r.tables, NewDbTable(t.Schema.String(), t.Name.String()))
		}
	}
	return in, r.err == nil
}

func (r *DbTableVisitor) Error() error {
	return r.err
}

func (r *DbTableVisitor) Tables() []DbTable {
	return r.Tables()
}

// RewriteSQLFromLP create sql string from lp with dialect
func RewriteSQLFromLP(lp LogicalPlan, m map[DbTable]DbTable, needRewrite bool) (sql string, newTableRefs []string, err error) {
	var dialect Dialect
	dialect = NewMySQLDialect()
	// use MySQL as default dialect to get ref tables
	ctx, err := BuildChildCtx(dialect, lp)
	if err != nil {
		return "", nil, err
	}
	stmt, err := ctx.GetSQLStmt()
	if err != nil {
		return "", nil, err
	}
	tableRefs := ctx.GetTableRefs()
	dbType := DBTypeMySQL

	if needRewrite {
		newTableRefs, dbType, err = RewriteTableRefsAndGetDBType(m, tableRefs)
		if err != nil {
			return
		}
		if dbType != DBTypeMySQL {
			ok := true
			dialect, ok = DBDialectMap[dbType]
			if !ok {
				return "", nil, fmt.Errorf("failed to find dialect for db type %v", dbType)
			}
			ctx, err := BuildChildCtx(dialect, lp)
			if err != nil {
				return "", nil, err
			}
			stmt, err = ctx.GetSQLStmt()
			if err != nil {
				return "", nil, err
			}
		}

		r := NewDbTableRewriter(m)
		stmt.Accept(r)
		if r.Error() != nil {
			err = r.Error()
			return
		}
	} else {
		newTableRefs = tableRefs
	}

	b := new(bytes.Buffer)
	if err := stmt.Restore(format.NewRestoreCtxWithDialect(dialect.GetRestoreFlags(), b, dialect.GetFormatDialect())); err != nil {
		return "", nil, err
	}

	return b.String(), newTableRefs, nil
}

func ReplaceTableNameInSQL(sql string, dialect format.Dialect, restoreFlags format.RestoreFlags, m map[DbTable]DbTable) (newSql string, err error) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return "", err
	}
	stmt := stmts[0]
	r := NewDbTableRewriter(m)
	stmt.Accept(r)
	if r.Error() != nil {
		err = r.Error()
		return
	}
	b := new(bytes.Buffer)
	if err := stmt.Restore(format.NewRestoreCtxWithDialect(restoreFlags, b, dialect)); err != nil {
		return "", err
	}
	return b.String(), nil
}

func GetSourceTables(sql string) ([]DbTable, error) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	stmt := stmts[0]
	r := NewDbTableVisitor()
	stmt.Accept(r)
	if r.Error() != nil {
		err = r.Error()
		return nil, err
	}
	return r.tables, nil
}
