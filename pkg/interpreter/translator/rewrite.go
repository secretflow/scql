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

	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/parser/model"
)

type DbTable struct {
	dbName    string
	tableName string
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
	return DbTable{strings.ToLower(ss[0]), strings.ToLower(ss[1])}, nil
}

// rewriteTableRefs rewrites the tableRefs with local db_dbname.table_name
func rewriteTableRefs(m map[DbTable]DbTable, tableRefs []string) ([]string, error) {
	newTableRefs := make([]string, 0, len(tableRefs))
	for _, dbTableName := range tableRefs {
		DbTable, err := newDbTable(dbTableName)
		if err != nil {
			return nil, err
		}
		newDbTable, ok := m[DbTable]
		if !ok {
			return nil, fmt.Errorf("table %s.%s not found", DbTable.dbName, DbTable.tableName)
		}
		newTableRefs = append(newTableRefs, fmt.Sprintf("%s.%s", newDbTable.dbName, newDbTable.tableName))
	}
	return newTableRefs, nil
}

// rewrite rewrites the query with local db_name.table_name
func rewrite(sql string, tableRefs []string, enginesInfo *EnginesInfo, skipDb bool) (string, []string, error) {
	needRewrite := false
	var newTableRefs []string
	for _, party := range enginesInfo.GetParties() {
		if len(enginesInfo.GetTablesByParty(party)) > 0 {
			needRewrite = true
		}
	}

	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", nil, err
	}

	if needRewrite {
		m := enginesInfo.GetDBTableMap()
		newTableRefs, err = rewriteTableRefs(m, tableRefs)
		if err != nil {
			return "", nil, err
		}

		r := newRewriter(m)
		stmt.Accept(r)
		if r.err != nil {
			return "", nil, err
		}
	} else {
		newTableRefs = tableRefs
	}

	if skipDb {
		r := newDbNameRemover()
		stmt.Accept(r)
	}

	b := new(bytes.Buffer)
	// TODO(xiaoyuan) check flag here
	f := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes
	if err := stmt.Restore(format.NewRestoreCtx(f, b)); err != nil {
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
		x.Name.Schema.O = ""
	case *ast.ColumnName:
		x.Schema.O = ""
	}
	return in, r.err == nil
}
