// Copyright 2023 Ant Group Co., Ltd.

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

// Modified by Ant Group in 2023

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/transaction"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor
	stmt ast.StmtNode
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	switch x := e.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		err = e.executeDropTableOrView(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(x)
	default:
		err = fmt.Errorf("ddl.Next: Unsupported statement %v", x)

	}
	return err
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		tx.Commit()
	}()

	// check database exists
	dbName := strings.ToLower(s.Name)
	if exist, err := storage.CheckDatabaseExist(transaction.AddExclusiveLock(tx), dbName); err != nil || exist {
		if err != nil {
			return fmt.Errorf("executeCreateDatabase: %v", err)
		}
		if s.IfNotExists {
			return nil
		}
		return fmt.Errorf("database %v already exists", dbName)
	}

	// check too long database dbName
	if len(dbName) > mysql.MaxDatabaseNameLength {
		return fmt.Errorf(`database dbName "%v" is too long`, dbName)
	}

	// check user has create database privilege
	userName := e.ctx.GetSessionVars().User.Username
	hostName := e.ctx.GetSessionVars().User.Hostname
	user, err := storage.FindUser(tx, userName, hostName)
	if err != nil {
		return fmt.Errorf("executeCreateDatabase failed: %v", err)
	}
	if !user.CreatePriv {
		return fmt.Errorf(`access denied for user %v to database %v`, userName, dbName)
	}

	// create database
	result := tx.Create(&storage.Database{
		Db: dbName,
	})
	if result.Error != nil {
		return fmt.Errorf("ddl.executeCreateDatabase: %v", result.Error)
	}
	return nil
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) (err error) {
	if s.ReferTable != nil {
		return fmt.Errorf("ddl.executeCreateTable: unsupported CREATE TABLE ... LIKE ... statement")
	}
	if s.Partition != nil {
		return fmt.Errorf("ddl.executeCreateTable: unsupported PARTITION options")
	}
	if len(s.Cols) != 0 {
		return fmt.Errorf("ddl.executeCreateTable: unsupport CREATE TABLE with columns")
	}
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	dbName := s.Table.Schema.L
	if dbName == "" {
		if e.ctx.GetSessionVars().CurrentDB == "" {
			return fmt.Errorf("no database selected")
		}
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	// check database exists
	if exist, err := storage.CheckDatabaseExist(transaction.AddExclusiveLock(tx), dbName); err != nil || !exist {
		if err != nil {
			return fmt.Errorf("ddl.executeCreateTable: %v", err)
		}
		return fmt.Errorf("ddl.executeCreateTable: database %v doesn't exists", dbName)
	}

	// check table exists
	tblName := s.Table.Name.String()
	if exist, err := storage.CheckTableExist(transaction.AddExclusiveLock(tx), dbName, tblName); err != nil || exist {
		if err != nil {
			return fmt.Errorf("ddl.executeCreateTable: %v", err)
		}
		if s.IfNotExists {
			return nil
		}
		return fmt.Errorf("ddl.executeCreateTable: table %v.%v already exists", dbName, tblName)
	}

	// fetch table schema from grm
	tid, err := extractTID(s.Options)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateTable: extract tid failed %v", err)
	}
	grmToken := e.ctx.GetSessionVars().GrmToken
	if pass, err := e.ctx.GetSessionVars().GrmClient.VerifyTableOwnership(tid, grmToken); err != nil || !pass {
		if err != nil {
			return fmt.Errorf("ddl.executeCreateTable: verify table owner ship failed %v", err)
		}
		return fmt.Errorf("ddl.executeCreateTable: user %v is not the owner of tid %v", e.ctx.GetSessionVars().User.Username, tid)
	}
	userName := e.ctx.GetSessionVars().User.Username
	hostName := e.ctx.GetSessionVars().User.Hostname
	issuerPartyCode, err := storage.QueryUserPartyCode(tx, userName, hostName)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateTable: failed to query issuer party code: %v", err)
	}
	tableSchema, err := e.ctx.GetSessionVars().GrmClient.GetTableMeta(tid, issuerPartyCode, grmToken)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateTable: get table info failed: %v", err)
	}

	if tableSchema.DbName == "" || tableSchema.TableName == "" {
		return fmt.Errorf("ddl.executeCreateTable: the reference db name or table name is empty")
	}

	if len(tableSchema.Columns) == 0 {
		return fmt.Errorf("ddl.executeCreateTable: the reference table %v.%v is empty", tableSchema.DbName, tableSchema.TableName)
	}

	for _, col := range tableSchema.Columns {
		colType := strings.ToLower(col.Type)
		if !constant.SupportTypes[colType] {
			return fmt.Errorf("ddl.executeCreateTable: unknown type in schema: %s", colType)
		}
	}

	tableSchemaString, err := json.Marshal(tableSchema)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateTable: unexpected marshal failure: %v", err)
	}

	// creat table
	result := tx.Create(&storage.Table{
		Db:       dbName,
		Table:    tblName,
		Schema:   string(tableSchemaString),
		Owner:    e.ctx.GetSessionVars().User.Username,
		Host:     e.ctx.GetSessionVars().User.Hostname,
		RefDb:    tableSchema.DbName,
		RefTable: tableSchema.TableName,
	})
	if result.Error != nil {
		return fmt.Errorf("ddl.executeCreateTable: %v", result.Error)
	}

	for _, c := range tableSchema.Columns {
		result = tx.Create(&storage.Column{
			Db:          dbName,
			TableName:   tblName,
			ColumnName:  strings.ToLower(c.Name),
			Type:        c.Type,
			Description: c.Description,
		})
		if result.Error != nil {
			return fmt.Errorf("ddl.executeCreateTable: %v", result.Error)
		}
	}

	return nil
}

func extractTID(opts []*ast.TableOption) (string, error) {
	for _, opt := range opts {
		if opt.Tp == ast.TableOptionTID {
			return opt.StrValue, nil
		}
	}
	return "", fmt.Errorf("unable to find TID")
}

func (e *DDLExec) executeCreateView(s *ast.CreateViewStmt) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		tx.Commit()
	}()

	// check database exists
	dbName := s.ViewName.Schema.L
	if dbName == "" {
		if e.ctx.GetSessionVars().CurrentDB == "" {
			return fmt.Errorf("no database selected")
		}
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	if exist, err := storage.CheckDatabaseExist(transaction.AddExclusiveLock(tx), dbName); err != nil || !exist {
		if err != nil {
			return fmt.Errorf("checkDatabase failed when createView: %v", err)
		}
		return fmt.Errorf("database %v doesn't exists", dbName)
	}

	is, err := storage.QueryDBInfoSchema(transaction.AddExclusiveLock(tx), dbName)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateView: %v", err)
	}

	// check view exists
	viewName := s.ViewName.Name.String()
	if exist, err := storage.CheckTableExist(transaction.AddExclusiveLock(tx), dbName, viewName); err != nil || exist {
		if err != nil {
			return fmt.Errorf("ddl.executeCreateView: %v", err)
		}
		if !s.OrReplace {
			return fmt.Errorf("view %v.%v already exists", dbName, viewName)
		}
		// replace old view
		err = storage.NewDDLHandler(tx).DropTable(model.NewCIStr(dbName), model.NewCIStr(viewName))
		if err != nil {
			return fmt.Errorf("executeCreateView: %v", err)
		}
	}

	// create view
	result := tx.Create(&storage.Table{
		Db:           dbName,
		Table:        viewName,
		Owner:        e.ctx.GetSessionVars().User.Username,
		Host:         e.ctx.GetSessionVars().User.Hostname,
		IsView:       true,
		SelectString: s.Select.Text(),
	})
	if result.Error != nil {
		return fmt.Errorf("ddl.executeCreateView: %v", result.Error)
	}

	lp, _, err := core.BuildLogicalPlan(context.Background(), e.ctx, s.Select, is)
	if err != nil {
		return fmt.Errorf("ddl.executeCreateView: %v", err)
	}

	for i, field := range lp.OutputNames() {
		if field.ColName.String() == "" {
			return fmt.Errorf("ddl.executeCreateView must set column name explicitly")
		}
		t, err := infoschema.FieldTypeString(*lp.Schema().Columns[i].RetType)
		if err != nil {
			return fmt.Errorf("ddl.executeCreateView: %v", err)
		}
		result = tx.Create(&storage.Column{
			Db:              dbName,
			TableName:       viewName,
			ColumnName:      field.ColName.L,
			Type:            t,
			OrdinalPosition: uint(i),
		})
		if result.Error != nil {
			return fmt.Errorf("ddl.executeCreateView: %v", result.Error)
		}
	}
	return nil
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		tx.Commit()
	}()

	// check database exists
	dbName := strings.ToLower(s.Name)
	if exist, err := storage.CheckDatabaseExist(transaction.AddExclusiveLock(tx), dbName); err != nil || !exist {
		if err != nil {
			return fmt.Errorf("executeDropDatabase: %v", err)
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("database %v not exists", dbName)
	}
	err = storage.NewDDLHandler(tx).DropSchema(model.NewCIStr(dbName))
	return err
}

func (e *DDLExec) executeDropTableOrView(s *ast.DropTableStmt) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		tx.Commit()
	}()

	if s.IsTemporary {
		return fmt.Errorf("drop table statement dose not support drop temporary table yet")
	}

	if len(s.Tables) != 1 {
		return fmt.Errorf("drop table statement only support drop one table at once")
	}

	tn := s.Tables[0]
	dbName := tn.Schema.L
	if dbName == "" {
		if e.ctx.GetSessionVars().CurrentDB == "" {
			return fmt.Errorf("no specified database")
		}
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	// check database exists
	if exist, err := storage.CheckDatabaseExist(transaction.AddExclusiveLock(tx), dbName); err != nil || !exist {
		if err != nil {
			return fmt.Errorf("executeDropTableOrView: %v", err)
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("database %v doesn't exists", dbName)
	}

	// check table exists
	tblName := tn.Name.String()
	if exist, err := storage.CheckTableExist(transaction.AddExclusiveLock(tx), dbName, tblName); err != nil || !exist {
		if err != nil {
			return fmt.Errorf("executeDropTableOrView: %v", err)
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("table %v.%v not exists", dbName, tblName)
	}
	userName := e.ctx.GetSessionVars().User.Username
	hostName := e.ctx.GetSessionVars().User.Hostname
	if err := storage.CheckTableOwner(transaction.AddExclusiveLock(tx), dbName, tblName, userName, hostName); err != nil {
		return err
	}
	err = storage.NewDDLHandler(tx).DropTable(model.NewCIStr(dbName), model.NewCIStr(tblName))
	return err
}
