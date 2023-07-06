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
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/privilege"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/table"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/mathutil"
)

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    model.CIStr
	Table     *ast.TableName       // Used for showing columns.
	Column    *ast.ColumnName      // Used for `desc table column`.
	IndexName model.CIStr          // Used for show table regions.
	Flag      int                  // Some flag parsed from sql, such as FULL.
	Roles     []*auth.RoleIdentity // Used for show grants.
	User      *auth.UserIdentity   // Used by show grants, show create user.

	is infoschema.InfoSchema

	result *chunk.Chunk
	cursor int

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`
	GlobalScope bool // GlobalScope is used by show variables
	Extended    bool // Used for `show extended columns from ...`
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		e.result = newFirstChunk(e)
		if err := e.fetchAll(ctx); err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := 0; colIdx < e.Schema().Len(); colIdx++ {
			retType := e.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				continue
			}
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				if valLen := len(row.GetString(colIdx)); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if e.cursor >= e.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), e.result.NumRows()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) appendRow(row []interface{}) {
	for i, col := range row {
		if col == nil {
			e.result.AppendNull(i)
			continue
		}
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}

func (e *ShowExec) fetchAll(ctx context.Context) error {
	switch e.Tp {
	case ast.ShowGrants:
		return e.fetchShowGrants()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowColumns:
		return e.fetchShowColumns(ctx)
	default:
		return fmt.Errorf("showExec: doesn't support %v", e.Tp)
	}
}

func (e *ShowExec) fetchShowGrants() (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// 1. check if database exists
	dbName := e.DBName.String()
	if exist, err := storage.CheckDatabaseExist(tx, dbName); !exist || err != nil {
		if err != nil {
			return fmt.Errorf("showGrants.fetchShowGrants: %v", err)
		}
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	// 2. check authUserName privileges
	authUserName := e.ctx.GetSessionVars().User.Username
	authHostName := e.ctx.GetSessionVars().User.Hostname
	authUserPrivs, err := e.getUserPrivileges(tx, authUserName, authHostName)
	if err != nil {
		return err
	}
	authUserDbGrants, err := e.getDatabaseGrants(tx, authUserName, e.DBName.String())
	if err != nil {
		return err
	}
	if len(authUserPrivs) == 0 && len(authUserDbGrants) == 0 {
		return nil
	}

	// 3. check userName privileges
	userName := e.User.Username
	hostName := e.User.Hostname
	userPrivs, err := e.getUserPrivileges(tx, userName, hostName)
	if err != nil {
		return err
	}
	userDbGrants, err := e.getDatabaseGrants(tx, userName, e.DBName.String())
	if err != nil {
		return err
	}
	if len(userPrivs) == 0 && len(userDbGrants) == 0 {
		return nil
	}

	// 4. merge
	var grants []string
	// Only root role(auth user) can see root role user's privilege.
	// Currently we provide a naive implementation: user who has any user privileges is treated as root role.
	// In the future if normal user has grants for CREATE USER but no CREATE/DROP/GRANT privilege,
	// We should not let them see the privileges for root role who has CREATE/DROP/GRANT privilege.
	if len(userPrivs) > 0 && len(authUserPrivs) > 0 {
		grants = append(grants, fmt.Sprintf("GRANT %s ON *.* TO %s", strings.Join(userPrivs, ", "), userName))
	}
	grants = append(grants, userDbGrants...)
	for _, g := range grants {
		e.appendRow([]interface{}{
			g,
		})
	}
	return nil
}

func (e *ShowExec) getDatabaseGrants(db *gorm.DB, userName string, dbName string) ([]string, error) {
	var grants []string
	// database level grant
	{
		var dbPrivs []storage.DatabasePriv
		result := db.Where(&storage.DatabasePriv{User: userName, Db: dbName}).Find(&dbPrivs)
		if result.Error != nil {
			return nil, result.Error
		}

		for _, dbPriv := range dbPrivs {
			var privs []string
			if dbPriv.CreatePriv {
				privs = append(privs, "CREATE")
			}
			if dbPriv.DropPriv {
				privs = append(privs, "DROP")
			}
			if dbPriv.GrantPriv {
				privs = append(privs, "GRANT OPTION")
			}
			if dbPriv.DescribePriv {
				privs = append(privs, "DESCRIBE")
			}
			if dbPriv.ShowPriv {
				privs = append(privs, "SHOW")
			}
			if len(privs) > 0 {
				grants = append(grants,
					fmt.Sprintf("GRANT %s ON %s.* TO %s",
						strings.Join(privs, ", "), dbPriv.Db, userName))
			}
		}
	}

	// table level grant
	{
		var tablePrivs []storage.TablePriv
		result := db.Where(&storage.TablePriv{User: userName, Db: dbName}).Find(&tablePrivs)
		if result.Error != nil {
			return nil, result.Error
		}
		for _, tp := range tablePrivs {
			var privs []string
			if tp.CreatePriv {
				privs = append(privs, "CREATE")
			}
			if tp.GrantPriv {
				privs = append(privs, "GRANT")
			}
			if tp.DropPriv {
				privs = append(privs, "DROP")
			}
			if tp.VisibilityPriv != 0 {
				privs = append(privs, mysql.Priv2Str[tp.VisibilityPriv])
			}
			if len(privs) > 0 {
				grants = append(grants,
					fmt.Sprintf("GRANT %s ON %s.%s TO %s",
						strings.Join(privs, ", "), tp.Db, tp.TableName, userName))
			}
		}
	}

	// column level grant
	{
		var colPrivs []storage.ColumnPriv
		result := db.Where(&storage.ColumnPriv{User: userName, Db: dbName}).Find(&colPrivs)
		if result.Error != nil {
			return nil, result.Error
		}
		for _, cp := range colPrivs {
			if cp.VisibilityPriv != 0 {
				grants = append(grants,
					fmt.Sprintf("GRANT %s(%s) ON %s.%s TO %s",
						mysql.Priv2Str[cp.VisibilityPriv], cp.ColumnName, cp.Db, cp.TableName, userName))
			}
		}
	}
	return grants, nil
}

func (e *ShowExec) getUserPrivileges(db *gorm.DB, userName, hostName string) ([]string, error) {
	user, err := storage.FindUser(db, userName, hostName)
	if err != nil {
		return nil, fmt.Errorf("getUserPrivileges failed `%v", err)
	}
	var privs []string
	if user.CreatePriv {
		privs = append(privs, "CREATE")
	}
	if user.CreateUserPriv {
		privs = append(privs, "CREATE USER")
	}
	if user.DropPriv {
		privs = append(privs, "DROP")
	}
	if user.GrantPriv {
		privs = append(privs, "GRANT OPTION")
	}
	if user.DescribePriv {
		privs = append(privs, "DESCRIBE")
	}
	if user.ShowPriv {
		privs = append(privs, "SHOW")
	}
	return privs, nil
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs, err := getAllDatabaseNames(e.ctx)
	if err != nil {
		return fmt.Errorf("showExec.showDatabases: %v", err)
	}
	sort.Strings(dbs)
	checker := privilege.GetPrivilegeManager(e.ctx)
	for _, d := range dbs {
		// check database is visible
		ok, err := checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, d, mysql.ShowPriv)
		if err != nil {
			return fmt.Errorf("showExec.fetchShowDatabases: %v", err)
		}
		if !ok {
			continue
		}
		e.appendRow([]interface{}{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowTables() (err error) {
	// check database is visible
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		ok, err := checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O, mysql.ShowPriv)
		if err != nil {
			return fmt.Errorf("showExec.fetchShowTables: %v", err)
		}
		if !ok {
			return e.dbAccessDenied()
		}
	}
	// DBIsVisible use e.ctx.GetSessionVars().Storage to check databases, so we should start a instance after check database finished
	// TODO (taochen.lk) maybe we should rewrite privilege.go
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	dbName := e.DBName.String()
	if exist, err := storage.CheckDatabaseExist(tx, dbName); !exist || err != nil {
		if err != nil {
			return fmt.Errorf("showExec.fetchShowTables: %v", err)
		}
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	// sort for tables
	tableNames, tableTypes, err := getAllTableNames(tx, dbName)
	if err != nil {
		return fmt.Errorf("showExec.fetchShowTables: %v", err)
	}
	sort.Strings(tableNames)

	// NOTE: we don't check table's visibility, we allow a user to see all the tables
	// within the database as long as the user has one privilege on that database.
	for _, v := range tableNames {
		if e.Full {
			e.appendRow([]interface{}{v, tableTypes[v]})
		} else {
			e.appendRow([]interface{}{v})
		}
	}
	return nil
}

const (
	TableTypeBase = `BASE TABLE`
	TableTypeView = `VIEW`
)

func getAllTableNames(db *gorm.DB, dbName string) ([]string, map[string]string, error) {
	var tables []storage.Table
	tableTypes := make(map[string]string)
	result := db.Where(&storage.Table{Db: dbName}).Find(&tables)
	if result.Error != nil {
		return nil, nil, result.Error
	}

	var names []string
	for _, t := range tables {
		names = append(names, t.Table)
		if t.IsView {
			tableTypes[t.Table] = TableTypeView
		} else {
			tableTypes[t.Table] = TableTypeBase
		}
	}
	return names, tableTypes, nil
}

func getAllDatabaseNames(ctx sessionctx.Context) ([]string, error) {
	store := ctx.GetSessionVars().Storage
	var databases []storage.Database
	result := store.Find(&databases)
	if result.Error != nil {
		return nil, result.Error
	}
	var names []string
	for _, d := range databases {
		names = append(names, d.Db)
	}
	return names, nil
}

func (e *ShowExec) fetchShowColumns(ctx context.Context) error {
	tb, err := e.getTable()

	if err != nil {
		return errors.Trace(err)
	}

	// check database is visible
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		ok, err := checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O, mysql.DescribePriv)
		if err != nil {
			return fmt.Errorf("showExec.fetchShowColumns: %v", err)
		}
		if !ok {
			return e.dbAccessDenied()
		}
	}

	for _, col := range tb.Cols() {
		if e.Column != nil && e.Column.Name.String() != col.Name.String() {
			continue
		}

		tpStr, err := infoschema.FieldTypeString(col.FieldType)
		if err != nil {
			return fmt.Errorf("fail to convert mysql field type %v to scdb field type: %v", col.FieldType, err)
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		if e.Full {
			e.appendRow([]interface{}{
				col.Name.String(),
				tpStr,
				col.Comment,
			})
		} else {
			e.appendRow([]interface{}{
				col.Name.String(),
				tpStr,
			})
		}
	}
	return nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, err := e.is.TableByName(e.Table.Schema, e.Table.Name)
	if err != nil {
		return nil, err
	}
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}
