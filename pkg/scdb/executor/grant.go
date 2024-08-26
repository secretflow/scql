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

	"github.com/pkg/errors"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/transaction"
)

// Grant Statement
// See https://dev.mysql.com/doc/refman/5.7/en/grant.html

var (
	_ Executor = (*GrantExec)(nil)
)

// GrantExec executes GrantStmt.
type GrantExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec
	TLSOptions []*ast.TLSOption

	is        infoschema.InfoSchema
	WithGrant bool
	done      bool
}

// Next implements the Executor Next interface.
func (e *GrantExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	if e.done {
		return nil
	}
	e.done = true

	if e.WithGrant {
		return fmt.Errorf("`WITH GRANT OPTION` is not supported, please use GRANT OPTION ON `db`.`table` TO `user`")
	}

	if len(e.Users) != 1 {
		return fmt.Errorf("grant statement only supports grant privileges to one user at one time")
	}

	user := e.Users[0]

	if err := CheckUserGrantPriv(tx, e.ctx.GetSessionVars().User, user.User, e.Privs); err != nil {
		return fmt.Errorf("grant privilege failed: %v", err)
	}

	// If there is no privilege entry in corresponding table, insert a new one.
	// Global scope:		scdb.user(user account, global privilege)
	// DB scope:			scdb.database_priv(database-level privilege)
	// Table scope:			scdb.tables_priv(table-level privilege)
	// Column scope:		scdb.columns_priv(column-level privilege)
	if len(e.Level.DBName) == 0 {
		e.Level.DBName = e.ctx.GetSessionVars().CurrentDB
	}

	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		if err = e.grantGlobalLevelPriv(tx, user); err != nil {
			return err
		}
	case ast.GrantLevelDB:
		if exist, err := storage.CheckDatabaseExist(tx, e.Level.DBName); !exist || err != nil {
			if err != nil {
				return err
			}
			return fmt.Errorf("database %v not exists", e.Level.DBName)
		}
		if err = e.grantDBLevelPriv(tx, user); err != nil {
			return err
		}
	case ast.GrantLevelTable:
		userName := e.ctx.GetSessionVars().User.Username
		hostName := e.ctx.GetSessionVars().User.Hostname
		if err := storage.CheckTableOwner(tx, e.Level.DBName, e.Level.TableName, userName, hostName); err != nil {
			return err
		}
		if err = e.grantTableLevelPriv(tx, user); err != nil {
			return err
		}
		if err = e.grantColumnLevelPriv(tx, user); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown grant level %v", e.Level.Level)
	}
	return nil
}

// grantGlobalLevel manipulates scdb.user table.
func (e *GrantExec) grantGlobalLevelPriv(tx *gorm.DB, user *ast.UserSpec) error {
	attrs, err := e.composePrivUpdate()
	if err != nil {
		return err
	}
	if len(attrs) == 0 {
		return nil
	}

	result := tx.Model(&storage.User{}).Where(&storage.User{
		Host: user.User.Hostname,
		User: user.User.Username,
	}).Updates(attrs)

	return result.Error
}

// grantDBLevel manipulates scdb.database_priv table.
func (e *GrantExec) grantDBLevelPriv(tx *gorm.DB, user *ast.UserSpec) (err error) {
	attrs, err := e.composePrivUpdate()
	if err != nil {
		return err
	}
	if len(attrs) == 0 {
		return nil
	}

	result := transaction.AddExclusiveLock(tx).Model(&storage.DatabasePriv{}).Where(&storage.DatabasePriv{
		Host: user.User.Hostname,
		User: user.User.Username,
		Db:   e.Level.DBName,
	}).Updates(attrs)
	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		attrs["host"] = user.User.Hostname
		attrs["user"] = user.User.Username
		attrs["db"] = e.Level.DBName
		result = tx.Model(&storage.DatabasePriv{}).Create(attrs)
	}
	return result.Error
}

func (e *GrantExec) grantTableLevelPriv(tx *gorm.DB, user *ast.UserSpec) error {
	attrs, err := e.composeTablePrivUpdate()
	if err != nil {
		return err
	}
	if len(attrs) == 0 {
		return nil
	}

	result := transaction.AddExclusiveLock(tx).Model(&storage.TablePriv{}).Where(&storage.TablePriv{
		Host:      user.User.Hostname,
		User:      user.User.Username,
		Db:        e.Level.DBName,
		TableName: e.Level.TableName,
	}).Updates(attrs)
	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		attrs["host"] = user.User.Hostname
		attrs["user"] = user.User.Username
		attrs["db"] = e.Level.DBName
		attrs["table_name"] = e.Level.TableName
		result = tx.Model(&storage.TablePriv{}).Create(attrs)
	}
	return result.Error
}

func (e *GrantExec) grantColumnLevelPriv(tx *gorm.DB, user *ast.UserSpec) error {
	for _, priv := range e.Privs {
		if len(priv.Cols) == 0 {
			continue
		}

		var colNames []string
		for _, c := range priv.Cols {
			colNames = append(colNames, c.Name.L)
		}

		if err := storage.CheckColumnsExist(tx, e.Level.DBName, e.Level.TableName, colNames); err != nil {
			return err
		}
		originNames, err := storage.GetColumnOriginNameByLowerName(tx, e.Level.DBName, e.Level.TableName, colNames)
		if err != nil {
			return err
		}
		if err := e.grantColumnPriv(tx, priv.Priv, user, originNames); err != nil {
			return err
		}
	}
	return nil
}

func (e *GrantExec) grantColumnPriv(tx *gorm.DB, priv mysql.PrivilegeType, user *ast.UserSpec, colNames []string) error {

	if err := e.initColumnPriv(tx, user, colNames); err != nil {
		return err
	}

	attrs, err := e.composeColumnPrivUpdate(priv)
	if err != nil {
		return err
	}
	result := transaction.AddExclusiveLock(tx).Model(&storage.ColumnPriv{}).Where(&storage.ColumnPriv{
		Host:      user.User.Hostname,
		User:      user.User.Username,
		Db:        e.Level.DBName,
		TableName: e.Level.TableName,
	}).Where("column_name IN ?", colNames).Updates(attrs)
	return result.Error
}

func (e *GrantExec) composePrivUpdate() (map[string]interface{}, error) {
	attributes := make(map[string]interface{})
	for _, priv := range e.Privs {
		if len(priv.Cols) != 0 {
			continue
		}
		if priv.Priv == mysql.AllPriv {
			err := SetAllGrantPrivTo(e.Level.Level, attributes, true)
			return attributes, err
		}
		v, ok := mysql.Priv2UserCol[priv.Priv]
		if !ok {
			return nil, errors.Errorf("Unknown table priv: %v", priv)
		}
		attributes[v] = true
	}
	return attributes, nil
}

func (e *GrantExec) composeTablePrivUpdate() (map[string]interface{}, error) {
	attributes := make(map[string]interface{})
	for _, priv := range e.Privs {
		if len(priv.Cols) != 0 {
			continue
		}
		if priv.Priv == mysql.AllPriv {
			if err := SetAllGrantPrivTo(e.Level.Level, attributes, true); err != nil {
				return nil, err
			}
		} else if v, exist := storage.VisibilityPrivColName[priv.Priv]; exist {
			attributes[v] = priv.Priv
		} else if v, exist := storage.TablePrivColName[priv.Priv]; exist {
			attributes[v] = true
		} else {
			return nil, errors.Errorf("Unknown table priv: %v", mysql.Priv2UserCol[priv.Priv])
		}
	}
	return attributes, nil
}
func (e *GrantExec) composeColumnPrivUpdate(priv mysql.PrivilegeType) (map[string]interface{}, error) {
	attributes := make(map[string]interface{})
	// only support visibility privilege
	v, ok := storage.VisibilityPrivColName[priv]
	if !ok {
		return nil, fmt.Errorf("unknown visibility priv: %v", mysql.Priv2Str[priv])
	}
	attributes[v] = priv
	return attributes, nil
}

// initColumnPrivEntry inserts a new row into scdb.columns_priv with empty privilege if not exist
func (e *GrantExec) initColumnPriv(tx *gorm.DB, user *ast.UserSpec, colNames []string) error {

	var columnPrivExist []string
	if err := transaction.AddExclusiveLock(tx).Model(&storage.ColumnPriv{}).Select("lower(column_name)").Where(&storage.ColumnPriv{
		Host:      user.User.Hostname,
		Db:        e.Level.DBName,
		User:      user.User.Username,
		TableName: e.Level.TableName,
	}).Find(&columnPrivExist, "column_name IN ?", colNames).Error; err != nil {
		return err
	}

	columnPrivExistMap := make(map[string]bool, len(columnPrivExist))
	for _, col := range columnPrivExist {
		columnPrivExistMap[col] = true
	}

	var columnPrivNotExist []storage.ColumnPriv
	for _, colName := range colNames {
		if _, exist := columnPrivExistMap[colName]; !exist {
			columnPriv := storage.ColumnPriv{
				Host:       user.User.Hostname,
				Db:         e.Level.DBName,
				User:       user.User.Username,
				TableName:  e.Level.TableName,
				ColumnName: colName,
			}
			columnPrivNotExist = append(columnPrivNotExist, columnPriv)
		}
	}
	if len(columnPrivNotExist) > 0 {
		result := tx.Model(&storage.ColumnPriv{}).Create(&columnPrivNotExist)
		return result.Error
	}
	return nil

}
