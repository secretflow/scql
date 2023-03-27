// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/transaction"
)

// Revoke Statement
// See https://dev.mysql.com/doc/refman/5.7/en/revoke.html

var (
	_ Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  sessionctx.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
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

	if len(e.Level.DBName) == 0 {
		e.Level.DBName = e.ctx.GetSessionVars().CurrentDB
	}

	// Revoke for each user.
	for _, user := range e.Users {
		// Check if user exists.
		if exist, err := storage.CheckUserExist(tx, user.User.Username, user.User.Hostname); err != nil || !exist {
			if err != nil {
				return err
			}
			return errors.Errorf("Unknown user: %s", user.User)
		}

		err = e.revokeOneUser(tx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *RevokeExec) revokeOneUser(tx *gorm.DB, user, host string) (err error) {

	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		if err = e.revokeGlobalLevelPriv(tx, user, host); err != nil {
			return err
		}
	case ast.GrantLevelDB:
		if err = e.revokeDBLevelPriv(tx, user, host); err != nil {
			return err
		}
	case ast.GrantLevelTable:
		if err = e.revokeTableLevelPriv(tx, user, host); err != nil {
			return err
		}
	default:
		return errors.Errorf("unknown revoke level %v", e.Level.Level)
	}
	return nil
}

func (e *RevokeExec) revokeGlobalLevelPriv(tx *gorm.DB, user, host string) error {
	return fmt.Errorf("unsupported revokeGlobalPriv")
}

func (e *RevokeExec) revokeDBLevelPriv(tx *gorm.DB, user, host string) error {
	return fmt.Errorf("unsupported revokeDBPriv")
}

func (e *RevokeExec) revokeTableLevelPriv(tx *gorm.DB, user, host string) (err error) {
	for _, priv := range e.Privs {
		if len(priv.Cols) == 0 {
			err = e.revokeTablePriv(tx, priv, user, host)
		} else {
			err = e.revokeColumnPriv(tx, priv, user, host)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *RevokeExec) revokeTablePriv(tx *gorm.DB, priv *ast.PrivElem, user, host string) error {
	return fmt.Errorf("unsupported revokeTablePriv")
}

func (e *RevokeExec) revokeColumnPriv(tx *gorm.DB, priv *ast.PrivElem, user, host string) error {
	tblName := e.Level.TableName
	userName := e.ctx.GetSessionVars().User.Username
	hostName := e.ctx.GetSessionVars().User.Hostname
	if err := storage.CheckTableOwner(tx, e.Level.DBName, tblName, userName, hostName); err != nil {
		return err
	}

	var colNames []string
	for _, c := range priv.Cols {
		colNames = append(colNames, c.Name.L)
	}

	if err := storage.CheckColumnsExist(tx, e.Level.DBName, tblName, colNames); err != nil {
		return err
	}

	_, ok := storage.VisibilityPriv2UserCol[priv.Priv]
	if !ok {
		return fmt.Errorf("revokeColumnPriv: doesn't support privType %s", mysql.Priv2Str[priv.Priv])
	}

	condition := storage.ColumnPriv{
		Host:           host,
		Db:             e.Level.DBName,
		User:           user,
		TableName:      tblName,
		VisibilityPriv: priv.Priv,
	}
	var columnPrivExist []storage.ColumnPriv
	if err := transaction.AddExclusiveLock(tx).Model(&storage.ColumnPriv{}).Where(&condition).Find(&columnPrivExist, "column_name IN ?", colNames).Error; err != nil {
		return err
	}

	columnPrivExistMap := make(map[string]bool, len(columnPrivExist))
	for _, col := range columnPrivExist {
		columnPrivExistMap[col.ColumnName] = true
	}

	for _, colName := range colNames {
		if _, exist := columnPrivExistMap[colName]; !exist {
			return fmt.Errorf("there is no %s defined for user '%s' on host '%s' on table column '%s.%s'",
				mysql.Priv2Str[priv.Priv], user, host, tblName, colName)
		}
	}

	if err := tx.Model(&storage.ColumnPriv{}).Delete(&columnPrivExist).Error; err != nil {
		return fmt.Errorf("revokeColumnPriv failed: %v", err)
	}

	return nil
}
