// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/transaction"
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
type SimpleExec struct {
	baseExecutor

	Statement ast.StmtNode
	done      bool
	is        infoschema.InfoSchema
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	switch x := e.Statement.(type) {
	case *ast.CreateUserStmt:
		err = e.executeCreateUser(ctx, x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(x)
	default:
		err = fmt.Errorf("simpleExec.Next: unsupported ast %v", x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) (err error) {

	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	if len(s.Specs) != 1 {
		return fmt.Errorf("simpleExec.executeCreateUser: create user statement only supports creating one user at a time")
	}
	spec := s.Specs[0]

	// Check if user exists
	userName := spec.User.Username
	hostName := spec.User.Hostname
	if exist, err := storage.CheckUserExist(transaction.AddExclusiveLock(tx), userName, hostName); err != nil || exist {
		if err != nil {
			return fmt.Errorf("simpleExec.executeCreateUser: %v", err)
		}
		if s.IfNotExists {
			return nil
		}
		return fmt.Errorf("user %v host %v already exists", userName, hostName)
	}

	// Add user
	if spec.AuthOpt != nil && spec.AuthOpt.ByAuthString && spec.AuthOpt.AuthString != "" {
		if err := storage.CheckValidPassword(spec.AuthOpt.AuthString); err != nil {
			return fmt.Errorf("password for use %v is not valid: %v", userName, err)
		}
	} else {
		return fmt.Errorf("no password for user %v", userName)
	}
	pwd, ok := spec.EncodedPassword()
	if !ok {
		return errors.Trace(ErrPasswordFormat)
	}
	result := tx.Create(&storage.User{
		Host:      hostName,
		User:      strings.ToLower(userName),
		Password:  pwd,
		PartyCode: spec.PartyCode,
	})
	if result.Error != nil {
		return fmt.Errorf("simpleExec.executeCreateUser: %v", result.Error)
	}

	return nil
}

func (e *SimpleExec) executeDropUser(s *ast.DropUserStmt) (err error) {
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	if len(s.UserList) != 1 {
		return fmt.Errorf("simpleExec.executeDropUser: drop user statement only supports dropping one user at one time")
	}

	user := s.UserList[0]
	if exist, err := storage.CheckUserExist(transaction.AddExclusiveLock(tx), user.Username, user.Hostname); err != nil || !exist {
		if err != nil {
			return err
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("drop an non-exist user `%v`", user)
	}

	// delete privileges from scdb.databases_priv
	result := tx.Where(&storage.DatabasePriv{User: user.Username, Host: user.Hostname}).Delete(&storage.DatabasePriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.databases_priv: %v", result.Error)
	}

	// delete privileges from scdb.tables_priv
	result = tx.Where(&storage.TablePriv{User: user.Username, Host: user.Hostname}).Delete(&storage.TablePriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.tables_priv: %v", result.Error)
	}

	// delete privileges from scdb.column_priv
	result = tx.Where(&storage.ColumnPriv{User: user.Username, Host: user.Hostname}).Delete(&storage.ColumnPriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.column_priv: %v", result.Error)
	}

	// delete user from scdb.user
	result = tx.Where(&storage.User{User: user.Username, Host: user.Hostname}).Delete(&storage.User{})
	if result.Error != nil {
		return fmt.Errorf("simpleExec.executeDropUser: %v", result.Error)
	}

	return nil
}
