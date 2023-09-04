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

	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/scdb/auth"
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
	tx := e.ctx.GetSessionVars().Storage.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	switch x := e.Statement.(type) {
	case *ast.CreateUserStmt:
		err = e.executeCreateUser(tx, x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(tx, x)
	case *ast.AlterUserStmt:
		err = e.executeAlterUser(tx, x)
	default:
		err = fmt.Errorf("simpleExec.Next: unsupported ast %v", x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) executeCreateUser(store *gorm.DB, s *ast.CreateUserStmt) (err error) {
	if len(s.Specs) != 1 {
		return fmt.Errorf("create user statement only supports creating one user at a time")
	}
	spec := s.Specs[0]
	userName := spec.User.Username
	hostName := spec.User.Hostname
	partyCode := spec.PartyCode

	if partyCode == "" {
		return fmt.Errorf("user %v has no party code", userName)
	}

	var userExist storage.User
	if result := store.Where(&storage.User{PartyCode: spec.PartyCode}).Find(&userExist); result.Error != nil || result.RowsAffected != 0 {
		if result.Error != nil {
			return fmt.Errorf("simpleExec.executeCreateUser: %v", err)
		}
		if userExist.User == userName && userExist.Host == hostName {
			if s.IfNotExists {
				return nil
			}
			return fmt.Errorf("user %v host %v already exists", userName, hostName)
		}
		return fmt.Errorf("only one user is allowed to exist in a party, user %v is already exist in party %v", userExist.User, spec.PartyCode)
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
		return fmt.Errorf("failed to encode password")
	}

	if pa := auth.GetPartyAuthenticator(e.ctx); pa != nil {
		if err := pa.VerifyPartyAuthentication(s.EngineOpt); err != nil {
			return fmt.Errorf("simpleExec.executeCreateUser: %+v", err)
		}
	}

	user := storage.User{
		Host:      hostName,
		User:      strings.ToLower(userName),
		Password:  pwd,
		PartyCode: spec.PartyCode,
	}

	if s.EngineOpt != nil {
		if s.EngineOpt.TokenAuth != nil && s.EngineOpt.PubKeyAuth != nil {
			return fmt.Errorf("invalid Engine Auth option")
		}
		if s.EngineOpt.TokenAuth != nil {
			user.EngineAuthMethod = int(storage.TokenAuth)
			user.EngineToken = s.EngineOpt.TokenAuth.Token
		}
		if s.EngineOpt.PubKeyAuth != nil {
			user.EngineAuthMethod = int(storage.PubKeyAuth)
			user.EnginePubKey = s.EngineOpt.PubKeyAuth.PubKey
		}
		// concate multiply endpoints with
		user.EngineEndpoints = strings.Join(s.EngineOpt.Endpoints, ";")
	}

	if result := store.Create(&user); result.Error != nil {
		return fmt.Errorf("simpleExec.executeCreateUser: %v", result.Error)
	}
	return nil
}

func (e *SimpleExec) executeDropUser(store *gorm.DB, s *ast.DropUserStmt) (err error) {
	if len(s.UserList) != 1 {
		return fmt.Errorf("simpleExec.executeDropUser: drop user statement only supports dropping one user at one time")
	}

	user := s.UserList[0]
	if exist, err := storage.CheckUserExist(transaction.AddExclusiveLock(store), user.Username, user.Hostname); err != nil || !exist {
		if err != nil {
			return err
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("drop an non-exist user `%v`", user)
	}

	// delete privileges from scdb.databases_priv
	result := store.Where(&storage.DatabasePriv{User: user.Username, Host: user.Hostname}).Delete(&storage.DatabasePriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.databases_priv: %v", result.Error)
	}

	// delete privileges from scdb.tables_priv
	result = store.Where(&storage.TablePriv{User: user.Username, Host: user.Hostname}).Delete(&storage.TablePriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.tables_priv: %v", result.Error)
	}

	// delete privileges from scdb.column_priv
	result = store.Where(&storage.ColumnPriv{User: user.Username, Host: user.Hostname}).Delete(&storage.ColumnPriv{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete privileges from scdb.column_priv: %v", result.Error)
	}

	// delete user from scdb.user
	result = store.Where(&storage.User{User: user.Username, Host: user.Hostname}).Delete(&storage.User{})
	if result.Error != nil {
		return fmt.Errorf("simpleExec.executeDropUser: %v", result.Error)
	}

	return nil
}

func (e *SimpleExec) executeAlterUser(store *gorm.DB, s *ast.AlterUserStmt) (err error) {
	if len(s.Specs) != 1 {
		return fmt.Errorf("alter user statement only supports alter one user at one time")
	}
	spec := s.Specs[0]
	userName := spec.User.Username
	hostName := spec.User.Hostname

	authUserName := e.ctx.GetSessionVars().User.Username
	authHostName := e.ctx.GetSessionVars().User.Hostname

	if (authUserName != userName) || (authHostName != hostName) {
		return fmt.Errorf("only support user themselves to execute alter user")
	}

	if exist, err := storage.CheckUserExist(transaction.AddExclusiveLock(store), userName, hostName); err != nil || !exist {
		if err != nil {
			return err
		}
		if s.IfExists {
			return nil
		}
		return fmt.Errorf("user %v host %v not exists", userName, hostName)
	}

	updateNeeded := false
	newUser := &storage.User{}

	if spec.AuthOpt != nil && spec.AuthOpt.ByAuthString && spec.AuthOpt.AuthString != "" {
		if err := storage.CheckValidPassword(spec.AuthOpt.AuthString); err != nil {
			return fmt.Errorf("password for use %v is not valid: %v", userName, err)
		}
		pwd, ok := spec.EncodedPassword()
		if !ok {
			return fmt.Errorf("failed to encode password")
		}
		updateNeeded = true
		newUser.Password = pwd
	}

	if s.EngineOpt != nil {
		if s.EngineOpt.TokenAuth != nil || s.EngineOpt.PubKeyAuth != nil {
			return fmt.Errorf("alterUserStmt don't support alter engine authentication setting")
		}
		if len(s.EngineOpt.Endpoints) > 0 {
			updateNeeded = true
			newUser.EngineEndpoints = strings.Join(s.EngineOpt.Endpoints, ";")
		}
	}

	if !updateNeeded {
		return fmt.Errorf("no password or endpoint provided for user %v", userName)
	}

	result := store.Model(&storage.User{}).Where(&storage.User{
		Host: hostName,
		User: userName,
	}).Updates(newUser)

	if result.Error != nil {
		return result.Error
	}
	return nil

}
