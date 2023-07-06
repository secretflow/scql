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

package privileges

import (
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/sessionctx"
)

// SCDBPrivilege is the in-memory cache of scdb privilege tables.
// NOTE(shunde.csd)：currently it fetch information from storage directly, not a real cache
type SCDBPrivilege struct {
	Storage *gorm.DB
}

// userRecord is used to represent a user record in privilege cache.
type userRecord struct {
	Host     string
	User     string
	Password string

	Privileges mysql.PrivilegeType
}

func (p *SCDBPrivilege) matchUser(userName, hostName string) (*userRecord, error) {
	var user storage.User
	result := p.Storage.Where(&storage.User{User: userName, Host: hostName}).Find(&user)

	if result.Error != nil {
		return nil, errors.Wrap(result.Error, fmt.Sprintf("SCDBPrivilege.matchUser user %v failed", userName))
	}

	if result.RowsAffected == 0 {
		return nil, nil
	}

	return &userRecord{
		Host:       user.Host,
		User:       user.User,
		Password:   user.Password,
		Privileges: user.Privs(),
	}, nil
}

type dbPrivRecord struct {
	Host string
	User string
	Db   string

	Privileges mysql.PrivilegeType
}

func (p *SCDBPrivilege) matchDb(user, host, db string) (*dbPrivRecord, error) {
	var dbPriv storage.DatabasePriv
	result := p.Storage.Where(&storage.DatabasePriv{Host: host, User: user, Db: db}).Find(&dbPriv)
	if result.Error != nil {
		return nil, errors.Wrap(result.Error, "SCDBPrivilege.mathDb error")
	}

	if result.RowsAffected == 0 {
		return nil, nil
	}

	return &dbPrivRecord{
		Host:       dbPriv.Host,
		User:       dbPriv.User,
		Db:         dbPriv.Db,
		Privileges: dbPriv.Privs(),
	}, nil
}

type tablesPrivRecord struct {
	Host      string
	User      string
	Db        string
	TableName string

	TablePriv mysql.PrivilegeType
}

func (p *SCDBPrivilege) matchTables(user, host, db, table string) (*tablesPrivRecord, error) {
	var tablePriv storage.TablePriv
	result := p.Storage.Where(&storage.TablePriv{Host: host, User: user, Db: db, TableName: table}).Find(&tablePriv)
	if result.Error != nil {
		return nil, errors.Wrap(result.Error, "SCDB.matchTables error")
	}

	if result.RowsAffected == 0 {
		return nil, nil
	}

	return &tablesPrivRecord{
		Host:      tablePriv.Host,
		User:      tablePriv.User,
		Db:        tablePriv.Db,
		TableName: tablePriv.TableName,
		TablePriv: tablePriv.Privs(),
	}, nil
}

// RequestVerification checks whether the user have sufficient privileges to do the operation.
func (p *SCDBPrivilege) RequestVerification(activeRoles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType) (bool, error) {
	// roleList := p.FindAllRole(activeRoles)
	// roleList = append(roleList, &auth.RoleIdentity{Username: user, Hostname: host})
	roleList := []*auth.RoleIdentity{
		{Username: user, Hostname: host},
	}

	var userPriv mysql.PrivilegeType
	for _, r := range roleList {
		userRecord, err := p.matchUser(r.Username, r.Hostname)
		if err != nil {
			return false, errors.Wrap(err, "SCDBPrivilege.RequestVerification error")
		}
		if userRecord != nil {
			userPriv |= userRecord.Privileges
		}
	}

	if userPriv&priv > 0 {
		return true, nil
	}

	if db == "" {
		return false, nil
	}

	var dbPriv mysql.PrivilegeType
	for _, r := range roleList {
		dbRecord, err := p.matchDb(r.Username, r.Hostname, db)
		if err != nil {
			return false, errors.Wrap(err, "SCDBPrivilege.RequestVerification error")
		}

		if dbRecord != nil {
			dbPriv |= dbRecord.Privileges
		}
	}
	if dbPriv&priv > 0 {
		return true, nil
	}

	if table == "" {
		return false, nil
	}

	var tablePriv mysql.PrivilegeType
	for _, r := range roleList {
		tableRecord, err := p.matchTables(r.Username, r.Hostname, db, table)
		if err != nil {
			return false, errors.Wrap(err, "SCDBPrivilege.RequestVerification error")
		}

		if tableRecord != nil {
			tablePriv |= tableRecord.TablePriv
		}
	}
	if tablePriv&priv > 0 {
		return true, nil
	}

	// NOTICE(shunde.csd): we do not check the column visibility privileges here
	// it will be checked in scql-interpreter or related executor
	if priv == 0 {
		return true, nil
	}
	return false, fmt.Errorf("user %v need privilege %v to do this operation", user, mysql.Priv2UserCol[priv])
}

// DBIsVisible checks whether the user can see the db for privilege `priv`
// Example of the `priv` to check is mysql.ShowPriv or mysql.DescribePriv
func (p *SCDBPrivilege) DBIsVisible(user, host, db string, priv mysql.PrivilegeType) (bool, error) {
	// User who has `CreatePriv` also has privilege for `SHOW`, `DESCRIBE`
	equivalentlyInclude := func(actualValue, toCheckValue mysql.PrivilegeType) bool {
		exactlyInclude := func(actualValue, toCheckValue mysql.PrivilegeType) bool {
			return (actualValue & toCheckValue) == toCheckValue
		}
		return exactlyInclude(actualValue, priv) || exactlyInclude(actualValue, mysql.CreatePriv)
	}
	// 1. check user
	userRecord, err := p.matchUser(user, host)
	if err != nil {
		return false, errors.Wrap(err, "SCDBPrivilege.DBIsVisible error")
	}
	if userRecord != nil && equivalentlyInclude(userRecord.Privileges, priv) {
		return true, nil
	}

	// 2. check database
	if db == "" {
		return false, nil
	}
	dbRecord, err := p.matchDb(user, host, db)
	if err != nil {
		return false, errors.Wrap(err, "SCDBPrivilege.DBIsVisible error")
	}
	return dbRecord != nil && equivalentlyInclude(dbRecord.Privileges, priv), nil
}

// Handle wraps SCDBPrivilege providing thread safe access.
type Handle struct {
	priv atomic.Value
}

// NewHandle returns a Handle.
func NewHandle(ctx sessionctx.Context) *Handle {
	h := &Handle{}
	h.Update(ctx)
	return h
}

// Get the SCDBPrivilege for read.
func (h *Handle) Get() *SCDBPrivilege {
	if result, ok := h.priv.Load().(*SCDBPrivilege); ok {
		return result
	}
	return nil
}

// Update loads all the privilege info from storage.
func (h *Handle) Update(ctx sessionctx.Context) error {
	var priv SCDBPrivilege
	// FIXME(shunde.csd): load all the privilege info into in-memory cache from storage
	// NOTE(shunde.csd): for simpilicity, just clone a storage connection
	priv.Storage = ctx.GetSessionVars().Storage

	h.priv.Store(&priv)
	return nil
}
