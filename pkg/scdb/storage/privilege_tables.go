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

package storage

import (
	"time"

	"github.com/secretflow/scql/pkg/parser/mysql"
)

const (
	VisibilityPrivColumnName = "visibility_priv"
)

// AllGlobalPrivs is all the privileges in global scope in scdb.
var AllGlobalPrivs = []mysql.PrivilegeType{mysql.CreatePriv, mysql.CreateUserPriv, mysql.DropPriv, mysql.GrantPriv}

var GlobalPrivColName = map[mysql.PrivilegeType]string{
	mysql.CreatePriv:     mysql.Priv2UserCol[mysql.CreatePriv],
	mysql.CreateUserPriv: mysql.Priv2UserCol[mysql.CreateUserPriv],
	mysql.DropPriv:       mysql.Priv2UserCol[mysql.DropPriv],
	mysql.GrantPriv:      mysql.Priv2UserCol[mysql.GrantPriv],
}

type EngineAuthMethod int

const (
	UnknownAuth EngineAuthMethod = iota
	TokenAuth
	PubKeyAuth
)

// User Table stores the metadata of a user.
// `CREATE USER` statement will add a row in this table.
// This table mimics `mysql.user` in MySQL
type User struct {
	ID               uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Host             string `gorm:"column:host;type:varchar(128);uniqueIndex:uk_host_user;comment:'host name'"`
	User             string `gorm:"column:user;type:varchar(64);uniqueIndex:uk_host_user;comment:'user name'"`
	Password         string `gorm:"column:password;type:varchar(256);comment:'password'"`
	PartyCode        string `gorm:"column:party_code;type:varchar(64);comment:'The party code the user belongs to'"`
	CreatePriv       bool   `gorm:"column:create_priv;comment:'create privilege'"`
	CreateUserPriv   bool   `gorm:"column:create_user_priv;comment:'create user privilege'"`
	DropPriv         bool   `gorm:"column:drop_priv;comment:'drop privilege'"`
	GrantPriv        bool   `gorm:"column:grant_priv;comment:'grant privilege'"`
	DescribePriv     bool   `gorm:"column:describe_priv;comment:'describe privilege'"`
	ShowPriv         bool   `gorm:"column:show_priv;comment:'show privilege'"`
	CreateViewPriv   bool   `gorm:"column:create_view_priv;comment:'create view privilege'"`
	EngineAuthMethod int    `gorm:"column:eng_auth_method;comment:'0:unknown; 1:token; 2:public key'"`
	EngineToken      string `gorm:"column:eng_token;type:text;comment:'scqlengine token string'"`
	EnginePubKey     string `gorm:"column:eng_pubkey;type:text;comment:'scqlengine public key'"`
	EngineEndpoints  string `gorm:"column:eng_endpoints;type:text;comment:'scqlengine endpoint list, multiple endpoints concated with ;'"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Privs return all the privileges the user have
func (user *User) Privs() mysql.PrivilegeType {
	var privs mysql.PrivilegeType

	if user.CreatePriv {
		privs |= mysql.CreatePriv
	}

	if user.CreateUserPriv {
		privs |= mysql.CreateUserPriv
	}

	if user.DropPriv {
		privs |= mysql.DropPriv
	}

	if user.GrantPriv {
		privs |= mysql.GrantPriv
	}

	if user.DescribePriv {
		privs |= mysql.DescribePriv
	}

	if user.ShowPriv {
		privs |= mysql.ShowPriv
	}

	if user.CreateViewPriv {
		privs |= mysql.CreateViewPriv
	}

	return privs
}

// AllDBPrivs is all the privileges in database scope in scdb.
var AllDBPrivs = []mysql.PrivilegeType{mysql.CreatePriv, mysql.DropPriv, mysql.GrantPriv}

var DbPrivColName = map[mysql.PrivilegeType]string{
	mysql.CreatePriv: mysql.Priv2UserCol[mysql.CreatePriv],
	mysql.DropPriv:   mysql.Priv2UserCol[mysql.DropPriv],
	mysql.GrantPriv:  mysql.Priv2UserCol[mysql.GrantPriv],
}

// DatabasePriv Table stores the metadata of a database level user privilege.
// `GRANT ... ON db_name.* TO user_name` statement will add a row in this table.
// This table mimics `mysql.db` in MySQL
type DatabasePriv struct {
	ID             uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Host           string `gorm:"column:host;type:varchar(128);uniqueIndex:uk_host_user_db;comment:'host name'"`
	Db             string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_host_user_db;comment:'database name'"`
	User           string `gorm:"column:user;type:varchar(64);uniqueIndex:uk_host_user_db;comment:'user name'"`
	CreatePriv     bool   `gorm:"column:create_priv;comment:'create privilege'"`
	DropPriv       bool   `gorm:"column:drop_priv;comment:'drop privilege'"`
	GrantPriv      bool   `gorm:"column:grant_priv;comment:'grant privilege'"`
	DescribePriv   bool   `gorm:"column:describe_priv;comment:'describe privilege'"`
	ShowPriv       bool   `gorm:"column:show_priv;comment:'show privilege'"`
	CreateViewPriv bool   `gorm:"column:create_view_priv;comment:'create view privilege'"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Privs return all the privileges an user has on a db
func (dbPriv *DatabasePriv) Privs() mysql.PrivilegeType {
	var privs mysql.PrivilegeType

	if dbPriv.CreatePriv {
		privs |= mysql.CreatePriv
	}

	if dbPriv.DropPriv {
		privs |= mysql.DropPriv
	}

	if dbPriv.GrantPriv {
		privs |= mysql.GrantPriv
	}

	if dbPriv.DescribePriv {
		privs |= mysql.DescribePriv
	}

	if dbPriv.ShowPriv {
		privs |= mysql.ShowPriv
	}

	if dbPriv.CreateViewPriv {
		privs |= mysql.CreateViewPriv
	}

	return privs
}

// AllTablePrivs is all the privileges in table scope in scdb.
var AllTablePrivs = []mysql.PrivilegeType{mysql.CreatePriv, mysql.DropPriv, mysql.GrantPriv}

// VisibilityPrivColName is the privilege to scdb.tables_priv and scdb.columns_priv table column name.
var VisibilityPrivColName = map[mysql.PrivilegeType]string{
	mysql.PlaintextPriv:               VisibilityPrivColumnName,
	mysql.PlaintextAfterComparePriv:   VisibilityPrivColumnName,
	mysql.PlaintextAfterAggregatePriv: VisibilityPrivColumnName,
	mysql.EncryptedOnlyPriv:           VisibilityPrivColumnName,
	mysql.PlaintextAfterJoinPriv:      VisibilityPrivColumnName,
	mysql.PlaintextAsJoinPayloadPriv:  VisibilityPrivColumnName,
	mysql.PlaintextAfterGroupByPriv:   VisibilityPrivColumnName,
}

var TablePrivColName = map[mysql.PrivilegeType]string{
	mysql.CreatePriv: mysql.Priv2UserCol[mysql.CreatePriv],
	mysql.DropPriv:   mysql.Priv2UserCol[mysql.DropPriv],
	mysql.GrantPriv:  mysql.Priv2UserCol[mysql.GrantPriv],
}

// TablePriv Table stores the metadata of a table level user privilege.
// `GRANT ... ON db_name.table_name.* TO user_name` statement will add a row in this table.
// This table mimics `mysql.tables_priv` in MySQL
type TablePriv struct {
	ID        uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Host      string `gorm:"column:host;type:varchar(128);uniqueIndex:uk_host_user_db_table;comment:'host name'"`
	Db        string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_host_user_db_table;comment:'database name'"`
	User      string `gorm:"column:user;type:varchar(64);uniqueIndex:uk_host_user_db_table;comment:'user name'"`
	TableName string `gorm:"column:table_name;type:varchar(64);uniqueIndex:uk_host_user_db_table;comment:'table name'"`
	// table privs
	CreatePriv     bool                `gorm:"column:create_priv;comment:'create privilege'"`
	DropPriv       bool                `gorm:"column:drop_priv;comment:'drop privilege'"`
	GrantPriv      bool                `gorm:"column:grant_priv;comment:'grant privilege'"`
	VisibilityPriv mysql.PrivilegeType `gorm:"column:visibility_priv;comment:'visibility privilege'"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Privs return all the privileges an user has on a table
func (tblPriv *TablePriv) Privs() mysql.PrivilegeType {
	var privs mysql.PrivilegeType

	if tblPriv.CreatePriv {
		privs |= mysql.CreatePriv
	}

	if tblPriv.DropPriv {
		privs |= mysql.DropPriv
	}

	if tblPriv.GrantPriv {
		privs |= mysql.GrantPriv
	}

	return privs
}

// ColumnPriv Table stores the metadata of a column level user privilege.
type ColumnPriv struct {
	ID         uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Host       string `gorm:"column:host;type:varchar(128);uniqueIndex:uk_host_user_db_table_col;comment:'host name'"`
	Db         string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_host_user_db_table_col;comment:'database name'"`
	User       string `gorm:"column:user;type:varchar(64);uniqueIndex:uk_host_user_db_table_col;comment:'user name'"`
	TableName  string `gorm:"column:table_name;type:varchar(64);uniqueIndex:uk_host_user_db_table_col;comment:'table name'"`
	ColumnName string `gorm:"column:column_name;type:varchar(64);uniqueIndex:uk_host_user_db_table_col;comment:'column name'"`

	VisibilityPriv mysql.PrivilegeType `gorm:"column:visibility_priv;comment:'visibility privilege'"`
}
