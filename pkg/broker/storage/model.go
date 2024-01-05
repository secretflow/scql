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
)

type Project struct {
	// ->;<-:create means read and create
	// id can't be modified
	ID          string        `gorm:"column:id;type:varchar(64);primaryKey;uniqueIndex:;comment:'unique id';->;<-:create"`
	Name        string        `gorm:"column:name;type:varchar(64);not null;comment:'project name'"`
	Description string        `gorm:"column:desc;type:varchar(64);comment:'description'"`
	Creator     string        `gorm:"column:creator;type:varchar(64);comment:'creator of the project'"`
	Archived    bool          `gorm:"column:archived;comment:'if archived is true, whole project can't be modified'"`
	ProjectConf ProjectConfig `gorm:"embedded"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Member struct {
	ProjectID string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	Member    string `gorm:"column:member;type:varchar(64);primaryKey;not null;comment:'member in the project'"`
}

type ProjectConfig struct {
	// in json format
	SpuConf string `gorm:"column:spu_conf;comment:'description'"`
}

type TableIdentifier struct {
	ProjectID string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
}

type Table struct {
	TableIdentifier
	RefTable string `gorm:"column:ref_table;type:varchar(128);comment:'ref table'"`
	DBType   string `gorm:"column:db_type;type:varchar(64);comment:'database type like MYSQL'"`
	Owner    string `gorm:"column:owner;comment:'ref table'"`
	// view
	IsView       bool   `gorm:"column:is_view;comment:'this table is a view'"`
	SelectString string `gorm:"column:select_string;comment:'the internal select query in string format, the field is valid only when IsView is true'"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

type ColumnIdentifier struct {
	ProjectID  string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName  string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
	ColumnName string `gorm:"column:column_name;type:varchar(64);primaryKey;not null;"`
}

type Column struct {
	ColumnIdentifier
	DType     string `gorm:"column:data_type;type:varchar(64);comment:'data type like float'"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ColumnPrivIdentifier struct {
	ProjectID  string `gorm:"column:project_id;type:varchar(64);primaryKey;not null"`
	TableName  string `gorm:"column:table_name;type:varchar(64);primaryKey;not null"`
	ColumnName string `gorm:"column:column_name;type:varchar(64);primaryKey;not null;"`
	DestParty  string `gorm:"column:dest_party;type:varchar(64);primaryKey;not null;"`
}

type ColumnPriv struct {
	ColumnPrivIdentifier
	Priv      string `gorm:"column:priv;type:varchar(256);comment:'priv of column'"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Invitation struct {
	ID          uint64        `gorm:"column:id;primaryKey;comment:'auto generated increment id'"`
	ProjectID   string        `gorm:"column:project_id;type:varchar(64);not null;index:,composite:identifier;comment:'project id'"`
	Name        string        `gorm:"column:name;type:varchar(64);comment:'name'"`
	Description string        `gorm:"column:desc;type:varchar(64);comment:'description'"`
	Creator     string        `gorm:"column:creator;type:varchar(64);comment:'creator of the project'"`
	Member      string        `gorm:"column:member;type:varchar(64);not null;comment:'members, flattened string, like: alice;bob'"`
	ProjectConf ProjectConfig `gorm:"embedded"`
	Inviter     string        `gorm:"column:inviter;type:varchar(256);index:,composite:identifier;comment:'inviter'"`
	Invitee     string        `gorm:"column:invitee;type:varchar(256);index:,composite:identifier;comment:'invitee'"`
	// 0: default, not decided to accept invitation or not; 1: accepted; 2: rejected; 3: invalid
	Status     int8 `gorm:"column:status;default:0;comment:'accepted'"`
	InviteTime time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
