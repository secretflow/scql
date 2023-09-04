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
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/model"
)

// Database Table stores the metadata of a database.
// `CREATE DATABASE` statement will add a row in this table.
type Database struct {
	ID        uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Db        string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_db;comment:'database name'"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Table Table stores the metadata of a table.
// `CREATE TABLE` statement will add a row in this table.
type Table struct {
	ID           uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Db           string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_db_table;comment:'database name'"`
	Table        string `gorm:"column:table_name;type:varchar(64);uniqueIndex:uk_db_table;comment:'table name'"`
	Schema       string `gorm:"column:table_schema;comment:'table schema'"`
	Owner        string `gorm:"column:owner;type:varchar(64);comment:'owner user name'"`
	Host         string `gorm:"column:host;type:varchar(64);comment:'owner user host'"`
	RefDb        string `gorm:"column:ref_db;type:varchar(64);comment:'reference database name'"`
	RefTable     string `gorm:"column:ref_table;type:varchar(64);comment:'reference table name'"`
	IsView       bool   `gorm:"column:is_view;comment:'this table is a view'"`
	SelectString string `gorm:"column:select_string;comment:'the internal select query in string format'"`
	DBType       int    `gorm:"column:db_type;comment:'database type where table data stored'"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Column Table stores the columns of a table
// `CREATE TABLE` statement will add some rows in this table
// This table mimics `information_schema.columns` in MySQL
type Column struct {
	ID              uint   `gorm:"column:id;primaryKey;comment:'unique id'"`
	Db              string `gorm:"column:db;type:varchar(64);uniqueIndex:uk_db_table_col;comment:'database name'"`
	TableName       string `gorm:"column:table_name;type:varchar(64);uniqueIndex:uk_db_table_col;comment:'table name'"`
	ColumnName      string `gorm:"column:column_name;type:varchar(64);uniqueIndex:uk_db_table_col;comment:'column name'"`
	OrdinalPosition uint   `gorm:"column:ordinal_position;comment:'ordinal position'"`
	Type            string `gorm:"column:type;type:varchar(32);comment:'column type'"`
	Description     string `gorm:"column:description;type:varchar(1024);comment:'column description'"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	DropSchema(schema model.CIStr) error
	DropTable(schema, tblName model.CIStr) error
}

// Enforce DDLHandler implements DDL interface
var _ DDL = (*DDLHandler)(nil)

// DDLHandler handles DDL
type DDLHandler struct {
	store *gorm.DB
}

func NewDDLHandler(db *gorm.DB) DDL {
	return &DDLHandler{
		store: db,
	}
}

func (h *DDLHandler) DropSchema(schema model.CIStr) error {
	callFc := func(tx *gorm.DB) error {
		return h.dropSchemaByName(tx, schema.String())
	}
	if err := h.store.Transaction(callFc); err != nil {
		return fmt.Errorf("dropSchema: %v", err)
	}
	return nil
}

func (h *DDLHandler) DropTable(schema, tblName model.CIStr) error {
	callFc := func(tx *gorm.DB) error {
		return h.dropTableByName(tx, schema.String(), tblName.String())
	}
	if err := h.store.Transaction(callFc); err != nil {
		return fmt.Errorf("dropTable: %v", err)
	}
	return nil
}

func (h *DDLHandler) dropSchemaByName(store *gorm.DB, schemaName string) error {
	// drop table
	err := h.dropTableByName(store, schemaName, "")
	if err != nil {
		return err
	}
	// remove database in table databases
	result := store.Model(&Database{}).Where("db = ?", schemaName).Delete(&Database{})
	if result.Error != nil {
		return result.Error
	}

	// delete related database privilege
	result = store.Model(&DatabasePriv{}).Where("db = ?", schemaName).Delete(&DatabasePriv{})
	if result.Error != nil {
		return result.Error
	}

	return nil
}

// dropTableByName removes table `tableName` in database `db`
// it will remove all tables in database `db` if `tableName` is empty
func (h *DDLHandler) dropTableByName(store *gorm.DB, dbName, tableName string) (err error) {
	if dbName == "" {
		return fmt.Errorf("ddlHandler.dropTablesInDatabase: db name cannot be empty")
	}

	result := store.Where(&Table{Db: dbName, Table: tableName}).Delete(&Table{})
	if result.Error != nil {
		return result.Error
	}

	// delete related table privileges
	result = store.Where(&TablePriv{Db: dbName, TableName: tableName}).Delete(&TablePriv{})
	if result.Error != nil {
		return result.Error
	}

	// delete related columns
	result = store.Where(&Column{Db: dbName, TableName: tableName}).Delete(&Column{})
	if result.Error != nil {
		return result.Error
	}

	// delete related column privileges
	result = store.Where(&ColumnPriv{Db: dbName, TableName: tableName}).Delete(&ColumnPriv{})
	if result.Error != nil {
		return result.Error
	}
	return nil
}
