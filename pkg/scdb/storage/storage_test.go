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
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
)

func TestDDLHandlerDropSchema(t *testing.T) {
	r := require.New(t)

	// given
	db, err := newDbStore()
	r.NoError(err)

	// init db
	records := []interface{}{
		&Database{Db: "da1"},
		&Database{Db: "da2"},
		&DatabasePriv{Db: "da1", User: "root", CreatePriv: true},
		&Table{Db: "da1", Table: "t1"},
		&TablePriv{Db: "da1", TableName: "t1"},
		&Column{Db: "da1", TableName: "t1", ColumnName: "c1", Type: "string"},
		&Column{Db: "da1", TableName: "t1", ColumnName: "c2", Type: "string"},
		&ColumnPriv{Db: "da1", TableName: "t1", ColumnName: "c2", VisibilityPriv: mysql.PlaintextPriv, User: "root"},
	}
	r.NoError(batchInsert(db, records))

	handler := &DDLHandler{store: db}

	// when
	err = handler.DropSchema(model.NewCIStr("da1"))

	// then
	r.NoError(err)

	// check database
	var databases []Database
	result := db.Find(&databases)
	r.NoError(result.Error)
	r.Equal(1, len(databases))
	r.Equal("da2", databases[0].Db)

	// check database priv
	result = db.Find(&DatabasePriv{})
	r.NoError(result.Error)
	r.Zero(result.RowsAffected)

	// check table
	result = db.Find(&Table{})
	r.NoError(result.Error)
	r.Zero(result.RowsAffected)

	// check table priv
	result = db.Find(&TablePriv{})
	r.NoError(result.Error)
	r.Zero(result.RowsAffected)

	// check column
	result = db.Find(&Column{})
	r.NoError(result.Error)
	r.Zero(result.RowsAffected)

	// check column priv
	result = db.Find(&ColumnPriv{})
	r.NoError(result.Error)
	r.Zero(result.RowsAffected)
}

func newDbStore() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = Bootstrap(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func batchInsert(db *gorm.DB, records []interface{}) error {
	for _, record := range records {
		result := db.Create(record)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}
