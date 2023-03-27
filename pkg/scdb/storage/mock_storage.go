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
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewMemoryStorage inits a in memory storage
// use it only for testing
func NewMemoryStorage() (*gorm.DB, error) {
	// Use in memory SQLite for testing
	// In production, we may use MySQL as backend, setting the default database to `scdb`
	// so that we can access scdb.user, scdb.table etc.
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Migrate the schema
	if err := db.AutoMigrate(&User{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&Database{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&Table{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&Column{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&DatabasePriv{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&TablePriv{}); err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&ColumnPriv{}); err != nil {
		return nil, err
	}

	return db, nil
}
