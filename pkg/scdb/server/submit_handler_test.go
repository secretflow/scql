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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsQueryNeedInfoSchema(t *testing.T) {
	r := require.New(t)

	testCases := []struct {
		name        string
		query       string
		needSchema  bool
		shouldError bool
	}{
		// GRANT statements - should NOT need InfoSchema
		{
			name:       "GRANT on database level",
			query:      "GRANT CREATE ON db1.* TO alice",
			needSchema: false,
		},
		{
			name:       "GRANT on table level",
			query:      "GRANT CREATE ON db1.table1 TO alice",
			needSchema: false,
		},
		{
			name:       "GRANT with column level - PLAINTEXT",
			query:      "GRANT SELECT PLAINTEXT(col1, col2) ON db1.table1 TO alice",
			needSchema: false,
		},
		{
			name:       "GRANT with column level - PLAINTEXT_AFTER_JOIN",
			query:      "GRANT SELECT PLAINTEXT_AFTER_JOIN(device_number, month_id, day_id) ON scql_xxx.xxxx TO prov",
			needSchema: false,
		},
		{
			name:       "GRANT multiple privileges",
			query:      "GRANT CREATE, DROP, GRANT OPTION ON db1.* TO alice",
			needSchema: false,
		},

		// REVOKE statements - should NOT need InfoSchema
		{
			name:       "REVOKE on database level",
			query:      "REVOKE CREATE ON db1.* FROM alice",
			needSchema: false,
		},
		{
			name:       "REVOKE on table level",
			query:      "REVOKE DROP ON db1.table1 FROM alice",
			needSchema: false,
		},
		{
			name:       "REVOKE with column level",
			query:      "REVOKE SELECT PLAINTEXT_AFTER_AGGREGATE(col1) ON db1.table1 FROM alice",
			needSchema: false,
		},

		// CREATE/DROP/ALTER USER - should NOT need InfoSchema
		{
			name:       "CREATE USER",
			query:      `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`,
			needSchema: false,
		},
		{
			name:       "CREATE USER with full options",
			query:      `CREATE USER alice PARTY_CODE 'party_alice' IDENTIFIED BY 'alice123' WITH '2023-08-23T15:46:16+08:00' 'signature' 'pubkey'`,
			needSchema: false,
		},
		{
			name:       "DROP USER",
			query:      "DROP USER alice",
			needSchema: false,
		},
		{
			name:       "DROP USER IF EXISTS",
			query:      "DROP USER IF EXISTS alice",
			needSchema: false,
		},
		{
			name:       "ALTER USER password",
			query:      `ALTER USER alice IDENTIFIED BY "new_password"`,
			needSchema: false,
		},
		{
			name:       "ALTER USER endpoint",
			query:      "ALTER USER alice WITH ENDPOINT 'engine-alice-host:port'",
			needSchema: false,
		},

		// DROP TABLE - should NOT need InfoSchema
		{
			name:       "DROP TABLE",
			query:      "DROP TABLE db1.table1",
			needSchema: false,
		},
		{
			name:       "DROP TABLE IF EXISTS",
			query:      "DROP TABLE IF EXISTS db1.table1",
			needSchema: false,
		},

		// CREATE/DROP DATABASE - should NOT need InfoSchema
		{
			name:       "CREATE DATABASE",
			query:      "CREATE DATABASE db1",
			needSchema: false,
		},
		{
			name:       "CREATE DATABASE IF NOT EXISTS",
			query:      "CREATE DATABASE IF NOT EXISTS test_db",
			needSchema: false,
		},
		{
			name:       "DROP DATABASE",
			query:      "DROP DATABASE db1",
			needSchema: false,
		},
		{
			name:       "DROP DATABASE IF EXISTS",
			query:      "DROP DATABASE IF EXISTS test_db",
			needSchema: false,
		},

		// CREATE TABLE - should NOT need InfoSchema
		{
			name:       "CREATE TABLE",
			query:      "CREATE TABLE db1.t1 (c1 int) REF_TABLE=d1.t1 DB_TYPE='mysql'",
			needSchema: false,
		},
		{
			name:       "CREATE TABLE IF NOT EXISTS",
			query:      "CREATE TABLE IF NOT EXISTS db1.t1 (c1 int, c2 string) REF_TABLE=d1.t1 DB_TYPE='mysql'",
			needSchema: false,
		},

		// SHOW COLUMNS / DESCRIBE - should NOT need InfoSchema
		{
			name:       "SHOW COLUMNS",
			query:      "SHOW COLUMNS FROM db1.table1",
			needSchema: false,
		},
		{
			name:       "SHOW FULL COLUMNS",
			query:      "SHOW FULL COLUMNS FROM db1.table1",
			needSchema: false,
		},
		{
			name:       "DESCRIBE table",
			query:      "DESCRIBE db1.table1",
			needSchema: false,
		},
		{
			name:       "DESC table",
			query:      "DESC db1.table1",
			needSchema: false,
		},

		// Other statements - should NEED InfoSchema
		{
			name:       "SELECT statement",
			query:      "SELECT * FROM db1.table1",
			needSchema: true,
		},

		// Invalid SQL - should error
		{
			name:        "Invalid SQL",
			query:       "INVALID SQL STATEMENT",
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			needSchema, err := isQueryNeedInfoSchema(tc.query)

			if tc.shouldError {
				r.Error(err, "Expected error for query: %s", tc.query)
			} else {
				r.NoError(err, "Query: %s", tc.query)
				r.Equal(tc.needSchema, needSchema,
					"Query: %s\nExpected needSchema=%v, got %v",
					tc.query, tc.needSchema, needSchema)
			}
		})
	}
}

// TestIsQueryNeedInfoSchema_EdgeCases tests edge cases
func TestIsQueryNeedInfoSchema_EdgeCases(t *testing.T) {
	r := require.New(t)

	// Empty query
	_, err := isQueryNeedInfoSchema("")
	r.Error(err, "Empty query should return error")

	// Query with comments
	needSchema, err := isQueryNeedInfoSchema("/* comment */ GRANT CREATE ON db.* TO user")
	r.NoError(err)
	r.False(needSchema, "GRANT with comment should not need InfoSchema")

	// Case insensitive
	needSchema, err = isQueryNeedInfoSchema("grant create on db.* to user")
	r.NoError(err)
	r.False(needSchema, "Lowercase GRANT should not need InfoSchema")

	needSchema, err = isQueryNeedInfoSchema("GRANT CREATE ON db.* TO user")
	r.NoError(err)
	r.False(needSchema, "Uppercase GRANT should not need InfoSchema")
}
