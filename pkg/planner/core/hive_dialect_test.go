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

package core

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/util/mock"

	_ "github.com/secretflow/scql/pkg/types/parser_driver"
)

// TestHiveDialectConversion tests that SQL is properly converted to Hive dialect
func TestHiveDialectConversion(t *testing.T) {
	mockTables, err := mock.MockAllTables()
	require.NoError(t, err)
	is := infoschema.MockInfoSchema(mockTables)
	ctx := mock.MockContext()
	p := parser.New()
	dialect := NewHiveDialect()

	testCases := []struct {
		name        string
		inputSQL    string
		expectedSQL string
		skip        bool
	}{
		{
			name:        "Simple select",
			inputSQL:    "select plain_float_0 from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1",
		},
		{
			name:        "IFNULL to NVL conversion",
			inputSQL:    "select ifnull(plain_float_0, plain_float_1) from alice.tbl_1",
			expectedSQL: "select nvl(tbl_1.plain_float_0, tbl_1.plain_float_1) as expr_121 from alice.tbl_1",
		},
		{
			name:        "TRUNCATE to TRUNC conversion",
			inputSQL:    "select truncate(plain_float_0, 2) from alice.tbl_1",
			expectedSQL: "select trunc(tbl_1.plain_float_0, 2) as expr_121 from alice.tbl_1",
		},
		{
			name:        "NOW to CURRENT_TIMESTAMP conversion",
			inputSQL:    "select plain_datetime_0 < now() from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_datetime_0<current_timestamp() as expr_121 from alice.tbl_1",
		},
		{
			name:        "CURDATE to CURRENT_DATE conversion",
			inputSQL:    "select plain_datetime_0 < curdate() from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_datetime_0<current_date() as expr_121 from alice.tbl_1",
		},
		{
			name:        "CAST VARCHAR to STRING",
			inputSQL:    "select cast(plain_string_0 as CHAR(100)) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.plain_string_0 as string) as expr_121 from alice.tbl_0",
		},
		{
			name:        "CAST to DATETIME becomes TIMESTAMP",
			inputSQL:    "select cast(compare_string_0 as datetime) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_string_0 as timestamp) as expr_121 from alice.tbl_0",
		},
		{
			name:        "CAST to DOUBLE",
			inputSQL:    "select cast(compare_float_0 as double) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_float_0 as double) as expr_121 from alice.tbl_0",
		},
		{
			name:        "CAST to SIGNED becomes BIGINT",
			inputSQL:    "select cast(compare_float_0 as signed) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_float_0 as bigint) as expr_121 from alice.tbl_0",
		},
		{
			name:        "CAST to DECIMAL",
			inputSQL:    "select cast(compare_float_0 as decimal(11,2)) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_float_0 as decimal(11, 2)) as expr_121 from alice.tbl_0",
		},
		{
			name:        "Comparison operators need parentheses",
			inputSQL:    "select (plain_float_0 = plain_float_1) = 1 from alice.tbl_1",
			expectedSQL: "select (tbl_1.plain_float_0=tbl_1.plain_float_1)=1 as expr_121 from alice.tbl_1",
		},
		{
			name:        "GROUP BY with aggregate",
			inputSQL:    "select count(*) from alice.tbl_1 group by plain_float_0",
			expectedSQL: "select count(1) as expr_121 from alice.tbl_1 group by tbl_1.plain_float_0 having count(1)>=4",
		},
		{
			name:        "JOIN query",
			inputSQL:    "select tbl_1.plain_float_0 from alice.tbl_1 join alice.tbl_2 on tbl_1.plain_int_0 = tbl_2.plain_int_0",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 join alice.tbl_2 on tbl_1.plain_int_0=tbl_2.plain_int_0",
		},
		{
			name:        "DIV operator",
			inputSQL:    "select plain_float_0 div plain_float_1 from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_float_0 div tbl_1.plain_float_1 as expr_121 from alice.tbl_1",
		},
		{
			name:        "String functions",
			inputSQL:    "select lower(plain_string_0), upper(plain_string_0) from alice.tbl_0",
			expectedSQL: "select lower(tbl_0.plain_string_0) as expr_121,upper(tbl_0.plain_string_0) as expr_122 from alice.tbl_0",
		},
		{
			name:        "Arithmetic expressions",
			inputSQL:    "select plain_float_0 + plain_float_1 * 2 from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_float_0+tbl_1.plain_float_1*2 as expr_121 from alice.tbl_1",
		},
		{
			name:        "WHERE clause",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where plain_float_0 > 0 and plain_float_1 < 100",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where (tbl_1.plain_float_0>0) and (tbl_1.plain_float_1<100)",
		},
		{
			name:        "LIMIT and OFFSET",
			inputSQL:    "select plain_float_0 from alice.tbl_1 limit 10 offset 5",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 limit 10 offset 5",
		},
		// Extended function mappings tests (only using SCQL-supported functions)
		{
			name:        "SUBSTRING to SUBSTR",
			inputSQL:    "select substring(plain_string_0, 1, 5) from alice.tbl_0",
			expectedSQL: "select substr(tbl_0.plain_string_0, 1, 5) as expr_121 from alice.tbl_0",
		},
		{
			name:        "Math function CEIL",
			inputSQL:    "select ceil(plain_float_0) from alice.tbl_1",
			expectedSQL: "select ceil(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function FLOOR",
			inputSQL:    "select floor(plain_float_0) from alice.tbl_1",
			expectedSQL: "select floor(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function ROUND",
			inputSQL:    "select round(plain_float_0, 2) from alice.tbl_1",
			expectedSQL: "select round(tbl_1.plain_float_0, 2) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function ABS",
			inputSQL:    "select abs(plain_float_0) from alice.tbl_1",
			expectedSQL: "select abs(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "TRIM function",
			inputSQL:    "select trim(plain_string_0) from alice.tbl_0",
			expectedSQL: "select trim(tbl_0.plain_string_0) as expr_121 from alice.tbl_0",
		},
		{
			name:        "COALESCE function",
			inputSQL:    "select coalesce(plain_float_0, plain_float_1, 0) from alice.tbl_1",
			expectedSQL: "select coalesce(tbl_1.plain_float_0, tbl_1.plain_float_1, 0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function SQRT",
			inputSQL:    "select sqrt(plain_float_0) from alice.tbl_1",
			expectedSQL: "select sqrt(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function LN",
			inputSQL:    "select ln(plain_float_0) from alice.tbl_1",
			expectedSQL: "select ln(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function LOG10",
			inputSQL:    "select log10(plain_float_0) from alice.tbl_1",
			expectedSQL: "select log10(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function EXP",
			inputSQL:    "select exp(plain_float_0) from alice.tbl_1",
			expectedSQL: "select exp(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "Math function POW",
			inputSQL:    "select pow(plain_float_0, 2) from alice.tbl_1",
			expectedSQL: "select pow(tbl_1.plain_float_0, 2) as expr_121 from alice.tbl_1",
		},
		{
			name:        "LENGTH function",
			inputSQL:    "select length(plain_string_0) from alice.tbl_0",
			expectedSQL: "select length(tbl_0.plain_string_0) as expr_121 from alice.tbl_0",
		},
		{
			name:        "REPLACE function (Hive 3.x native replace)",
			inputSQL:    "select replace(plain_string_0, 'a', 'b') from alice.tbl_0",
			expectedSQL: "select replace(tbl_0.plain_string_0, 'a', 'b') as expr_121 from alice.tbl_0",
		},
		{
			name:        "INSTR function",
			inputSQL:    "select instr(plain_string_0, 'test') from alice.tbl_0",
			expectedSQL: "select instr(tbl_0.plain_string_0, 'test') as expr_121 from alice.tbl_0",
		},
		// ========== Extended SQL Pattern Tests ==========
		// CASE WHEN
		{
			name:        "CASE WHEN expression",
			inputSQL:    "select case when plain_float_0 > 100 then 1 else 0 end from alice.tbl_1",
			expectedSQL: "select case when (tbl_1.plain_float_0>100) then 1 else 0 end as expr_121 from alice.tbl_1",
		},
		{
			name:        "CASE WHEN with multiple branches",
			inputSQL:    "select case when plain_float_0 > 100 then 1 when plain_float_0 > 50 then 2 else 0 end from alice.tbl_1",
			expectedSQL: "select case when (tbl_1.plain_float_0>100) then 1 when (tbl_1.plain_float_0>50) then 2 else 0 end as expr_121 from alice.tbl_1",
		},
		// IS NULL / IS NOT NULL
		{
			name:        "IS NULL condition",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where plain_float_0 is null",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where tbl_1.plain_float_0 is null",
		},
		{
			name:        "IS NOT NULL condition",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where plain_float_0 is not null",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where not(tbl_1.plain_float_0 is null)",
		},
		// BETWEEN
		{
			name:        "BETWEEN clause",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where plain_float_0 between 10 and 100",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where (tbl_1.plain_float_0>=10) and (tbl_1.plain_float_0<=100)",
		},
		{
			name:        "ORDER BY clause",
			inputSQL:    "select plain_float_0 from alice.tbl_1 order by plain_float_0",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 order by tbl_1.plain_float_0",
		},
		{
			name:        "ORDER BY DESC",
			inputSQL:    "select plain_float_0 from alice.tbl_1 order by plain_float_0 desc",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 order by tbl_1.plain_float_0 desc",
		},
		// DISTINCT
		{
			name:        "DISTINCT select",
			inputSQL:    "select distinct plain_float_0 from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 group by tbl_1.plain_float_0",
		},
		// Multiple aggregates
		{
			name:        "SUM aggregate",
			inputSQL:    "select sum(plain_float_0) from alice.tbl_1",
			expectedSQL: "select sum(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "AVG aggregate",
			inputSQL:    "select avg(plain_float_0) from alice.tbl_1",
			expectedSQL: "select avg(tbl_1.plain_float_0) as expr_121 from alice.tbl_1",
		},
		{
			name:        "MIN and MAX aggregates",
			inputSQL:    "select min(plain_float_0), max(plain_float_1) from alice.tbl_1",
			expectedSQL: "select min(tbl_1.plain_float_0) as expr_121,max(tbl_1.plain_float_1) as expr_122 from alice.tbl_1",
		},
		// Additional function coverage (implemented but untested)
		{
			name:        "CONCAT function",
			inputSQL:    "select concat(plain_string_0, plain_string_1) from alice.tbl_0",
			expectedSQL: "select concat(tbl_0.plain_string_0, tbl_0.plain_string_1) as expr_121 from alice.tbl_0",
		},
		{
			name:        "COALESCE with two args",
			inputSQL:    "select coalesce(plain_float_0, 0) from alice.tbl_1",
			expectedSQL: "select coalesce(tbl_1.plain_float_0, 0) as expr_121 from alice.tbl_1",
		},
		// CAST types not yet tested
		{
			name:        "CAST to INT",
			inputSQL:    "select cast(compare_float_0 as signed) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_float_0 as bigint) as expr_121 from alice.tbl_0",
		},
		{
			name:        "CAST to FLOAT",
			inputSQL:    "select cast(compare_string_0 as double) from alice.tbl_0",
			expectedSQL: "select cast(tbl_0.compare_string_0 as double) as expr_121 from alice.tbl_0",
		},
		// Complex WHERE with AND/OR
		{
			name:        "Complex WHERE with OR",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where plain_float_0 > 0 or plain_float_1 < 100",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where (tbl_1.plain_float_0>0) or (tbl_1.plain_float_1<100)",
		},
		// NOT operator
		{
			name:        "NOT condition",
			inputSQL:    "select plain_float_0 from alice.tbl_1 where not plain_float_0 > 100",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 where not(tbl_1.plain_float_0>100)",
		},
		// Alias
		{
			name:        "Column alias",
			inputSQL:    "select plain_float_0 as val from alice.tbl_1",
			expectedSQL: "select tbl_1.plain_float_0 as val from alice.tbl_1",
		},
		// LEFT JOIN
		{
			name:        "LEFT JOIN query",
			inputSQL:    "select tbl_1.plain_float_0 from alice.tbl_1 left join alice.tbl_2 on tbl_1.plain_int_0 = tbl_2.plain_int_0",
			expectedSQL: "select tbl_1.plain_float_0 from alice.tbl_1 left join alice.tbl_2 on tbl_1.plain_int_0=tbl_2.plain_int_0",
		},
		// GROUP BY + HAVING
		{
			name:        "GROUP BY with HAVING",
			inputSQL:    "select plain_float_0, count(*) from alice.tbl_1 group by plain_float_0 having count(*) > 1",
			expectedSQL: "select tbl_1.plain_float_0,count(1) as expr_123 from alice.tbl_1 group by tbl_1.plain_float_0 having (count(1)>1) and (count(1)>=4)",
		},
		// Nested function call
		{
			name:        "Nested functions",
			inputSQL:    "select round(abs(plain_float_0), 2) from alice.tbl_1",
			expectedSQL: "select round(abs(tbl_1.plain_float_0), 2) as expr_121 from alice.tbl_1",
		},
		// IF function (SCQL supports IF, Hive also has IF)
		{
			name:        "IF function",
			inputSQL:    "select if(plain_float_0 > 0, 1, 0) from alice.tbl_1",
			expectedSQL: "select if(tbl_1.plain_float_0>0, 1, 0) as expr_121 from alice.tbl_1",
		},


	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Test case skipped")
				return
			}

			stmt, err := p.ParseOneStmt(tc.inputSQL, "", "")
			require.NoError(t, err, "Failed to parse SQL: %s", tc.inputSQL)

			err = Preprocess(ctx, stmt, is)
			require.NoError(t, err, "Failed to preprocess SQL: %s", tc.inputSQL)

			lp, _, err := BuildLogicalPlanWithOptimization(context.Background(), ctx, stmt, is)
			require.NoError(t, err, "Failed to build logical plan: %s", tc.inputSQL)

			sqlCtx, err := BuildChildCtx(dialect, lp)
			require.NoError(t, err, "Failed to build SQL context")

			newStmt, err := sqlCtx.GetSQLStmt()
			require.NoError(t, err, "Failed to get SQL statement")

			b := new(bytes.Buffer)
			err = newStmt.Restore(format.NewRestoreCtxWithDialect(
				format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase,
				b,
				dialect.GetFormatDialect(),
			))
			require.NoError(t, err, "Failed to restore SQL")

			actualSQL := b.String()
			assert.Equal(t, tc.expectedSQL, actualSQL, "SQL conversion mismatch for input: %s", tc.inputSQL)
		})
	}
}

// TestHiveDialectProperties tests the properties of HiveDialect
func TestHiveDialectProperties(t *testing.T) {
	dialect := NewHiveDialect()

	t.Run("SupportAnyValue should be false", func(t *testing.T) {
		assert.False(t, dialect.SupportAnyValue(), "Hive should not support ANY_VALUE")
	})

	t.Run("GetRestoreFlags", func(t *testing.T) {
		flags := dialect.GetRestoreFlags()
		assert.True(t, flags&format.RestoreStringSingleQuotes != 0, "Should use single quotes for strings")
		assert.True(t, flags&format.RestoreKeyWordLowercase != 0, "Should use lowercase keywords")
	})

	t.Run("GetFormatDialect should not be nil", func(t *testing.T) {
		assert.NotNil(t, dialect.GetFormatDialect())
	})
}

// TestDBTypeHive tests DBType constants and parsing
func TestDBTypeHive(t *testing.T) {
	t.Run("DBTypeHive constant", func(t *testing.T) {
		assert.Equal(t, "hive", DBTypeHive.String())
	})

	t.Run("ParseDBType for hive", func(t *testing.T) {
		dbType, err := ParseDBType("hive")
		require.NoError(t, err)
		assert.Equal(t, DBTypeHive, dbType)
	})

	t.Run("ParseDBType case insensitive", func(t *testing.T) {
		dbType, err := ParseDBType("HIVE")
		require.NoError(t, err)
		assert.Equal(t, DBTypeHive, dbType)
	})

	t.Run("DBDialectMap contains Hive", func(t *testing.T) {
		dialect, ok := DBDialectMap[DBTypeHive]
		assert.True(t, ok, "DBDialectMap should contain Hive")
		assert.NotNil(t, dialect)
	})
}
