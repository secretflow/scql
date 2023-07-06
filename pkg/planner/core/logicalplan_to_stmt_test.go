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

	. "github.com/pingcap/check"
	"github.com/pingcap/log"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/proto-gen/grm"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/testleak"
	"github.com/secretflow/scql/pkg/util/testutil"

	// NOTE(yang.y): import parser_driver so that ast.NewValueExpr
	// https://github.com/pingcap/parser/blob/a9496438d77d525d8759e0103f6eca40ce1d2788/ast/expressions.go#L66
	// is initialized.

	_ "github.com/secretflow/scql/pkg/types/parser_driver"
)

var _ = Suite(&testRunSQLSuite{})
var testBackEnds = []string{MySQL, Postgres}

const (
	MySQL    = "MYSQL"
	Postgres = "POSTGRESQL"
)

type testRunSQLSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testData testutil.TestData
}

type TestCaseSqlString struct {
	Sql               string `json:"sql"`
	SkipProjection    bool   `json:"skip_projection"`
	RewrittenSqlMysql string `json:"rewritten_sql_mysql"`
	RewrittenSqlPg    string `json:"rewritten_sql_pg"`
	SkipPgTest        bool   `json:"skip_pg_test"`
	// default; if RewrittenSql set, all back ends use this sql as default
	RewrittenSql string `json:"rewritten_sql"`
}

func (s *testRunSQLSuite) SetUpSuite(c *C) {
	mockTables, err := mock.MockAllTables()
	c.Assert(err, IsNil)
	s.is = infoschema.MockInfoSchema(mockTables)
	s.ctx = mock.MockContext()
	s.Parser = parser.New()
	c.Assert(err, IsNil)

	testutil.SkipOutJson = true

	s.testData, err = testutil.LoadTestSuiteData("testdata", "runsql")
	c.Assert(err, IsNil)
}

func (s *testRunSQLSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testRunSQLSuite) TestRunSQL(c *C) {
	defer testleak.AfterTest(c)()
	var input []TestCaseSqlString
	s.testData.GetTestCasesWithoutOut(c, &input)
	for _, ca := range input {
		s.testRunSQL(c, ca, true)
	}
}

func GetExpectSQL(backEnd string, testCase TestCaseSqlString) string {
	if backEnd == MySQL {
		if testCase.RewrittenSqlMysql != "" {
			return testCase.RewrittenSqlMysql
		}
	}
	if backEnd == Postgres {
		if testCase.RewrittenSqlPg != "" {
			return testCase.RewrittenSqlPg
		}
	}
	return testCase.RewrittenSql
}

func SkipTestFor(backEnd string, testCase TestCaseSqlString) bool {
	switch backEnd {
	case MySQL:
		return true
	case Postgres:
		return testCase.SkipPgTest
	}
	return false
}

func (s *testRunSQLSuite) testRunSQL(c *C, testCase TestCaseSqlString, useV2 bool) {
	for _, backEnd := range testBackEnds {
		expect := GetExpectSQL(backEnd, testCase)
		// test mysql
		sql, err := regenerateSql(testCase, s, DBDialectMap[grm.DataSourceKind(grm.DataSourceKind_value[backEnd])])
		comment := Commentf("%s tests: for %+v", backEnd, testCase)
		log.Info(sql)
		c.Assert(err, IsNil, comment)
		c.Assert(expect == sql, IsTrue, comment)
	}
}

func regenerateSql(testCase TestCaseSqlString, s *testRunSQLSuite, dialect Dialect) (string, error) {
	stmt, err := s.ParseOneStmt(testCase.Sql, "", "")
	if err != nil {
		return "", err
	}

	err = Preprocess(s.ctx, stmt, s.is)
	if err != nil {
		return "", err
	}

	lp, _, err := BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
	if err != nil {
		return "", err
	}
	// log.Info(ToString(lp))
	var sqlCtx *runSqlCtx
	if testCase.SkipProjection {
		switch p := lp.(type) {
		case *LogicalProjection:
			sqlCtx, err = BuildChildCtx(dialect, p.Children()[0])
		default:
			sqlCtx, err = BuildChildCtx(dialect, p)
		}
	} else {
		sqlCtx, err = BuildChildCtx(dialect, lp)
	}
	if err != nil {
		return "", err
	}

	newStmt, err := sqlCtx.GetSQLStmt()
	if err != nil {
		return "", err
	}
	// StripFieldAsName(newStmt)
	b := new(bytes.Buffer)
	if err := newStmt.Restore(format.NewRestoreCtxWithDialect(dialect.GetRestoreFlags(), b, dialect.GetFormatDialect())); err != nil {
		return "", err
	}
	return b.String(), err
}
