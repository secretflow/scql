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
	"context"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/testleak"
	"github.com/secretflow/scql/pkg/util/testutil"

	// NOTE(yang.y): import parser_driver so that ast.NewValueExpr
	// https://github.com/pingcap/parser/blob/a9496438d77d525d8759e0103f6eca40ce1d2788/ast/expressions.go#L66
	// is initialized.
	log "github.com/sirupsen/logrus"

	_ "github.com/secretflow/scql/pkg/types/parser_driver"
)

var _ = Suite(&testRunSQLSuite{})

type testRunSQLSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testData testutil.TestData
}

type TestCaseSqlString struct {
	Sql            string
	SkipProjection bool
	RewrittenSql   string
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

func (s *testRunSQLSuite) testRunSQL(c *C, testCase TestCaseSqlString, useV2 bool) {
	sql, err := regenerateSql(testCase, s)
	comment := Commentf("for %+v", testCase)
	log.Info(sql)
	c.Assert(err, IsNil, comment)
	c.Assert(sql == testCase.RewrittenSql, IsTrue, comment)
}

func regenerateSql(testCase TestCaseSqlString, s *testRunSQLSuite) (string, error) {
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
	log.Info(ToString(lp))
	sql := ""
	if testCase.SkipProjection {
		switch p := lp.(type) {
		case *LogicalProjection:
			sql, _, err = ToSqlString(p.children[0])
			return sql, err
		}
	}
	sql, _, err = ToSqlString(lp)
	return sql, err
}
