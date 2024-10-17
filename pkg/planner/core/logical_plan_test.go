// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"testing"

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
	_ "github.com/secretflow/scql/pkg/types/parser_driver"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPlanSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testData testutil.TestData
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	mockTables, err := mock.MockAllTables()
	c.Assert(err, IsNil)
	s.is = infoschema.MockInfoSchema(mockTables)
	s.ctx = mock.MockContext()
	s.Parser = parser.New()

	s.testData, err = testutil.LoadTestSuiteData("testdata", "typical_query")
	c.Assert(err, IsNil)
}

func (s *testPlanSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPlanSuite) testPlanBuilder(c *C, ca string, output string) {
	comment := Commentf("for %s", ca)
	stmt, err := s.ParseOneStmt(ca, "", "")
	c.Assert(err, IsNil, comment)
	err = Preprocess(s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	p, _, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	c.Assert(ToString(p), Equals, output, Commentf("for %s", ca))
}

func (s *testPlanSuite) testPlanBuilderWithOptimization(c *C, ca string, output string) {
	comment := Commentf("for %s", ca)
	stmt, err := s.ParseOneStmt(ca, "", "")
	c.Assert(err, IsNil, comment)
	err = Preprocess(s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	p, _, err := BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	c.Assert(ToString(p), Equals, output, Commentf("for %s", ca))
}

func (s *testPlanSuite) TestPlanBuilderSimple(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderSelection(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderWindow(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderSubQuery(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderInOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderLowerUpperOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderCoalesceOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderLengthOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderReplaceOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuildeLikeRlikeOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderSubstringOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderTrimOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuildeLimitOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderInstrOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderGreatestLeastOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderLogicalOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderMathOp(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderCast(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilder(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderWithMissingDataSource(c *C) {
	ca := `select alice.tbl_0.long_0 < bob.tbl_0.long_0 from alice.tbl_0`
	comment := Commentf("for %s", ca)
	stmt, err := s.ParseOneStmt(ca, "", "")
	c.Assert(err, IsNil, comment)

	err = Preprocess(s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	_, _, err = BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err != nil, IsTrue)
}

func (s *testPlanSuite) TestPlanBuilderWithMissingWhereClause(c *C) {
	ca := `select 1`
	comment := Commentf("for %s", ca)
	stmt, err := s.ParseOneStmt(ca, "", "")
	c.Assert(err, IsNil, comment)

	err = Preprocess(s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	_, _, err = BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err != nil, IsTrue)
}

// name test after optimizing with TestxxxxWithOptimization
func (s *testPlanSuite) TestPlanBuilderSimpleWithOptimization(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilderWithOptimization(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderSelectionWithOptimization(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilderWithOptimization(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderWindowWithOptimization(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilderWithOptimization(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderSubQueryWithOptimization(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilderWithOptimization(c, ca, output[i])
	}
}

func (s *testPlanSuite) TestPlanBuilderDateTimeWithOptimization(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, ca := range input {
		s.testPlanBuilderWithOptimization(c, ca, output[i])
	}
}
