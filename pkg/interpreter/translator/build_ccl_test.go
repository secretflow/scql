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

package translator

import (
	"context"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/testleak"
	"github.com/secretflow/scql/pkg/util/testutil"
)

var _ = Suite(&testCCLSuite{})

type testCCLSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	engineInfo *graph.EnginesInfo

	issuerParty string
	testData    testutil.TestData
}

type TestCaseCCLString struct {
	Sql        string `json:"sql"`
	NodeString string `json:"node_string"`
	PrintDSCCL bool   `json:"print_ds"`
}

func (s *testCCLSuite) SetUpSuite(c *C) {
	mockTables, err := mock.MockAllTables()
	c.Assert(err, IsNil)
	s.is = infoschema.MockInfoSchema(mockTables)
	s.ctx = mock.MockContext()
	s.Parser = parser.New()
	c.Assert(err, IsNil)

	testutil.SkipOutJson = true

	s.testData, err = testutil.LoadTestSuiteData("testdata", "ccl_test")
	c.Assert(err, IsNil)

	mockEngines, err := mock.MockEngines()
	c.Assert(err, IsNil)
	s.engineInfo, err = ConvertMockEnginesToEnginesInfo(mockEngines)
	c.Assert(err, IsNil)

	s.issuerParty = "alice"
}

func (s *testCCLSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testCCLSuite) TestCheckCCL(c *C) {
	defer testleak.AfterTest(c)()
	var input []TestCaseCCLString
	s.testData.GetTestCasesWithoutOut(c, &input)
	for _, ca := range input {
		s.testCheckCCL(c, ca)
	}
}

func (s *testCCLSuite) testCheckCCL(c *C, testCase TestCaseCCLString) {
	sql := testCase.Sql
	expected := testCase.NodeString
	comment := Commentf("for %s", sql)
	ccl, err := mock.MockAllCCL()
	c.Assert(err, IsNil, comment)
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil, comment)

	err = core.Preprocess(s.ctx, stmt, s.is)
	c.Assert(err, IsNil)

	lp, _, err := core.BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err, IsNil, comment)
	compileOpts := scql.CompileOptions{
		SecurityCompromise: &scql.SecurityCompromiseConfig{RevealGroupMark: false, GroupByThreshold: 4},
	}
	t, err := NewTranslator(
		s.engineInfo, &scql.SecurityConfig{ColumnControlList: ccl},
		s.issuerParty, &compileOpts)
	c.Assert(err, IsNil, comment)
	// preprocessing lp
	processor := LpPrePocessor{}
	err = processor.process(lp)
	c.Assert(err, IsNil)
	builder, err := newLogicalNodeBuilder(t.issuerPartyCode, t.enginesInfo, convertOriginalCCL(t.sc), 4)
	c.Assert(err, IsNil)
	ln, err := builder.buildLogicalNode(lp)
	c.Assert(err, IsNil, comment)
	// c.Log(ToString(ln, !testCase.PrintDSCCL))
	c.Assert(ToString(ln, !testCase.PrintDSCCL) == expected, IsTrue, comment)
}
