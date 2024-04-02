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
	"fmt"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/mock"
)

func (s *testTranslatorSuite) TestTranslateWithCCL(c *C) {
	ccl, err := mock.MockAllCCL()
	c.Assert(err, IsNil)
	for _, ca := range translateWithCCLTestCases {
		sql := ca.sql
		dot := ca.dotGraph
		conf := ca.conf
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)

		err = core.Preprocess(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		lp, _, err := core.BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		groupThreshold := 4
		if conf.groupThreshold > 0 {
			groupThreshold = conf.groupThreshold
		}
		compileOpts := scql.CompileOptions{
			SecurityCompromise: &scql.SecurityCompromiseConfig{RevealGroupMark: false, GroupByThreshold: uint64(groupThreshold)},
		}
		t, err := NewTranslator(
			s.engineInfo, &scql.SecurityConfig{ColumnControlList: ccl},
			s.issuerParty, &compileOpts)
		c.Assert(err, IsNil)
		ep, err := t.Translate(lp)
		c.Assert(err, IsNil, Commentf("for %s", sql))
		// if you want to copy the graph created by DumpGraphviz, uncomment this line
		c.Log(ep.DumpGraphviz())
		if recordTestOutput {
			_, err := s.recordFile.WriteString(fmt.Sprintf("{`%s`, `%s`},\n", sql, ep.DumpGraphviz()))
			c.Assert(err, IsNil)
		} else {
			c.Assert(ep.DumpGraphviz(), Equals, dot, Commentf("for %s", sql))
		}
	}
}

func (s *testTranslatorSuite) TestBuildCCL(c *C) {
	ccl, err := mock.MockAllCCL()
	c.Assert(err, IsNil)
	for _, sql := range buildCCLTestCases {
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)

		err = core.Preprocess(s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)

		lp, _, err := core.BuildLogicalPlanWithOptimization(context.Background(), s.ctx, stmt, s.is)

		c.Assert(err, IsNil)
		compileOpts := scql.CompileOptions{
			SecurityCompromise: &scql.SecurityCompromiseConfig{RevealGroupMark: false, GroupByThreshold: 4},
		}
		t, err := NewTranslator(
			s.engineInfo, &scql.SecurityConfig{ColumnControlList: ccl},
			s.issuerParty, &compileOpts)
		c.Assert(err, IsNil)
		builder, err := newLogicalNodeBuilder(t.issuerPartyCode, t.enginesInfo, convertOriginalCCL(t.sc), 4)
		if err != nil {
			c.Assert(err, IsNil)
		}
		ln, err := builder.buildLogicalNode(lp)
		if err != nil {
			c.Assert(err, IsNil)
		}
		// Check if the result is visible to the issuerPartyCode
		for _, col := range ln.Schema().Columns {
			cc := ln.CCL()[col.UniqueID]
			c.Assert(cc.IsVisibleFor(t.issuerPartyCode), IsTrue, Commentf("for %s", sql))
		}
	}
}

var buildCCLTestCases = []string{
	"select plain_string_0 + plain_string_0 from alice.tbl_0 group by plain_string_0",
	"select concat(plain_string_0, plain_string_1) as tt from alice.tbl_0",
	"select count(*), max(aggregate_int_0) from (select aggregate_int_0, groupby_int_0 from alice.tbl_0 union all select aggregate_int_0, groupby_int_0 from bob.tbl_1 union all select aggregate_int_0, groupby_int_0 from carol.tbl_2) as u",
	"select count(*), max(aggregate_int_0) from (select aggregate_int_0, groupby_int_0 from alice.tbl_0 union all select aggregate_int_0, groupby_int_0 from bob.tbl_1 union all select aggregate_int_0, groupby_int_0 from carol.tbl_2) as u group by groupby_int_0",
	"select gb_gb_gb_int_0 from alice.tbl_3 group by gb_gb_gb_int_0",
}
