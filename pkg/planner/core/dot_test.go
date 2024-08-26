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
	"github.com/secretflow/scql/pkg/util/testutil"
)

var _ = Suite(&testDotSuite{})

type testDotSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testData testutil.TestData
}

func (s *testDotSuite) SetUpSuite(c *C) {
	mockTables, err := mock.MockAllTables()
	c.Assert(err, IsNil)
	s.is = infoschema.MockInfoSchema(mockTables)
	s.ctx = mock.MockContext()
	s.Parser = parser.New()
}

func (s *testDotSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testDotSuite) TestDrawLogicalPlan(c *C) {
	cases := map[string]string{
		`select plain_int_1, plain_int_2 from alice.tbl_0 where alice.tbl_0.plain_int_0 = 1`: `digraph G {
1 [label="DataScan(tbl_0)"]
2 [label="Sel([eq(alice.tbl_0.plain_int_0, 1)])"]
3 [label="Projection([alice.tbl_0.plain_int_1 alice.tbl_0.plain_int_2])"]
2 -> 1
3 -> 2
}`,

		`select count(distinct (alice.tbl_1.plain_float_0)) as co,sum(distinct (alice.tbl_1.plain_int_0)) as su from alice.tbl_1;`: `digraph G {
1 [label="DataScan(tbl_1)"]
2 [label="Agg([count(alice.tbl_1.plain_float_0) sum(alice.tbl_1.plain_int_0) firstrow(alice.tbl_1.aggregate_datetime_0) firstrow(alice.tbl_1.aggregate_datetime_1) firstrow(alice.tbl_1.aggregate_datetime_2) firstrow(alice.tbl_1.aggregate_float_0) firstrow(alice.tbl_1.aggregate_float_1) firstrow(alice.tbl_1.aggregate_float_2) firstrow(alice.tbl_1.aggregate_int_0) firstrow(alice.tbl_1.aggregate_int_1) firstrow(alice.tbl_1.aggregate_int_2) firstrow(alice.tbl_1.aggregate_string_0) firstrow(alice.tbl_1.aggregate_string_1) firstrow(alice.tbl_1.aggregate_string_2) firstrow(alice.tbl_1.aggregate_timestamp_0) firstrow(alice.tbl_1.aggregate_timestamp_1) firstrow(alice.tbl_1.aggregate_timestamp_2) firstrow(alice.tbl_1.compare_datetime_0) firstrow(alice.tbl_1.compare_datetime_1) firstrow(alice.tbl_1.compare_datetime_2) firstrow(alice.tbl_1.compare_float_0) firstrow(alice.tbl_1.compare_float_1) firstrow(alice.tbl_1.compare_float_2) firstrow(alice.tbl_1.compare_int_0) firstrow(alice.tbl_1.compare_int_1) firstrow(alice.tbl_1.compare_int_2) firstrow(alice.tbl_1.compare_string_0) firstrow(alice.tbl_1.compare_string_1) firstrow(alice.tbl_1.compare_string_2) firstrow(alice.tbl_1.compare_timestamp_0) firstrow(alice.tbl_1.compare_timestamp_1) firstrow(alice.tbl_1.compare_timestamp_2) firstrow(alice.tbl_1.encrypt_datetime_0) firstrow(alice.tbl_1.encrypt_datetime_1) firstrow(alice.tbl_1.encrypt_datetime_2) firstrow(alice.tbl_1.encrypt_float_0) firstrow(alice.tbl_1.encrypt_float_1) firstrow(alice.tbl_1.encrypt_float_2) firstrow(alice.tbl_1.encrypt_int_0) firstrow(alice.tbl_1.encrypt_int_1) firstrow(alice.tbl_1.encrypt_int_2) firstrow(alice.tbl_1.encrypt_string_0) firstrow(alice.tbl_1.encrypt_string_1) firstrow(alice.tbl_1.encrypt_string_2) firstrow(alice.tbl_1.encrypt_timestamp_0) firstrow(alice.tbl_1.encrypt_timestamp_1) firstrow(alice.tbl_1.encrypt_timestamp_2) firstrow(alice.tbl_1.groupby_datetime_0) firstrow(alice.tbl_1.groupby_datetime_1) firstrow(alice.tbl_1.groupby_datetime_2) firstrow(alice.tbl_1.groupby_float_0) firstrow(alice.tbl_1.groupby_float_1) firstrow(alice.tbl_1.groupby_float_2) firstrow(alice.tbl_1.groupby_int_0) firstrow(alice.tbl_1.groupby_int_1) firstrow(alice.tbl_1.groupby_int_2) firstrow(alice.tbl_1.groupby_string_0) firstrow(alice.tbl_1.groupby_string_1) firstrow(alice.tbl_1.groupby_string_2) firstrow(alice.tbl_1.groupby_timestamp_0) firstrow(alice.tbl_1.groupby_timestamp_1) firstrow(alice.tbl_1.groupby_timestamp_2) firstrow(alice.tbl_1.join_datetime_0) firstrow(alice.tbl_1.join_datetime_1) firstrow(alice.tbl_1.join_datetime_2) firstrow(alice.tbl_1.join_float_0) firstrow(alice.tbl_1.join_float_1) firstrow(alice.tbl_1.join_float_2) firstrow(alice.tbl_1.join_int_0) firstrow(alice.tbl_1.join_int_1) firstrow(alice.tbl_1.join_int_2) firstrow(alice.tbl_1.join_string_0) firstrow(alice.tbl_1.join_string_1) firstrow(alice.tbl_1.join_string_2) firstrow(alice.tbl_1.join_timestamp_0) firstrow(alice.tbl_1.join_timestamp_1) firstrow(alice.tbl_1.join_timestamp_2) firstrow(alice.tbl_1.joinpayload_datetime_0) firstrow(alice.tbl_1.joinpayload_datetime_1) firstrow(alice.tbl_1.joinpayload_datetime_2) firstrow(alice.tbl_1.joinpayload_float_0) firstrow(alice.tbl_1.joinpayload_float_1) firstrow(alice.tbl_1.joinpayload_float_2) firstrow(alice.tbl_1.joinpayload_int_0) firstrow(alice.tbl_1.joinpayload_int_1) firstrow(alice.tbl_1.joinpayload_int_2) firstrow(alice.tbl_1.joinpayload_string_0) firstrow(alice.tbl_1.joinpayload_string_1) firstrow(alice.tbl_1.joinpayload_string_2) firstrow(alice.tbl_1.joinpayload_timestamp_0) firstrow(alice.tbl_1.joinpayload_timestamp_1) firstrow(alice.tbl_1.joinpayload_timestamp_2) firstrow(alice.tbl_1.plain_datetime_0) firstrow(alice.tbl_1.plain_datetime_1) firstrow(alice.tbl_1.plain_datetime_2) firstrow(alice.tbl_1.plain_float_0) firstrow(alice.tbl_1.plain_float_1) firstrow(alice.tbl_1.plain_float_2) firstrow(alice.tbl_1.plain_int_0) firstrow(alice.tbl_1.plain_int_1) firstrow(alice.tbl_1.plain_int_2) firstrow(alice.tbl_1.plain_string_0) firstrow(alice.tbl_1.plain_string_1) firstrow(alice.tbl_1.plain_string_2) firstrow(alice.tbl_1.plain_timestamp_0) firstrow(alice.tbl_1.plain_timestamp_1) firstrow(alice.tbl_1.plain_timestamp_2)])"]
3 [label="Projection([Column#106 Column#107])"]
2 -> 1
3 -> 2
}`,
	}
	for ca, expected := range cases {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)

		err = Preprocess(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		p, _, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		lp, ok := p.(LogicalPlan)
		c.Assert(ok, IsTrue)
		dotStr := DrawLogicalPlan(lp)
		c.Assert(dotStr, Equals, expected, Commentf("for %s", ca))
	}
}
