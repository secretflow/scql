// Copyright 2018 PingCAP, Inc.
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

package driver

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/types"
)

var _ = Suite(&testValueExprRestoreSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testValueExprRestoreSuite struct {
}

func (s *testValueExprRestoreSuite) TestValueExprRestore(c *C) {
	testCases := []struct {
		datum  types.Datum
		expect string
	}{
		{types.NewDatum(nil), "NULL"},
		{types.NewIntDatum(1), "1"},
		{types.NewIntDatum(-1), "-1"},
		{types.NewUintDatum(1), "1"},
		{types.NewFloat32Datum(1.1), "1.1e+00"},
		{types.NewFloat64Datum(1.1), "1.1e+00"},
		{types.NewStringDatum("test `s't\"r."), "'test `s''t\"r.'"},
		{types.NewBytesDatum([]byte("test `s't\"r.")), "'test `s''t\"r.'"},
	}
	// Run Test
	var sb strings.Builder
	for _, testCase := range testCases {
		sb.Reset()
		expr := &ValueExpr{Datum: testCase.datum}
		err := expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil)
		c.Assert(sb.String(), Equals, testCase.expect, Commentf("Datum: %#v", testCase.datum))
	}
}
