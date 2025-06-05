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

package stringutil

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testStringUtilSuite{})

type testStringUtilSuite struct {
}

func (s *testStringUtilSuite) TestUnquote(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
		ok     bool
	}{
		{``, ``, false},
		{`'`, ``, false},
		{`'abc"`, ``, false},
		{`abcdea`, ``, false},
		{`'abc'def'`, ``, false},
		{`"abc\"`, ``, false},

		{`"abcdef"`, `abcdef`, true},
		{`"abc'def"`, `abc'def`, true},
		{`"\a汉字测试"`, `a汉字测试`, true},
		{`"☺"`, `☺`, true},
		{`"\xFF"`, `xFF`, true},
		{`"\U00010111"`, `U00010111`, true},
		{`"\U0001011111"`, `U0001011111`, true},
		{`"\a\b\f\n\r\t\v\\\""`, "a\bf\n\r\tv\\\"", true},
		{`"\Z\%\_"`, "\032" + `\%\_`, true},
		{`"abc\0"`, "abc\000", true},
		{`"abc\"abc"`, `abc"abc`, true},

		{`'abcdef'`, `abcdef`, true},
		{`'"'`, "\"", true},
		{`'\a\b\f\n\r\t\v\\\''`, "a\bf\n\r\tv\\'", true},
		{`' '`, ` `, true},
		{"'\\a汉字'", "a汉字", true},
		{"'\\a\x90'", "a\x90", true},
		{"\"\\a\x18èàø»\x05\"", "a\x18èàø»\x05", true},
	}

	for _, t := range table {
		x, err := Unquote(t.str)
		c.Assert(x, Equals, t.expect)
		comment := Commentf("source %v", t.str)
		if t.ok {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}

func (s *testStringUtilSuite) TestPatternMatch(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{``, `a`, '\\', false},
		{`a`, `a`, '\\', true},
		{`a`, `b`, '\\', false},
		{`aA`, `aA`, '\\', true},
		{`_`, `a`, '\\', true},
		{`_`, `ab`, '\\', false},
		{`__`, `b`, '\\', false},
		{`%`, `abcd`, '\\', true},
		{`%`, ``, '\\', true},
		{`%b`, `AAA`, '\\', false},
		{`%a%`, `BBB`, '\\', false},
		{`a%`, `BBB`, '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', true},
		{`%%_`, `abc`, '\\', true},
		{`%_%_aA`, "aaaA", '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`++_a`, `+xa`, '+', true},
		// We may reopen these test when like function go back to case insensitive.
		/*
			{"_ab", "AAB", '\\', true},
			{"%a%", "BAB", '\\', true},
			{"%a", "AAA", '\\', true},
			{"b%", "BBB", '\\', true},
		*/
	}
	for _, v := range tbl {
		patChars, patTypes := CompilePattern(v.pattern, v.escape)
		match := DoMatch(v.input, patChars, patTypes)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
}

func (s *testStringUtilSuite) TestCompileLike2Regexp(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern string
		regexp  string
	}{
		{``, ``},
		{`a`, `a`},
		{`aA`, `aA`},
		{`_`, `.`},
		{`__`, `..`},
		{`%`, `.*`},
		{`%b`, `.*b`},
		{`%a%`, `.*a.*`},
		{`a%`, `a.*`},
		{`\%a`, `%a`},
		{`\_a`, `_a`},
		{`\\_a`, `\.a`},
		{`\a\b`, `\a\b`},
		{`%%_`, `.*`},
		{`%_%_aA`, ".*aA"},
	}
	for _, v := range tbl {
		result := CompileLike2Regexp(v.pattern)
		c.Assert(result, Equals, v.regexp, Commentf("%v", v))
	}
}

func (s *testStringUtilSuite) TestIsExactMatch(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern    string
		escape     byte
		exactMatch bool
	}{
		{``, '\\', true},
		{`_`, '\\', false},
		{`%`, '\\', false},
		{`a`, '\\', true},
		{`a_`, '\\', false},
		{`a%`, '\\', false},
		{`a\_`, '\\', true},
		{`a\%`, '\\', true},
		{`a\\`, '\\', true},
		{`a\\_`, '\\', false},
		{`a+%`, '+', true},
		{`a\%`, '+', false},
		{`a++`, '+', true},
		{`a++_`, '+', false},
	}
	for _, v := range tbl {
		_, patTypes := CompilePattern(v.pattern, v.escape)
		c.Assert(IsExactMatch(patTypes), Equals, v.exactMatch, Commentf("%v", v))
	}
}

func BenchmarkMatchSpecial(b *testing.B) {
	var (
		pattern = `a%a%a%a%a%a%a%a%b`
		target  = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`
		escape  = byte('\\')
	)

	patChars, patTypes := CompilePattern(pattern, escape)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := DoMatch(target, patChars, patTypes)
		if match {
			b.Fatal("Unmatch expected.")
		}
	}
}

func TestRemoveSensitiveInfo(t *testing.T) {
	r := require.New(t)
	type sPair struct {
		originSql string
		expectSql string
	}
	testCase := []sPair{
		{`select plain_int_0 from scdb.alice_tbl_1;`, `select plain_int_0 from scdb.alice_tbl_1;`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED ***`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY     "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED ***`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED    BY "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED    ***`},
		{`CREATE USER alice PARTY_CODE "party_A"    IDENTIFIED    BY "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A"    IDENTIFIED    ***`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED WITH "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED ***`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED WITH    "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED ***`},
		{`CREATE USER alice PARTY_CODE "party_A" IDENTIFIED    WITH "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED    ***`},
		{`CREATE USER alice PARTY_CODE "party_A"   IDENTIFIED WITH "some_pwd"`, `CREATE USER alice PARTY_CODE "party_A"   IDENTIFIED ***`},
		{`CREATE USER alice PARTY_CODE "party_A"   IDENTIFIED WITH "     some_pwd"`, `CREATE USER alice PARTY_CODE "party_A"   IDENTIFIED ***`},
		{`ALTER USER alice IDENTIFIED BY "new_password"`, `ALTER USER alice IDENTIFIED ***`},
		{`ALTER USER alice IDENTIFIED WITH "new_password"`, `ALTER USER alice IDENTIFIED ***`},
	}
	for _, ca := range testCase {
		originSql := ca.originSql
		expected := ca.expectSql
		actual := RemoveSensitiveInfo(originSql)
		r.Equal(expected, actual)
	}
}

func TestIsDateString(t *testing.T) {
	r := require.New(t)
	type pair struct {
		in     string
		expect bool
	}
	testCases := []pair{
		{"2024-05-01", true},
		{"2024-5-1", false},
		{"2024-05-01 11:12:13", true},
		{"2024-05-01T11:12:13Z", false},
		{"05/01/2024", false},
		{"", false},
		{"2024-05-01 1:12:13", false},
	}
	for _, ca := range testCases {
		r.Equal(ca.expect, IsDateString(ca.in), ca.in)
	}
}

func TestStringToUnixMilli(t *testing.T) {
	r := require.New(t)
	type pair struct {
		in         string
		expectUnix int64
		expectErr  bool
	}
	testCases := []pair{
		{"2024-05-01", 1714521600000, false},
		{"2024-05-01 11:12:13", 1714561933000, false},
		{"2024-05-01T11:12:13Z", 0, true},
		{"", 0, true},
		{"2024-05-01 1:12:13", 0, true},
	}
	for _, ca := range testCases {
		unixMilli, err := StringToUnixMilli(ca.in)
		if ca.expectErr {
			r.Error(err, ca.in)
		} else {
			r.NoError(err, ca.in)
			r.Equal(ca.expectUnix, unixMilli, ca.in)
		}
	}
}

func TestStrptimeToGoLayout(t *testing.T) {
	r := require.New(t)

	type testCase struct {
		name           string
		strptimeFormat string
		expectedLayout string
		expectErr      bool
		expectedErrMsg string
	}

	testCases := []testCase{
		{
			name:           "YYYY-MM-DD",
			strptimeFormat: "%Y-%m-%d",
			expectedLayout: "2006-01-02",
			expectErr:      false,
		},
		{
			name:           "HH:MM:SS",
			strptimeFormat: "%H:%i:%S",
			expectedLayout: "15:04:05",
			expectErr:      false,
		},
		{
			name:           "YYYY-MM-DD HH:MM:SS",
			strptimeFormat: "%Y-%m-%d %H:%i:%S",
			expectedLayout: "2006-01-02 15:04:05",
			expectErr:      false,
		},
		{
			name:           "YYYY/MM/DD with T",
			strptimeFormat: "%Y/%m/%d %T",
			expectedLayout: "2006/01/02 15:04:05",
			expectErr:      false,
		},
		{
			name:           "MySQL full datetime with microseconds",
			strptimeFormat: "%Y-%m-%d %H:%i:%s.%f",
			expectedLayout: "2006-01-02 15:04:05.000000",
			expectErr:      false,
		},
		{
			name:           "Abbreviated month and day",
			strptimeFormat: "%b %d, %Y",
			expectedLayout: "Jan 02, 2006",
			expectErr:      false,
		},
		{
			name:           "Literal percent sign",
			strptimeFormat: "Rate: %%", // strptime "%%" means literal "%"
			expectedLayout: "Rate: %",
			expectErr:      false,
		},
		{
			name:           "Literal percent sign with other specifiers",
			strptimeFormat: "%%Y is %Y", // "%%Y" -> "%Y", "%Y" -> "2006"
			expectedLayout: "%Y is 2006",
			expectErr:      false,
		},
		{
			name:           "Empty format string",
			strptimeFormat: "",
			expectedLayout: "",
			expectErr:      false,
		},
		{
			name:           "Unsupported specifier",
			strptimeFormat: "%Y-%m-%q", // %q is not in our map
			expectedLayout: "",
			expectErr:      true,
			expectedErrMsg: "unsupported strptime specifier: %q",
		},
		{
			name:           "Dangling percent",
			strptimeFormat: "%Y-%m-%",
			expectedLayout: "",
			expectErr:      true,
			expectedErrMsg: "invalid format string: trailing %",
		},
		{
			name:           "No specifiers, just literals",
			strptimeFormat: "Year-Month-Day",
			expectedLayout: "Year-Month-Day",
			expectErr:      false,
		},
		{
			name:           "12-hour format with AM/PM",
			strptimeFormat: "%h:%i:%S %p",
			expectedLayout: "03:04:05 PM",
			expectErr:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goLayout, err := StrptimeToGoLayout(tc.strptimeFormat)
			if tc.expectErr {
				r.Error(err, "strptimeFormat: %s", tc.strptimeFormat)
				if tc.expectedErrMsg != "" {
					r.Contains(err.Error(), tc.expectedErrMsg, "Error message mismatch for %s", tc.strptimeFormat)
				}
			} else {
				r.NoError(err, "strptimeFormat: %s", tc.strptimeFormat)
				r.Equal(tc.expectedLayout, goLayout, "Layout mismatch for strptimeFormat: %s", tc.strptimeFormat)
			}
		})
	}
}
