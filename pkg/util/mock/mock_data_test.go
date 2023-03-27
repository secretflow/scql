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

package mock

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/testleak"
)

var _ = Suite(&testMockSuite{})

type TestSchema struct {
	DbName    string     `json:"db_name"`
	TableName string     `json:"table_name"`
	Columns   [][]string `json:"columns"`
}

type MockData struct {
	MockTableSchema TestSchema `json:"mock_table_schema"`
	MockCcl         [][]string `json:"mock_ccl"`
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testMockSuite struct {
	output MockData
}

func (s *testMockSuite) SetUpSuite(c *C) {
	mockDataPath = []string{"testdata/test_mock_in.json"}
	byteValue, err := getByteArrayFromJson("testdata/test_mock_out.json")
	c.Assert(err, IsNil)
	err = json.Unmarshal(byteValue, &s.output)
	c.Assert(err, IsNil)
}

func (s *testMockSuite) TestMockData(c *C) {
	defer testleak.AfterTest(c)()
	// test mock table
	table, err := MockAllTables()
	c.Assert(err, IsNil)
	singleTable := table["test"][0]
	c.Assert(singleTable.Name.O == s.output.MockTableSchema.TableName, IsTrue)

	actualColumnNames := extractNamesFromColumns(singleTable.Columns)
	expectedColumnNames := selectColumn(s.output.MockTableSchema.Columns, 0)
	c.Assert(reflect.DeepEqual(actualColumnNames, expectedColumnNames), IsTrue)

	actualColumnTypes := extractTypesFromColumns(singleTable.Columns)
	expectedColumnTypes := selectColumn(s.output.MockTableSchema.Columns, 1)
	c.Assert(reflect.DeepEqual(actualColumnTypes, expectedColumnTypes), IsTrue)

	// test mock ccl
	cclInfos, err := MockAllCCL()
	c.Assert(err, IsNil)
	c.Assert(len(cclInfos) == len(s.output.MockCcl), IsTrue)

	actualCcls := convertCCLsToStrings(cclInfos)
	var expectedCcls []string
	for _, cc := range s.output.MockCcl {
		expectedCcls = append(expectedCcls, fmt.Sprintf("%s-%s-%s", cc[0], cc[1], cc[2]))
	}
	sort.Strings(actualCcls)
	sort.Strings(expectedCcls)
	c.Assert(reflect.DeepEqual(actualCcls, expectedCcls), IsTrue)
}

func extractNamesFromColumns(columns []*model.ColumnInfo) []string {
	var res []string
	for _, col := range columns {
		res = append(res, col.Name.O)
	}
	return res
}

func extractTypesFromColumns(columns []*model.ColumnInfo) []string {
	var res []string
	for _, col := range columns {
		res = append(res, col.FieldType.String())
	}
	return res
}

func selectColumn(columns [][]string, rowNumber int) []string {
	var res []string
	for _, col := range columns {
		res = append(res, col[rowNumber])
	}
	return res
}

func convertCCLsToStrings(ccls []*scql.SecurityConfig_ColumnControl) []string {
	var res []string
	for _, ccl := range ccls {
		res = append(res, fmt.Sprintf("%s.%s.%s-%s-%s", ccl.DatabaseName, ccl.TableName, ccl.ColumnName, ccl.PartyCode, ccl.Visibility.String()))
	}
	return res
}
