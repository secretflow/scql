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

package regtest

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/server"
	"github.com/secretflow/scql/pkg/util/mock"
)

const (
	SkipCreate     = "SKIP_CREATE_USER_CCL"
	MySQLPort      = "MYSQL_PORT"
	SCDBPort       = "SCDB_PORT"
	SCDBHost       = "SCDB_HOST"
	MySQLConnStr   = "MYSQL_CONN_STR"
	SkipConcurrent = "SKIP_CONCURRENT_TEST"
)

const (
	concurrentNum = 10
)

var (
	testDataSource TestDataSource
	skipCreateFlag bool
)

type queryCase struct {
	Name       string       `json:"name"`
	View       []string     `json:"view"`
	Query      string       `json:"query"`
	MySQLQuery string       `json:"mysql_query"`
	Result     *ResultTable `json:"result"`
}

type queryTestSuit struct {
	Queries []queryCase
}

func TestMain(m *testing.M) {
	if os.Getenv(SCDBPort) == "" {
		fmt.Println("Skipping testing due to empty SCDBPort")
		return
	}
	if os.Getenv(SCDBHost) == "" {
		SCDBAddr = fmt.Sprintf("http://localhost:%s", os.Getenv(SCDBPort))
	} else {
		SCDBAddr = fmt.Sprintf("http://%s:%s", os.Getenv(SCDBHost), os.Getenv(SCDBPort))
	}

	skipCreateFlag = false
	if os.Getenv(SkipCreate) == "true" {
		skipCreateFlag = true
	}

	mysqlConnStr := os.Getenv(MySQLConnStr)
	if mysqlConnStr == "" {
		mysqlConnStr = fmt.Sprintf("root:testpass@tcp(localhost:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true", os.Getenv(MySQLPort), dbName)
	}

	mysqlConf := &server.StorageConf{
		ConnStr:         mysqlConnStr,
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 120,
		ConnMaxLifetime: 3000,
	}
	if err := testDataSource.ConnDB(mysqlConf); err != nil {
		fmt.Printf("connect mysql(%s) failed\n", mysqlConnStr)
		panic(err)
	}
	fmt.Printf("SCDBAddr %s\n", SCDBAddr)
	os.Exit(m.Run())
}

func TestSCDBWithNormalCCL(t *testing.T) {
	r := require.New(t)

	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	fillTableToPartyCodeMap(mockTables)

	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	r.NoError(createUserAndCcl(cclList, skipCreateFlag))

	r.NoError(runQueryTest(userNameAlice, true))
	r.NoError(runQueryTest(userNameAlice, false))
}

func TestSCDBWithAllCCLPlaintext(t *testing.T) {
	r := require.New(t)

	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	fillTableToPartyCodeMap(mockTables)

	cclList, err := mock.MockAllCCLPlaintext()
	r.NoError(err)
	r.NoError(createUserAndCcl(cclList, skipCreateFlag))

	r.NoError(runQueryTest(userNameAlice, true))
	r.NoError(runQueryTest(userNameAlice, false))
	r.NoError(testCaseForSerial("testdata/two_parties.json", true, userNameBob))

}

func runQueryTest(user string, sync bool) (err error) {
	path := []string{"testdata/single_party.json", "testdata/two_parties.json", "testdata/multi_parties.json", "testdata/view.json"}
	for _, fileName := range path {
		if err := testCaseForSerial(fileName, sync, user); err != nil {
			return err
		}
		if os.Getenv(SkipConcurrent) == "true" {
			continue
		}
		if err := testCaseForConcurrent(fileName, sync, user); err != nil {
			return err
		}
	}
	return nil
}

func testCaseForSerial(dataPath string, sync bool, issuerUser string) error {
	var suit queryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return nil
	}
	user := userMapCredential[issuerUser]
	for i, query := range suit.Queries {
		for _, viewSql := range query.View {
			if _, err := runSql(user, viewSql, true); err != nil {
				return nil
			}
		}
		comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", query.Query, query.Name, dataPath)
		answer, err := runSql(user, query.Query, sync)
		if err != nil {
			return fmt.Errorf("%s Error Info (%s)", comment, err)
		}
		if err = checkResult(query.Result, answer, query.MySQLQuery, comment); err != nil {
			return err
		}
		fmt.Printf("testfile: %s, sync: %v, issuerUser: %s, serial execution", dataPath, sync, issuerUser)
		percent := int64(float32(i+1) / float32(len(suit.Queries)) * 100)
		fmt.Printf(" %d%% (%v/%v)\r", percent, i+1, len(suit.Queries))
	}
	fmt.Println()
	return nil
}

func testCaseForConcurrent(dataPath string, sync bool, issuerUser string) (err error) {
	var suit queryTestSuit
	err = createSuit(dataPath, &suit)
	if err != nil {
		return
	}
	for i, query := range suit.Queries {
		inChan := make(chan int, concurrentNum)
		outChan := make(chan error)
		for j := 0; j < concurrentNum; j++ {
			go func(num int) {
				var err error
				inChan <- num
				defer func() {
					outChan <- err
					<-inChan
				}()
				for _, viewSql := range query.View {
					comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", viewSql, query.Name, dataPath)
					if _, err = runSql(userMapCredential[issuerUser], viewSql, sync); err != nil {
						fmt.Println(comment)
						return
					}
				}
				comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", query.Query, query.Name, dataPath)
				answer, err := runSql(userMapCredential[issuerUser], query.Query, sync)
				if err != nil {
					fmt.Println(comment)
					return
				}
				if err = checkResult(query.Result, answer, query.MySQLQuery, comment); err != nil {
					return
				}
			}(j)
		}
		for j := 0; j < concurrentNum; j++ {
			err := <-outChan
			if err != nil {
				return err
			}
		}
		close(inChan)
		close(outChan)
		fmt.Printf("testfile: %s, sync: %v, issuerUser: %s, concurrent execution ", dataPath, sync, issuerUser)
		percent := int64(float32(i+1) / float32(len(suit.Queries)) * 100)
		fmt.Printf("%d%% (%v/%v)\r", percent, i+1, len(suit.Queries))
	}
	fmt.Println()
	return nil
}

func checkResult(expected *ResultTable, answer []*scql.Tensor, mysqlQueryString, errInfo string) (err error) {
	if expected == nil {
		expected, err = testDataSource.GetQueryResultFromMySQL(mysqlQueryString)
		if err != nil {
			return fmt.Errorf("%s Error Info (%s)", errInfo, err)
		}
	}

	if expected == nil {
		return fmt.Errorf("%s Error Info (expected is nil)", errInfo)
	}
	if answer == nil {
		return fmt.Errorf("%s Error Info (answer is nil)", errInfo)
	}
	actualTable, err := convertTensorToResultTable(answer)
	if err != nil {
		return fmt.Errorf("%s Error Info (%s)", errInfo, err)
	}
	err = compareResultTableWithoutRowOrder(expected, actualTable)
	if err != nil {
		return fmt.Errorf("%s Error Info (%s)", errInfo, err)
	}
	return
}

func convertTensorToResultTable(ts []*scql.Tensor) (*ResultTable, error) {
	rt := &ResultTable{Column: make([]*ResultColumn, len(ts))}
	for i, t := range ts {
		rc := &ResultColumn{Name: t.GetName()}
		switch t.ElemType {
		case scql.PrimitiveDataType_BOOL:
			rc.Bools = t.GetBs().GetBs()
		case scql.PrimitiveDataType_INT32, scql.PrimitiveDataType_UINT32, scql.PrimitiveDataType_INT64, scql.PrimitiveDataType_UINT64, scql.PrimitiveDataType_TIMESTAMP:
			rc.Int64s = t.GetI64S().GetI64S()
		case scql.PrimitiveDataType_STRING, scql.PrimitiveDataType_DATETIME:
			rc.Ss = t.GetSs().GetSs()
		case scql.PrimitiveDataType_FLOAT, scql.PrimitiveDataType_DOUBLE:
			rc.Floats = t.GetFs().GetFs()
		default:
			return nil, fmt.Errorf("unsupported tensor type %v", t.ElemType)
		}
		rt.Column[i] = rc
	}
	return rt, nil
}

func compareResultTableWithoutRowOrder(expect, actual *ResultTable) error {
	if len(expect.Column) != len(actual.Column) {
		return fmt.Errorf("len(expect.Column) %v != len(actual.Column)) %v", len(expect.Column), len(actual.Column))
	}
	for i := range expect.Column {
		if expect.Column[i].Name != actual.Column[i].Name {
			return fmt.Errorf("i:%v th column name '%s' != '%s'", i, expect.Column[i].Name, actual.Column[i].Name)
		}
	}

	expectRows := expect.ConvertToRows()
	actualRows := actual.ConvertToRows()

	sort.Sort(expectRows)
	sort.Sort(actualRows)

	for _, col := range actual.Column {
		col.changeOrders(actualRows.getOrders())
	}
	for _, col := range expect.Column {
		col.changeOrders(expectRows.getOrders())
	}

	if !expect.EqualTo(actual) {
		return fmt.Errorf("expected:\n%v\nactual:\n%v\n", expectRows, actualRows)
	}
	return nil
}

func createSuit(dataPath string, suit *queryTestSuit) error {
	content, err := os.ReadFile(dataPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(content, &suit)
	if err != nil {
		return err
	}
	return nil
}
