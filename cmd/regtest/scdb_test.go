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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/server"
	"github.com/secretflow/scql/pkg/util/mock"
)

const (
	SkipCreate           = "SKIP_CREATE_USER_CCL"
	MySQLPort            = "MYSQL_PORT"
	SCDBPorts            = "SCDB_PORTS"
	SCDBHosts            = "SCDB_HOSTS"
	HttpProtocol         = "HTTP_PROTOCOL"
	MySQLConnStr         = "MYSQL_CONN_STR"
	SkipConcurrent       = "SKIP_CONCURRENT_TEST"
	SkipPlaintextCCLTest = "SKIP_PLAINTEXT_CCL_TEST"
	Deliminator          = ","
	Protocols            = "PROTOCOLS"
	ABY3                 = "ABY3"
	SEMI2K               = "SEMI2K"
	CHEETAH              = "CHEETAH"
)

const (
	concurrentNum = 5
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

type testFlag struct {
	sync           bool
	testConcurrent bool
	testSerial     bool
}

func getUrlList() ([]string, error) {
	portStr := os.Getenv(SCDBPorts)
	ports := strings.Split(portStr, Deliminator)
	hostStr := os.Getenv(SCDBHosts)
	httpProtocol := os.Getenv(HttpProtocol)
	var hostProtocol string
	if httpProtocol == "https" {
		hostProtocol = "https"
	} else {
		hostProtocol = "http"
	}
	var addr []string
	if hostStr == "" {
		for i := range ports {
			addr = append(addr, fmt.Sprintf("%s://localhost:%s", hostProtocol, strings.Trim(ports[i], " ")))
		}
	} else {
		hosts := strings.Split(hostStr, Deliminator)
		if len(ports) == 1 {
			for i := range hosts {
				if i == len(hosts)-1 {
					break
				}
				ports = append(ports, ports[i])
			}
		}
		if len(ports) != len(hosts) {
			return nil, fmt.Errorf("failed to get scdb address with host %s port %s", hostStr, portStr)
		}
		for i := range hosts {
			addr = append(addr, fmt.Sprintf("%s://%s:%s", hostProtocol, strings.Trim(hosts[i], " "), strings.Trim(ports[i], " ")))
		}
	}
	fmt.Printf("SCDB address: %+v\n", addr)
	return addr, nil
}

func getProtocols() ([]string, error) {
	ps := os.Getenv(Protocols)
	if ps == "" {
		return []string{SEMI2K}, nil
	}
	protocolList := strings.Split(ps, Deliminator)
	var trimProtocols []string
	for _, protocol := range protocolList {
		trimProtocols = append(trimProtocols, strings.Trim(protocol, " "))
	}
	fmt.Printf("test protocols: %+v\n", trimProtocols)
	return trimProtocols, nil
}

func TestMain(m *testing.M) {
	if os.Getenv(SCDBPorts) == "" {
		fmt.Println("Skipping testing due to empty SCDBPorts")
		return
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
	os.Exit(m.Run())
}

func TestSCDBWithNormalCCL(t *testing.T) {
	r := require.New(t)
	addresses, err := getUrlList()
	r.NoError(err)
	protocols, err := getProtocols()
	r.NoError(err)
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	fillTableToPartyCodeMap(mockTables)
	skipConcurrent := false
	if os.Getenv(SkipConcurrent) == "true" {
		skipConcurrent = true
	}
	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	for i, addr := range addresses {
		fmt.Printf("test protocol %s\n", protocols[i])
		r.NoError(createUserAndCcl(cclList, addresses[i], skipCreateFlag))
		if protocols[i] == SEMI2K {
			// We are temporarily using a separate file to save postgres test cases. After the complete support
			// of postgres functions, is it possible to remove this file?
			r.NoError(testCaseForSerial("testdata/single_party_postgres.json", true, addr, userNameCarol))
		}
		r.NoError(runQueryTest(userNameAlice, addr, protocols[i], testFlag{sync: true, testConcurrent: !skipConcurrent, testSerial: true}))
		r.NoError(runQueryTest(userNameAlice, addr, protocols[i], testFlag{sync: false, testConcurrent: !skipConcurrent, testSerial: false}))
	}
}

func TestSCDBWithAllCCLPlaintext(t *testing.T) {
	if os.Getenv(SkipPlaintextCCLTest) == "true" {
		fmt.Println("Skipping testing due to set SKIP_PLAINTEXT_CCL_TEST true")
		return
	}
	r := require.New(t)
	addresses, err := getUrlList()
	r.NoError(err)
	protocols, err := getProtocols()
	r.NoError(err)
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	fillTableToPartyCodeMap(mockTables)

	cclList, err := mock.MockAllCCLPlaintext()
	r.NoError(err)
	// only test concurrent case for all plaintext ccl
	for i, addr := range addresses {
		fmt.Printf("test protocol %s\n", protocols[i])
		r.NoError(createUserAndCcl(cclList, addresses[i], skipCreateFlag))
		r.NoError(runQueryTest(userNameAlice, addr, protocols[i], testFlag{sync: true, testConcurrent: true, testSerial: false}))
		r.NoError(runQueryTest(userNameAlice, addr, protocols[i], testFlag{sync: false, testConcurrent: true, testSerial: false}))
	}
}

func runQueryTest(user, addr string, protocol string, flags testFlag) (err error) {
	path := map[string][]string{SEMI2K: {"testdata/single_party.json", "testdata/two_parties.json", "testdata/multi_parties.json", "testdata/view.json"}, CHEETAH: {"testdata/two_parties.json"}, ABY3: {"testdata/multi_parties.json"}}
	for _, fileName := range path[protocol] {
		if flags.testSerial {
			if err := testCaseForSerial(fileName, flags.sync, addr, user); err != nil {
				return err
			}
		}
		if flags.testConcurrent {
			if err := testCaseForConcurrent(fileName, flags.sync, addr, user); err != nil {
				return err
			}
		}
	}
	return nil
}

func testCaseForSerial(dataPath string, sync bool, addr, issuerUser string) error {
	var suit queryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return nil
	}
	user := userMapCredential[issuerUser]
	for i, query := range suit.Queries {
		for _, viewSql := range query.View {
			if _, err := runSql(user, viewSql, addr, true); err != nil {
				return nil
			}
		}
		comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", query.Query, query.Name, dataPath)
		answer, err := runSql(user, query.Query, addr, sync)
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

func testCaseForConcurrent(dataPath string, sync bool, addr, issuerUser string) (err error) {
	var suit queryTestSuit
	err = createSuit(dataPath, &suit)
	if err != nil {
		return
	}

	inChan := make(chan int, concurrentNum)
	outChan := make(chan error)
	for i, query := range suit.Queries {
		go func(num int, testCase queryCase) {
			var err error
			inChan <- num
			defer func() {
				outChan <- err
				<-inChan
			}()
			for _, viewSql := range testCase.View {
				comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", viewSql, testCase.Name, dataPath)
				if _, err = runSql(userMapCredential[issuerUser], viewSql, addr, sync); err != nil {
					fmt.Println(comment)
					return
				}
			}
			comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", testCase.Query, testCase.Name, dataPath)
			answer, err := runSql(userMapCredential[issuerUser], testCase.Query, addr, sync)
			if err != nil {
				fmt.Println(comment)
				return
			}
			if err = checkResult(testCase.Result, answer, testCase.MySQLQuery, comment); err != nil {
				return
			}
		}(i, query)
	}
	for i := range suit.Queries {
		err := <-outChan
		if err != nil {
			return err
		}
		fmt.Printf("testfile: %s, sync: %v, issuerUser: %s, concurrent execution ", dataPath, sync, issuerUser)
		percent := int64(float32(i+1) / float32(len(suit.Queries)) * 100)
		fmt.Printf("%d%% (%v/%v)\r", percent, i+1, len(suit.Queries))
	}
	close(inChan)
	close(outChan)
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
			rc.Bools = t.GetBoolData()
		case scql.PrimitiveDataType_INT64, scql.PrimitiveDataType_TIMESTAMP:
			rc.Int64s = t.GetInt64Data()
		case scql.PrimitiveDataType_STRING, scql.PrimitiveDataType_DATETIME:
			rc.Ss = t.GetStringData()
		case scql.PrimitiveDataType_FLOAT32:
			dst := make([]float64, len(t.GetFloatData()))
			for i, v := range t.GetFloatData() {
				dst[i] = float64(v)
			}
			rc.Doubles = dst
		case scql.PrimitiveDataType_FLOAT64:
			rc.Doubles = t.GetDoubleData()
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
