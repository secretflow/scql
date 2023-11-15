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

package scdb_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/cmd/regtest"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/client"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/sqlbuilder"
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
	concurrentNum      = 5
	stubTimeoutMinutes = 5
)

var (
	testDataSource regtest.TestDataSource
	skipCreateFlag bool
)

// TODO: refactor regtest configuration with YAML?
var (
	alicePrivateKeyPemPath = flag.String("alicePem", "", "path to alice private key pem")
	bobPrivateKeyPemPath   = flag.String("bobPem", "", "path to bob private key pem")
	carolPrivateKeyPemPath = flag.String("carolPem", "", "path to carol private key pem")
)

var (
	aliceEngAddr = flag.String("aliceEngAddr", "engine-alice:8003", "endpoint of alice engine")
	bobEngAddr   = flag.String("bobEngAddr", "engine-bob:8003", "endpoint of bob engine")
	carolEngAddr = flag.String("carolEngAddr", "engine-carol:8003", "endpoint of carol engine")
)

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

	mysqlConf := &config.StorageConf{
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
	regtest.FillTableToPartyCodeMap(mockTables)
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
			r.NoError(testCaseForSerial("../testdata/single_party_postgres.json", true, addr, userNameCarol))
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
	regtest.FillTableToPartyCodeMap(mockTables)

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
	path := map[string][]string{SEMI2K: {"../testdata/single_party.json", "../testdata/two_parties.json", "../testdata/multi_parties.json", "../testdata/view.json"}, CHEETAH: {"../testdata/two_parties.json"}, ABY3: {"../testdata/multi_parties.json"}}
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
	var suit regtest.QueryTestSuit
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
		if err = regtest.CheckResult(testDataSource, query.Result, answer, query.MySQLQuery, comment); err != nil {
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
	var suit regtest.QueryTestSuit
	err = createSuit(dataPath, &suit)
	if err != nil {
		return
	}

	inChan := make(chan int, concurrentNum)
	outChan := make(chan error)
	for i, query := range suit.Queries {
		go func(num int, testCase regtest.QueryCase) {
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
			if err = regtest.CheckResult(testDataSource, testCase.Result, answer, testCase.MySQLQuery, comment); err != nil {
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

func createSuit(dataPath string, suit *regtest.QueryTestSuit) error {
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

func createUserAndCcl(cclList []*scql.SecurityConfig_ColumnControl, addr string, skipCreate bool) error {
	if skipCreate {
		fmt.Println("skip func createUserAndCcl")
		return nil
	}

	// TODO: refactor these userMapXxx

	userMapPrivateKeyPath := map[string]string{
		"alice": *alicePrivateKeyPemPath,
		"bob":   *bobPrivateKeyPemPath,
		"carol": *carolPrivateKeyPemPath,
	}

	userMapEngEndpoint := map[string]string{
		"alice": *aliceEngAddr,
		"bob":   *bobEngAddr,
		"carol": *carolEngAddr,
	}

	// create user
	for _, user := range userNames {
		builder := sqlbuilder.NewCreateUserStmtBuilder()
		sql, err := builder.IfNotExists().SetUser(user).SetPassword(userMapPassword[user]).SetParty(userMapPartyCode[user]).AuthByPubkeyWithPemFile(userMapPrivateKeyPath[user]).WithEndpoinits([]string{userMapEngEndpoint[user]}).ToSQL()
		if err != nil {
			return err
		}
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}

	// create database
	{
		sql := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, dbName)
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}
	// grant base auth
	for _, user := range userNames {
		sql := fmt.Sprintf(`GRANT CREATE, CREATE VIEW, GRANT OPTION ON %s.* TO %s`, dbName, user)
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}

	// grant ccl
	tableExistMap := map[string]bool{}
	// map db_table to party to ccl to columns
	grantMap := map[string]map[string]map[string][]string{}
	physicalTableMetas, err := mock.MockPhysicalTableMetas()
	if err != nil {
		return err
	}
	for _, ccl := range cclList {
		findTable := false
		dbTableName := fmt.Sprintf(`%s_%s`, ccl.DatabaseName, ccl.TableName)
		if _, exist := tableExistMap[dbTableName]; exist {
			findTable = true
		}
		tableOwner := partyCodeToUser[regtest.TableToPartyCode[dbTableName]]
		if !findTable {
			pt, ok := physicalTableMetas[dbTableName]
			if !ok {
				return fmt.Errorf("%s not found in physicalTableMetas", dbTableName)
			}
			sql := pt.ToCreateTableStmt(fmt.Sprintf("%s.%s", dbName, dbTableName), true)
			fmt.Println(sql)
			if _, err := runSql(userMapCredential[tableOwner], sql, addr, true); err != nil {
				return err
			}
			tableExistMap[dbTableName] = true
		}
		grantTo := fmt.Sprintf("%s.%s", dbName, dbTableName)
		cclStr := scql.SecurityConfig_ColumnControl_Visibility_name[int32(ccl.Visibility)]
		if grantMap[grantTo] == nil {
			grantMap[grantTo] = map[string]map[string][]string{}
		}
		if grantMap[grantTo][partyCodeToUser[ccl.PartyCode]] == nil {
			grantMap[grantTo][partyCodeToUser[ccl.PartyCode]] = map[string][]string{}
		}
		if len(grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr]) == 0 {
			grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr] = []string{}
		}
		grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr] = append(grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr], ccl.ColumnName)
	}
	fmt.Println("Grant CCL...")
	for dbTable, userGrantMap := range grantMap {
		for user, cclGrantMap := range userGrantMap {
			var cclColumns []string
			for cc, columns := range cclGrantMap {
				cclColumns = append(cclColumns, fmt.Sprintf("SELECT %s(%s)", cc, strings.Join(columns, ", ")))
			}
			sql := fmt.Sprintf("GRANT %s ON %s TO %s", strings.Join(cclColumns, ", "), dbTable, user)
			tableOwner := partyCodeToUser[regtest.TableToPartyCode[strings.Split(dbTable, ".")[1]]]
			if _, err := runSql(userMapCredential[tableOwner], sql, addr, true); err != nil {
				return fmt.Errorf("query failed: %v, with error:%v", sql, err)
			}
		}
	}
	fmt.Println("Grant Complete!")
	fmt.Println()
	return nil
}

func runSql(user *scql.SCDBCredential, sql, addr string, sync bool) ([]*scql.Tensor, error) {
	httpClient := &http.Client{Timeout: stubTimeoutMinutes * time.Minute}
	stub := client.NewDefaultClient(addr, httpClient)
	if sync {
		resp, err := stub.SubmitAndGet(user, sql)
		if err != nil {
			return nil, err
		}
		if int32(scql.Code_OK) != resp.GetStatus().GetCode() {
			return nil, fmt.Errorf("error code %d, %+v", resp.Status.Code, resp)
		}
		return resp.OutColumns, nil
	} else {
		resp, err := stub.Submit(user, sql)
		if err != nil {
			return nil, err
		}
		if resp.Status.Code != int32(scql.Code_OK) {
			return nil, fmt.Errorf("error code expected %d, actual %d", int32(scql.Code_OK), resp.Status.Code)
		}

		if resp.ScdbSessionId == "" {
			return nil, fmt.Errorf("errorCode: %v, msg: %v", resp.Status.Code, resp.Status.Message)
		}
		fetchResp, err := stub.Fetch(user, resp.ScdbSessionId)
		if err != nil {
			return nil, err
		}

		if int32(scql.Code_OK) != fetchResp.Status.Code {
			return nil, fmt.Errorf("error code %d, %+v", fetchResp.Status.Code, fetchResp)
		}
		return fetchResp.OutColumns, nil
	}
}
