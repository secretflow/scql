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

package p2p_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/secretflow/scql/cmd/regtest"
	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/util/brokerutil"
	"github.com/secretflow/scql/pkg/util/mock"
)

const (
	Deliminator = ","
)

const (
	concurrentNum      = 5
	defaultProjectName = "scdb"
	// timeout send request to intra broker
	timeoutS        = 60
	fetchIntervalMs = 100
	// party code
	alice = "alice"
	bob   = "bob"
	carol = "carol"
)

var (
	testDataSource regtest.TestDataSource

	// addresses
	aliceStub *brokerutil.Command
	bobStub   *brokerutil.Command
	carolStub *brokerutil.Command
	stubMap   map[string]*brokerutil.Command

	// test tables
	tables = []string{"members", "column_privs", "columns", "tables", "invitations", "projects"}
	dbs    = []string{"brokera", "brokerb", "brokerc"}
)

type testConfig struct {
	SkipCreateTableCCL   bool   `yaml:"skip_create_table_ccl"`
	SkipConcurrentTest   bool   `yaml:"skip_concurrent_test"`
	SkipPlaintextCCLTest bool   `yaml:"skip_plaintext_ccl_test"`
	ProjectConf          string `yaml:"project_conf"`
	HttpProtocol         string `yaml:"http_protocol"`
	BrokerPorts          string `yaml:"broker_ports"`
	MySQLConnStr         string `yaml:"mysql_conn_str"`
}

var (
	testConf *testConfig
)

type testFlag struct {
	sync           bool
	testConcurrent bool
	testSerial     bool
}

func readConf(path string) (*testConfig, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("failed due to no conf file for p2p regtest")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read test conf file %s: %v", path, err)
	}

	conf := &testConfig{}
	if err := yaml.Unmarshal(content, conf); err != nil {
		return nil, fmt.Errorf("failed to parse test conf: %v", err)
	}
	return conf, nil
}

func getUrlList(conf *testConfig) error {
	portStr := conf.BrokerPorts
	ports := strings.Split(portStr, Deliminator)
	httpProtocol := conf.HttpProtocol
	var hostProtocol string
	if httpProtocol == "https" {
		hostProtocol = "https"
	} else {
		hostProtocol = "http"
	}
	if len(ports) != 3 {
		return fmt.Errorf("need three ports for test, got: %s", portStr)
	}
	aliceAddress := fmt.Sprintf("%s://localhost:%s", hostProtocol, strings.Trim(ports[0], " "))
	aliceStub = brokerutil.NewCommand(aliceAddress, timeoutS)
	bobAddress := fmt.Sprintf("%s://localhost:%s", hostProtocol, strings.Trim(ports[1], " "))
	bobStub = brokerutil.NewCommand(bobAddress, timeoutS)
	carolAddress := fmt.Sprintf("%s://localhost:%s", hostProtocol, strings.Trim(ports[2], " "))
	carolStub = brokerutil.NewCommand(carolAddress, timeoutS)
	stubMap = make(map[string]*brokerutil.Command)
	stubMap[alice] = aliceStub
	stubMap[bob] = bobStub
	stubMap[carol] = carolStub
	return nil
}

func TestMain(m *testing.M) {
	confFile := flag.String("conf", "", "/path/to/conf")
	flag.Parse()
	var err error
	testConf, err = readConf(*confFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(testConf.BrokerPorts) == 0 {
		fmt.Println("Skipping testing due to empty BrokerPorts")
		return
	}

	mysqlConf := &config.StorageConf{
		ConnStr:         testConf.MySQLConnStr,
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 120,
		ConnMaxLifetime: 3000,
	}
	if err := testDataSource.ConnDB(mysqlConf); err != nil {
		fmt.Printf("connect mysql(%s) failed\n", testConf.MySQLConnStr)
		panic(err)
	}
	os.Exit(m.Run())
}

func TestRunQueryWithNormalCCL(t *testing.T) {
	r := require.New(t)
	err := clearData()
	r.NoError(err)
	err = getUrlList(testConf)
	r.NoError(err)
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	regtest.FillTableToPartyCodeMap(mockTables)
	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	r.NoError(createProjectTableAndCcl(testConf.ProjectConf, cclList, testConf.SkipCreateTableCCL))
	r.NoError(runQueryTest(alice, testFlag{sync: false, testConcurrent: !testConf.SkipConcurrentTest, testSerial: true}))
	r.NoError(runQueryTest(alice, testFlag{sync: true, testConcurrent: !testConf.SkipConcurrentTest, testSerial: true}))
}

func TestConcurrentModifyProject(t *testing.T) {
	r := require.New(t)
	err := clearData()
	r.NoError(err)
	err = getUrlList(testConf)
	r.NoError(err)
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	regtest.FillTableToPartyCodeMap(mockTables)
	if testConf.SkipConcurrentTest {
		return
	}
	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	testN := 1000
	// mock project id
	var projectIDs []string
	for i := 0; i < testN; i++ {
		projectID, err := application.GenerateProjectID()
		r.NoError(err)
		projectIDs = append(projectIDs, projectID)
	}
	outCh := make(chan error, testN)
	inChan := make(chan int, concurrentNum)
	go func() {
		for _, id := range projectIDs {
			inChan <- 0
			go func(id string) {
				err := concurrentModifyProjectTableAndCcl(id, testConf.ProjectConf, cclList)
				outCh <- err
				<-inChan
			}(id)
		}
	}()
	for i := 0; i < testN; i++ {
		r.NoError(<-outCh)
	}
}

func runQueryTest(user string, flags testFlag) (err error) {
	path := []string{"../testdata/single_party.json", "../testdata/two_parties.json", "../testdata/multi_parties.json"}
	for _, fileName := range path {
		if flags.testSerial {
			// use alice as issuer
			if err := testCaseForSerial(fileName, flags.sync, user); err != nil {
				return err
			}
		}
		if flags.testConcurrent {
			// use alice as issuer
			if err := testCaseForConcurrent(fileName, flags.sync, user); err != nil {
				return err
			}
		}
	}
	return nil
}

func testCaseForSerial(dataPath string, sync bool, issuer string) error {
	var suit regtest.QueryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return err
	}
	for i, query := range suit.Queries {
		comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", query.Query, query.Name, dataPath)
		answer, err := runSql(stubMap[issuer], query.Query, sync)
		if err != nil {
			return fmt.Errorf("%s Error Info (%s)", comment, err)
		}
		if err = regtest.CheckResult(testDataSource, query.Result, answer, query.MySQLQuery, comment); err != nil {
			return err
		}
		fmt.Printf("testfile: %s, sync: %v, issuer: %s, serial execution", dataPath, sync, issuer)
		percent := int64(float32(i+1) / float32(len(suit.Queries)) * 100)
		fmt.Printf(" %d%% (%v/%v)\r", percent, i+1, len(suit.Queries))
	}
	fmt.Println()
	return nil
}

func testCaseForConcurrent(dataPath string, sync bool, issuer string) error {
	var suit regtest.QueryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return err
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
			comment := fmt.Sprintf("For query='%s', name='%s' in testfile='%s'", testCase.Query, testCase.Name, dataPath)
			answer, err := runSql(stubMap[issuer], testCase.Query, sync)
			if err != nil {
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
		fmt.Printf("testfile: %s, sync: %v, issuer: %s, concurrent execution ", dataPath, sync, issuer)
		percent := int64(float32(i+1) / float32(len(suit.Queries)) * 100)
		fmt.Printf("%d%% (%v/%v)\r", percent, i+1, len(suit.Queries))
	}
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

func getUnAcceptedInvitation(response *scql.ListInvitationsResponse, projectID string) (uint64, error) {
	for _, invitation := range response.Invitations {
		if invitation.Status == scql.InvitationStatus_UNDECIDED && invitation.Project.ProjectId == projectID {
			return invitation.InvitationId, nil
		}
	}
	return 0, fmt.Errorf("unable to find unaccepted invitation")
}

func columnControlVisibilityToConstraint(level scql.SecurityConfig_ColumnControl_Visibility) scql.Constraint {
	return scql.Constraint(scql.Constraint_value[scql.SecurityConfig_ColumnControl_Visibility_name[int32(level)]])
}

func createProjectTableAndCcl(projectConf string, cclList []*scql.SecurityConfig_ColumnControl, skipCreate bool) error {
	if skipCreate {
		fmt.Println("skip func createProjectTableAndCcl")
		return nil
	}
	// create project
	_, err := aliceStub.CreateProject(defaultProjectName, projectConf)
	if err != nil {
		return err
	}
	// invite bob
	err = aliceStub.InviteMember(defaultProjectName, bob)
	if err != nil {
		return err
	}
	// bob accept
	err = processInvitation(bobStub, defaultProjectName)
	if err != nil {
		return err
	}
	// invite carol
	err = aliceStub.InviteMember(defaultProjectName, carol)
	if err != nil {
		return err
	}
	// carol accept
	err = processInvitation(carolStub, defaultProjectName)
	if err != nil {
		return err
	}

	physicalTableMetas, err := mock.MockPhysicalTableMetas()
	if err != nil {
		return err
	}
	tableExistMap := map[string]bool{}
	// party code to ccls
	fmt.Println("Create Table...")
	ccls := make(map[string][]*scql.ColumnControl)
	for _, ccl := range cclList {
		findTable := false
		dbTableName := fmt.Sprintf(`%s_%s`, ccl.DatabaseName, ccl.TableName)
		if _, exist := tableExistMap[dbTableName]; exist {
			findTable = true
		}
		tableOwner := regtest.TableToPartyCode[dbTableName]
		if !findTable {
			pt, ok := physicalTableMetas[dbTableName]
			if !ok {
				return fmt.Errorf("%s not found in physicalTableMetas", dbTableName)
			}
			err = stubMap[tableOwner].CreateTable(defaultProjectName, dbTableName, pt.DBType, pt.RefTable(), pt.GetColumnDesc())
			if err != nil {
				return err
			}
			tableExistMap[dbTableName] = true
		}
		ccls[tableOwner] = append(ccls[tableOwner], &scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: ccl.ColumnName, TableName: dbTableName}, PartyCode: ccl.PartyCode, Constraint: columnControlVisibilityToConstraint(ccl.Visibility)})
	}
	fmt.Println("Grant CCL...")
	for party, values := range ccls {
		err = stubMap[party].GrantCCL(defaultProjectName, values)
		if err != nil {
			return err
		}
	}
	fmt.Println("Grant Complete!")
	fmt.Println()
	return nil
}

func concurrentModifyProjectTableAndCcl(projectID, projectConf string, cclList []*scql.SecurityConfig_ColumnControl) error {
	// create project
	_, err := aliceStub.CreateProject(projectID, projectConf)
	if err != nil {
		return err
	}
	// make error channel big enough
	errCh := make(chan error, 100)
	// invite bob
	go func() {
		// invite bob
		err = aliceStub.InviteMember(projectID, bob)
		errCh <- err
	}()

	go func() {
		// invite carol
		err = aliceStub.InviteMember(projectID, carol)
		errCh <- err
	}()
	for i := 0; i < 2; i++ {
		err = <-errCh
		if err != nil {
			return err
		}
	}
	go func() {
		// bob accept
		err = processInvitation(bobStub, projectID)
		errCh <- err
	}()

	go func() {
		// carol accept
		err = processInvitation(carolStub, projectID)
		errCh <- err
	}()
	for i := 0; i < 2; i++ {
		err = <-errCh
		if err != nil {
			return err
		}
	}

	if err = checkProjectMemberNum(bobStub, projectID, 3); err != nil {
		return err
	}
	if err = checkProjectMemberNum(carolStub, projectID, 3); err != nil {
		return err
	}
	physicalTableMetas, err := mock.MockPhysicalTableMetas()
	if err != nil {
		return err
	}
	// table name to ccls
	tableExistMap := make(map[string][]*scql.ColumnControl)
	for _, ccl := range cclList {
		dbTableName := fmt.Sprintf(`%s_%s`, ccl.DatabaseName, ccl.TableName)
		tableExistMap[dbTableName] = append(tableExistMap[dbTableName], &scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: ccl.ColumnName, TableName: dbTableName}, PartyCode: ccl.PartyCode, Constraint: columnControlVisibilityToConstraint(ccl.Visibility)})
	}
	for tableName, ccls := range tableExistMap {
		pt, ok := physicalTableMetas[tableName]
		if !ok {
			return fmt.Errorf("%s not found in physicalTableMetas", tableName)
		}
		go func(tableName string, ccls []*scql.ColumnControl) {
			tableOwner := regtest.TableToPartyCode[tableName]
			fmt.Println("Create Table...")
			err = stubMap[tableOwner].CreateTable(projectID, tableName, pt.DBType, pt.RefTable(), pt.GetColumnDesc())
			errCh <- err
			fmt.Println("Grant CCL...")
			err = stubMap[tableOwner].GrantCCL(projectID, ccls)
			errCh <- err
		}(tableName, ccls)
	}
	for i := 0; i < len(tableExistMap)*2; i++ {
		err = <-errCh
		if err != nil {
			return err
		}
	}
	// run a query on each table
	for tableName := range tableExistMap {
		go func(tableName string) {
			// tricky skip tbl_3
			if strings.HasSuffix(tableName, "tbl_3") {
				errCh <- nil
				return
			}
			tableOwner := regtest.TableToPartyCode[tableName]
			_, err = stubMap[tableOwner].DoQuery(projectID, fmt.Sprintf("select * from %s limit 1", tableName))
			errCh <- err
		}(tableName)
	}
	for i := 0; i < len(tableExistMap); i++ {
		err = <-errCh
		if err != nil {
			return err
		}
	}
	// drop tables
	for tableName, ccls := range tableExistMap {
		go func(tableName string, ccls []*scql.ColumnControl) {
			tableOwner := regtest.TableToPartyCode[tableName]
			fmt.Println("Drop Table...")
			err = stubMap[tableOwner].DeleteTable(projectID, tableName)
			errCh <- err
		}(tableName, ccls)
	}
	for i := 0; i < len(tableExistMap); i++ {
		err = <-errCh
		if err != nil {
			return err
		}
	}
	fmt.Println()
	return nil
}

func checkProjectMemberNum(stub *brokerutil.Command, projectID string, expectNum int) error {
	for i := 0; i < 3; i++ {
		projectResponse, err := stub.GetProject(projectID)
		if err != nil {
			return err
		}
		if len(projectResponse.Projects) != 1 {
			return fmt.Errorf("expect only one project, but got %+v", projectResponse.Projects)
		}
		projDesc := projectResponse.Projects[0]
		if len(projDesc.Members) == expectNum {
			return nil
		}
		fmt.Printf("expect project member num %d, but got %d\n", expectNum, len(projDesc.Members))
		time.Sleep(time.Second)
	}
	return fmt.Errorf("fail to check project member")
}

func processInvitation(stub *brokerutil.Command, projectID string) error {
	inviteResponse, err := stub.GetInvitation()
	if err != nil {
		return err
	}
	inviteId, err := getUnAcceptedInvitation(inviteResponse, projectID)
	if err != nil {
		return err
	}
	return stub.ProcessInvitation(fmt.Sprintf("%d", inviteId), true)
}

func runSql(command *brokerutil.Command, sql string, sync bool) ([]*scql.Tensor, error) {
	if sync {
		resp, err := command.DoQuery(defaultProjectName, sql)
		if err != nil {
			return nil, err
		}
		if int32(scql.Code_OK) != resp.GetStatus().GetCode() {
			return nil, fmt.Errorf("error code %d, %+v", resp.Status.Code, resp)
		}
		return resp.OutColumns, nil
	} else {
		jobID, err := command.CreateJob(defaultProjectName, sql)
		if err != nil {
			return nil, err
		}
		count := 0
		for {
			if fetchIntervalMs*count > timeoutS*1000 {
				return nil, fmt.Errorf("fetch result timeout")
			}
			resp, notReady, err := command.GetResult(jobID)
			if err != nil {
				return nil, err
			}
			if !notReady {
				return resp.OutColumns, nil
			}
			count++
			time.Sleep(fetchIntervalMs * time.Millisecond)
		}
	}
}

func clearData() error {
	for _, dbName := range dbs {
		for _, tableName := range tables {
			err := testDataSource.TruncateTable(fmt.Sprintf("%s.%s", dbName, tableName))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
