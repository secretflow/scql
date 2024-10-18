// Copyright 2024 Ant Group Co., Ltd.
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

package p2p

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/secretflow/scql/cmd/regtest"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/brokerutil"
	"github.com/secretflow/scql/pkg/util/mock"
)

const (
	Deliminator = ","
	ABY3        = "ABY3"
	SEMI2K      = "SEMI2K"
	CHEETAH     = "CHEETAH"
)

const (
	concurrentNum = 5
	// timeout send request to intra broker
	timeoutS      = 60
	fetchInterval = 100 * time.Millisecond
	maxFetchCount = 1000
	// party code
	alice = "alice"
	bob   = "bob"
	carol = "carol"
)

var (
	projectName    = "scdb"
	testDataSource regtest.TestDataSource

	// addresses
	aliceStub *brokerutil.Command
	bobStub   *brokerutil.Command
	carolStub *brokerutil.Command
	stubMap   map[string]*brokerutil.Command

	// test tables
	tables = []string{"members", "column_privs", "columns", "tables", "invitations", "projects"}
	dbs    = []string{"brokera", "brokerb", "brokerc"}

	// test cases names, empty caseNames means all cases will be tested
	caseNames stringArrayFlag
)

type TestConfig struct {
	SkipCreateTableCCL   bool   `yaml:"skip_create_table_ccl"`
	SkipConcurrentTest   bool   `yaml:"skip_concurrent_test"`
	SkipPlaintextCCLTest bool   `yaml:"skip_plaintext_ccl_test"`
	ProjectConf          string `yaml:"project_conf"`
	HttpProtocol         string `yaml:"http_protocol"`
	BrokerAddrs          string `yaml:"broker_addrs"`
	SpuProtocol          string `yaml:"spu_protocol"`
	MySQLConnStr         string `yaml:"mysql_conn_str"`
}

var (
	testConf *TestConfig
)

type TestFlag struct {
	Sync           bool
	TestConcurrent bool
	TestSerial     bool
}

type fetchConf struct {
	maxFetchCount int
	fetchInterval time.Duration
}

type stringArrayFlag []string

func (s *stringArrayFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringArrayFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func NewFetchConf(maxFetchCount int, fetchInterval time.Duration) *fetchConf {
	return &fetchConf{maxFetchCount: maxFetchCount, fetchInterval: fetchInterval}
}

func ReadConf(path string) (*TestConfig, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("failed due to no conf file for p2p regtest")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read test conf file %s: %v", path, err)
	}

	conf := &TestConfig{}
	if err := yaml.Unmarshal(content, conf); err != nil {
		return nil, fmt.Errorf("failed to parse test conf: %v", err)
	}
	return conf, nil
}

func GetUrlList(conf *TestConfig) error {
	addrStr := conf.BrokerAddrs
	addrs := strings.Split(addrStr, Deliminator)
	httpProtocol := conf.HttpProtocol
	var hostProtocol string
	if httpProtocol == "https" {
		hostProtocol = "https"
	} else {
		hostProtocol = "http"
	}
	if len(addrs) != 3 {
		return fmt.Errorf("need three ports for test, got: %s", addrs)
	}
	aliceAddress := fmt.Sprintf("%s://%s", hostProtocol, strings.Trim(addrs[0], " "))
	aliceStub = brokerutil.NewCommand(aliceAddress, timeoutS)
	bobAddress := fmt.Sprintf("%s://%s", hostProtocol, strings.Trim(addrs[1], " "))
	bobStub = brokerutil.NewCommand(bobAddress, timeoutS)
	carolAddress := fmt.Sprintf("%s://%s", hostProtocol, strings.Trim(addrs[2], " "))
	carolStub = brokerutil.NewCommand(carolAddress, timeoutS)
	stubMap = make(map[string]*brokerutil.Command)
	stubMap[alice] = aliceStub
	stubMap[bob] = bobStub
	stubMap[carol] = carolStub
	return nil
}

func runQueryTestSessionExpired(user string, sleepSeconds int, sessionExpireSeconds int) error {
	sql := "select plain_int_0, plain_datetime_0, plain_timestamp_0 from alice_tbl_1;"
	jobID, err := stubMap[user].CreateJob(projectName, sql, &scql.DebugOptions{EnablePsiDetailLog: false}, fmt.Sprintf(`{"session_expire_seconds": %v}`, sessionExpireSeconds))

	if err != nil {
		return err
	}

	time.Sleep(time.Second * time.Duration(sleepSeconds))
	_, err = stubMap[user].GetResult(jobID)
	return err
}

func runQueryTest(user string, spu_protocol string, flags TestFlag, path map[string][]string) (err error) {
	fmt.Printf("test protocol: %s, sync=%v\n", spu_protocol, flags.Sync)
	for _, fileName := range path[spu_protocol] {
		if flags.TestSerial {
			// use alice as issuer
			if err := testCaseForSerial(fileName, flags.Sync, user); err != nil {
				return err
			}
		}
		if flags.TestConcurrent {
			// use alice as issuer
			if err := testCaseForConcurrent(fileName, flags.Sync, user); err != nil {
				return err
			}
		}
	}
	return nil
}

func genComment(name, query, path string) string {
	return fmt.Sprintf("Test case[%s], query=[%s] in testfile '%s'", name, query, path)
}

func filterQueries(queries []regtest.QueryCase, caseNames []string) ([]regtest.QueryCase, []string) {
	if len(caseNames) == 0 {
		return queries, nil
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Name < queries[j].Name
	})

	sort.Slice(caseNames, func(i, j int) bool {
		return caseNames[i] < caseNames[j]
	})

	queryIndex := 0
	nameIndex := 0
	var filtered []regtest.QueryCase
	var unmatchedNames []string
	for queryIndex < len(queries) && nameIndex < len(caseNames) {
		if queries[queryIndex].Name == caseNames[nameIndex] {
			filtered = append(filtered, queries[queryIndex])
			queryIndex++
			nameIndex++
		} else if queries[queryIndex].Name < caseNames[nameIndex] {
			queryIndex++
		} else {
			unmatchedNames = append(unmatchedNames, caseNames[nameIndex])
			nameIndex++
		}
	}
	for nameIndex < len(caseNames) {
		unmatchedNames = append(unmatchedNames, caseNames[nameIndex])
		nameIndex++
	}

	return filtered, unmatchedNames
}

func testCaseForSerial(dataPath string, sync bool, issuer string) error {
	var suit regtest.QueryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return err
	}
	filtered, unmatchedNames := filterQueries(suit.Queries, caseNames)
	if len(unmatchedNames) > 0 {
		fmt.Printf("testfile: %s, unmatched case names: %s\n", dataPath, strings.Join(unmatchedNames, ", "))
	}
	for i, query := range filtered {
		comment := genComment(query.Name, query.Query, dataPath)
		answer, err := runSql(stubMap[issuer], query.Query, sync, "{}", NewFetchConf(maxFetchCount, fetchInterval))
		if err != nil {
			return fmt.Errorf("%s Error Info (%s)", comment, err)
		}
		if err = regtest.CheckResult(testDataSource, query.Result, answer, query.MySQLQuery, comment); err != nil {
			return err
		}

		fmt.Printf("\x1b[2K\r")
		fmt.Printf("testfile: %s, sync: %v, issuer: %s, case name: [%s], serial execution", dataPath, sync, issuer, query.Name)
		percent := int64(float32(i+1) / float32(len(filtered)) * 100)
		fmt.Printf(" %d%% (%v/%v)", percent, i+1, len(filtered))
	}
	if len(filtered) > 0 {
		fmt.Println()
	}
	return nil
}

func testCaseForConcurrent(dataPath string, sync bool, issuer string) error {
	var suit regtest.QueryTestSuit
	if err := createSuit(dataPath, &suit); err != nil {
		return err
	}
	inChan := make(chan int, concurrentNum)
	outChan := make(chan error)
	filtered, unmatchedNames := filterQueries(suit.Queries, caseNames)
	if len(unmatchedNames) > 0 {
		fmt.Printf("testfile: %s, unmatched case names: %s\n", dataPath, strings.Join(unmatchedNames, ", "))
	}
	for i, query := range filtered {
		go func(num int, testCase regtest.QueryCase) {
			var err error
			inChan <- num
			defer func() {
				outChan <- err
				<-inChan
			}()
			comment := genComment(query.Name, query.Query, dataPath)
			answer, err := runSql(stubMap[issuer], testCase.Query, sync, "{}", NewFetchConf(maxFetchCount, fetchInterval))
			if err != nil {
				return
			}
			if err = regtest.CheckResult(testDataSource, testCase.Result, answer, testCase.MySQLQuery, comment); err != nil {
				return
			}
		}(i, query)
	}

	for i := range filtered {
		err := <-outChan
		if err != nil {
			return err
		}
		fmt.Printf("\x1b[2K\r")
		fmt.Printf("testfile: %s, sync: %v, issuer: %s, concurrent execution", dataPath, sync, issuer)
		percent := int64(float32(i+1) / float32(len(filtered)) * 100)
		fmt.Printf(" %d%% (%v/%v)", percent, i+1, len(filtered))
	}
	if len(filtered) > 0 {
		fmt.Println()
	}
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

func CreateProjectTableAndCcl(projectConf string, cclList []*scql.SecurityConfig_ColumnControl, skipCreate bool) error {
	if skipCreate {
		fmt.Println("skip func CreateProjectTableAndCcl")
		return nil
	}
	// create project
	_, err := aliceStub.CreateProject(projectName, projectConf)
	if err != nil {
		return err
	}
	// invite bob
	err = aliceStub.InviteMember(projectName, bob)
	if err != nil {
		return err
	}
	// bob accept
	err = processInvitation(bobStub, projectName)
	if err != nil {
		return err
	}
	// invite carol
	err = aliceStub.InviteMember(projectName, carol)
	if err != nil {
		return err
	}
	// carol accept
	err = processInvitation(carolStub, projectName)
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
			err = stubMap[tableOwner].CreateTable(projectName, dbTableName, pt.DBType, pt.RefTable(), pt.GetColumnDesc())
			if err != nil {
				return err
			}
			tableExistMap[dbTableName] = true
		}
		ccls[tableOwner] = append(ccls[tableOwner], &scql.ColumnControl{Col: &scql.ColumnDef{ColumnName: ccl.ColumnName, TableName: dbTableName}, PartyCode: ccl.PartyCode, Constraint: columnControlVisibilityToConstraint(ccl.Visibility)})
	}
	fmt.Println("Grant CCL...")
	for party, values := range ccls {
		err = stubMap[party].GrantCCL(projectName, values)
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
			// fmt.Println("Create Table...")
			err = stubMap[tableOwner].CreateTable(projectID, tableName, pt.DBType, pt.RefTable(), pt.GetColumnDesc())
			errCh <- err
			// fmt.Println("Grant CCL...")
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
			_, err = stubMap[tableOwner].DoQuery(projectID, fmt.Sprintf("select * from %s limit 1", tableName), &scql.DebugOptions{EnablePsiDetailLog: false}, "{}")
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
			// fmt.Println("Drop Table...")
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
	// fmt.Println()
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

func RunSql(issuer string, sql string, jobConf string, fc *fetchConf) ([]*scql.Tensor, error) {
	return runSql(stubMap[issuer], sql, false, jobConf, fc)
}

func runSql(command *brokerutil.Command, sql string, sync bool, jobConf string, fc *fetchConf) ([]*scql.Tensor, error) {
	if sync {
		resp, err := command.DoQuery(projectName, sql, &scql.DebugOptions{EnablePsiDetailLog: false}, jobConf)
		if err != nil {
			return nil, err
		}
		if int32(scql.Code_OK) != resp.GetStatus().GetCode() {
			return nil, fmt.Errorf("error code %d, %+v", resp.Status.Code, resp)
		}
		return resp.GetResult().GetOutColumns(), nil
	} else {
		jobID, err := command.CreateJob(projectName, sql, &scql.DebugOptions{EnablePsiDetailLog: false}, jobConf)
		if err != nil {
			return nil, err
		}

		for count := 0; count < fc.maxFetchCount; count++ {
			resp, err := command.GetResult(jobID)
			if err != nil {
				return nil, err
			}
			if resp.GetStatus().GetCode() == int32(scql.Code_OK) {
				return resp.GetResult().GetOutColumns(), nil
			} else if resp.GetStatus().GetCode() == int32(scql.Code_NOT_READY) {
				time.Sleep(fc.fetchInterval)
			} else {
				return nil, fmt.Errorf("GetResult status: %v", resp.GetStatus())
			}
		}
		return nil, fmt.Errorf("fetch result timeout")
	}
}

func ClearData(ds *regtest.TestDataSource) error {
	for _, dbName := range dbs {
		for _, tableName := range tables {
			err := ds.TruncateTable(fmt.Sprintf("%s.%s", dbName, tableName))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func validateParticipant(stub *brokerutil.Command) (err error) {
	maxRetries := 8
	retryDelay := 8 * time.Second
	var empty_ids []string
	for i := 0; i < maxRetries; i++ {
		_, err := stub.CheckAndUpdateStatus(empty_ids)
		if err == nil {
			return nil
		}
		if i == maxRetries-1 {
			return err
		} else {
			time.Sleep(retryDelay)
		}
	}
	return err
}

func ValidateAllParticipants() (err error) {
	if err = GetUrlList(testConf); err != nil {
		return err
	}
	if err = validateParticipant(aliceStub); err != nil {
		return err
	}
	if err = validateParticipant(bobStub); err != nil {
		return err
	}
	if err = validateParticipant(carolStub); err != nil {
		return err
	}
	return nil
}
