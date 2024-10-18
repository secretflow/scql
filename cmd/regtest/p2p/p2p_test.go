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

package p2p

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/cmd/regtest"
	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/util/mock"
)

func TestMain(m *testing.M) {
	confFile := flag.String("conf", "", "/path/to/conf")
	flag.Var(&caseNames, "case", "name of the case to be run")
	flag.Parse()
	var err error
	testConf, err = ReadConf(*confFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	projectName = fmt.Sprintf("scdb_%s", testConf.SpuProtocol)
	if len(testConf.BrokerAddrs) == 0 {
		fmt.Println("Skipping testing due to empty BrokerAddrs")
		return
	}

	mysqlConf := &config.StorageConf{
		ConnStr:         testConf.MySQLConnStr,
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 120,
		ConnMaxLifetime: 3000,
	}
	if err = GetUrlList(testConf); err != nil {
		panic(err)
	}

	start := time.Now()
	maxRetries := 8
	retryDelay := 8 * time.Second
	if err := testDataSource.ConnDB(mysqlConf, maxRetries, retryDelay); err != nil {
		fmt.Printf("connect MySQL(%s) failed\n", testConf.MySQLConnStr)
		panic(err)
	}
	ConnTime := time.Since(start)
	if ConnTime >= retryDelay {
		fmt.Println("Participant may be in initialization, start to validate all participants")
		if err = ValidateAllParticipants(); err != nil {
			fmt.Println("Validate all participants failed")
			panic(err)
		}
	}
	os.Exit(m.Run())
}

func TestRunQueryWithNormalCCL(t *testing.T) {
	r := require.New(t)

	r.NoError(ClearData(&testDataSource))
	r.NoError(GetUrlList(testConf))
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	regtest.FillTableToPartyCodeMap(mockTables)
	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	r.NoError(CreateProjectTableAndCcl(testConf.ProjectConf, cclList, testConf.SkipCreateTableCCL))
	path := map[string][]string{SEMI2K: {"../testdata/single_party.json", "../testdata/single_party_postgres.json", "../testdata/two_parties.json", "../testdata/multi_parties.json"}, CHEETAH: {"../testdata/two_parties.json"}, ABY3: {"../testdata/multi_parties.json"}}
	r.NoError(runQueryTest(alice, testConf.SpuProtocol, TestFlag{Sync: false, TestConcurrent: !testConf.SkipConcurrentTest, TestSerial: true}, path))
	r.NoError(runQueryTest(alice, testConf.SpuProtocol, TestFlag{Sync: true, TestConcurrent: !testConf.SkipConcurrentTest, TestSerial: true}, path))

	r.NoError(runQueryTestSessionExpired(alice, 1, 100))
	r.Error(runQueryTestSessionExpired(alice, 10, 1))
}

func TestConcurrentModifyProject(t *testing.T) {
	r := require.New(t)
	r.NoError(ClearData(&testDataSource))
	err := GetUrlList(testConf)
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
