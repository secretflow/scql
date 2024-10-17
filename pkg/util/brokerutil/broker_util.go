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

// Due to circular references, please avoid moving this file to testutil.
package brokerutil

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/storage"
)

type TestAppBuilder struct {
	PartyInfoTmpPath string
	PemFilePaths     map[string]string
	ServerAlice      *httptest.Server
	ServerBob        *httptest.Server
	ServerCarol      *httptest.Server
	ServerEngine     *httptest.Server
	ServerAliceUrl   string
	ServerBobUrl     string
	ServerCarolUrl   string
	ServerEngineUrl  string
	AppAlice         *application.App
	AppBob           *application.App
	AppCarol         *application.App
}

func (b *TestAppBuilder) buildHandlerTestApp(partyCode, engineEndpoint string) (*application.App, error) {
	cfg := config.Config{
		// won't bind to these two ports
		InterServer: config.ServerConfig{
			Port: 8081,
		},
		IntraServer: config.ServerConfig{
			Port: 8082,
		},
		PartyCode:                    partyCode,
		PartyInfoFile:                b.PartyInfoTmpPath,
		PrivateKeyPath:               b.PemFilePaths[partyCode],
		ExchangeJobInfoRetryTimes:    3,
		ExchangeJobInfoRetryInterval: time.Second,
		Engine: config.EngineConfig{
			ClientMode:    "HTTP",
			ClientTimeout: 1 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			Uris:          []config.EngineUri{{ForPeer: engineEndpoint}},
		},
	}
	partyMgr, err := partymgr.NewFilePartyMgr(cfg.PartyInfoFile)
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", partyCode)
	db, err := gorm.Open(sqlite.Open(connStr),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Info,
				}),
		})
	if err != nil {
		return nil, err
	}
	metaMgr := storage.NewMetaManager(db, false)
	return application.NewApp(partyMgr, metaMgr, &cfg)
}

func (b *TestAppBuilder) BuildAppTests(tempDir string) error {
	b.ServerAlice = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerBob = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerCarol = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerEngine = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	urlMap := make(map[string]string)
	b.ServerAliceUrl = "http://" + b.ServerAlice.Listener.Addr().String()
	urlMap["alice"] = b.ServerAlice.Listener.Addr().String()
	b.ServerBobUrl = "http://" + b.ServerBob.Listener.Addr().String()
	urlMap["bob"] = b.ServerBob.Listener.Addr().String()
	b.ServerCarolUrl = "http://" + b.ServerCarol.Listener.Addr().String()
	urlMap["carol"] = b.ServerCarol.Listener.Addr().String()
	portsMap := make(map[string]int)
	for party, url := range urlMap {
		_, port, err := net.SplitHostPort(url)
		if err != nil {
			return err
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		portsMap[party] = portNum
	}
	b.ServerEngineUrl = b.ServerEngine.Listener.Addr().String()
	filesMap, err := CreateTestPemFiles(portsMap, tempDir)
	if err != nil {
		return err
	}
	b.PemFilePaths = make(map[string]string)
	b.PartyInfoTmpPath = filesMap[PartyInfoFileKey]
	b.PemFilePaths["alice"] = filesMap[AlicePemFilKey]
	b.PemFilePaths["bob"] = filesMap[BobPemFileKey]
	b.PemFilePaths["carol"] = filesMap[CarolPemFileKey]
	b.AppAlice, err = b.buildHandlerTestApp("alice", b.ServerEngineUrl)
	if err != nil {
		return err
	}
	b.AppBob, err = b.buildHandlerTestApp("bob", b.ServerEngineUrl)
	if err != nil {
		return err
	}
	b.AppCarol, err = b.buildHandlerTestApp("carol", b.ServerEngineUrl)
	if err != nil {
		return err
	}
	return nil
}
