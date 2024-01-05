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
	"net/http"
	"net/http/httptest"
	"os"
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
		PrivatePemPath:               b.PemFilePaths[partyCode],
		ExchangeJobInfoRetryTimes:    3,
		ExchangeJobInfoRetryInterval: time.Second,
		Engine: config.EngineConfig{
			ClientTimeout: 1 * time.Second,
			Protocol:      "http",
			ContentType:   "application/json",
			Uris:          []config.EngineUri{{ForPeer: engineEndpoint}},
		},
	}
	partyMgr, err := partymgr.NewFilePartyMgr(cfg.PartyInfoFile, cfg.PartyCode)
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(sqlite.Open(":memory:"),
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
	metaMgr := storage.NewMetaManager(db)
	return application.NewApp(partyMgr, metaMgr, &cfg)
}

func (b *TestAppBuilder) BuildAppTests() error {
	partyJson, err := os.CreateTemp("", "party_info_*.json")
	defer partyJson.Close()
	formatter := `{
   "participants": [
     {
       "party_code": "alice",
       "endpoint": "%s",
       "pubkey": "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA="
     },
     {
       "party_code": "bob",
       "endpoint": "%s",
       "pubkey": "MCowBQYDK2VwAyEAN3w+v2uks/QEaVZiprZ8oRChMkBOZJSAl6V/5LvOnt4="
     },
     {
       "party_code": "carol",
       "endpoint": "%s",
       "pubkey": "MCowBQYDK2VwAyEANhiAXTvL4x2jYUiAbQRo9XuOTrFFnAX4Q+YlEAgULs8="
     }
   ]
 }
 `

	b.ServerAlice = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerBob = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerCarol = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerEngine = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	b.ServerAliceUrl = "http://" + b.ServerAlice.Listener.Addr().String()
	b.ServerBobUrl = "http://" + b.ServerBob.Listener.Addr().String()
	b.ServerCarolUrl = "http://" + b.ServerCarol.Listener.Addr().String()
	b.ServerEngineUrl = b.ServerEngine.Listener.Addr().String()

	partyJson.WriteString(fmt.Sprintf(formatter, b.ServerAliceUrl, b.ServerBobUrl, b.ServerCarolUrl))
	if err != nil {
		return err
	}
	b.PartyInfoTmpPath = partyJson.Name()
	pemAlice, err := os.CreateTemp("", "private_key_*.pem")
	if err != nil {
		return err
	}
	defer pemAlice.Close()
	pemAlice.WriteString(`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEICh+ZViILyFPq658OPYq6iKlSj802q1LfrmrV2i1GWcn
-----END PRIVATE KEY-----`)

	pemBob, err := os.CreateTemp("", "private_key_*.pem")
	if err != nil {
		return err
	}
	defer pemBob.Close()
	pemBob.WriteString(`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIOlHfgiMd9iEQtlJVdWGSBLyGnqIVc+sU3MAcfpJcP4S
-----END PRIVATE KEY-----`)

	pemCarol, err := os.CreateTemp("", "private_key_*.pem")
	if err != nil {
		return err
	}
	defer pemCarol.Close()
	pemCarol.WriteString(`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFvceWQFFw2LNbqVrJs1nSZji0HQZABKzTfrOABTBn5F
-----END PRIVATE KEY-----`)
	b.PemFilePaths = make(map[string]string)
	b.PemFilePaths["alice"] = pemAlice.Name()
	b.PemFilePaths["bob"] = pemBob.Name()
	b.PemFilePaths["carol"] = pemCarol.Name()
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
