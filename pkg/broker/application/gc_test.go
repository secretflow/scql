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

package application

import (
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/storage"
)

func TestSessionGc(t *testing.T) {
	// if using short flag in go test, skip this case
	// FIXME: fix this case in pipeline
	if testing.Short() {
		t.Skip("Skipping TestSessionGc")
	}
	r := require.New(t)

	app, err := buildTestApp()
	r.NoError(err)

	app.AddSession("s1", &Session{})
	app.AddSession("s2", &Session{})
	app.MetaMgr.SetSessionInfo(storage.SessionInfo{
		SessionID: "s1",
		Status:    int8(storage.SessionCanceled),
	})
	r.Equal(app.Sessions.ItemCount(), 2)

	go app.SessionGc()

	time.Sleep(app.Conf.SessionCheckInterval / 2)
	r.Equal(app.Sessions.ItemCount(), 2)
	time.Sleep(app.Conf.SessionCheckInterval * 3)
	r.Equal(app.Sessions.ItemCount(), 1)
}

func TestStorageGc(t *testing.T) {
	// if using short flag in go test, skip this case
	// FIXME: fix this case in pipeline
	if testing.Short() {
		t.Skip("Skipping TestStorageGc")
	}
	r := require.New(t)

	app, err := buildTestApp()
	r.NoError(err)

	err = app.MetaMgr.SetSessionResult(storage.SessionResult{
		SessionID: "s1",
		CreatedAt: time.Now(),
	})
	r.NoError(err)

	sr, err := app.MetaMgr.GetSessionResult("s1")
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	go app.StorageGc()

	time.Sleep(app.Conf.SessionCheckInterval * 3)
	sr, err = app.MetaMgr.GetSessionResult("s1")
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	time.Sleep(app.Conf.SessionExpireTime * 3)
	sr, err = app.MetaMgr.GetSessionResult("s1")
	r.Equal(err.Error(), "record not found")
}

func buildTestApp() (*App, error) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"))
	if err != nil {
		return nil, err
	}
	manager := storage.NewMetaManager(db, true)
	err = manager.Bootstrap()
	if err != nil {
		return nil, err
	}

	cfg := &config.Config{
		PersistSession:       true,
		SessionExpireTime:    100 * time.Millisecond,
		SessionCheckInterval: 10 * time.Millisecond,
	}
	app := &App{
		Sessions: cache.New(time.Duration(cfg.SessionExpireTime),
			time.Duration(cfg.SessionCheckInterval)),
		MetaMgr: manager,
		Conf:    cfg,
	}

	return app, nil
}
