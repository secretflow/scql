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

type TaskFunc func()

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
	app.AddSession("s3", &Session{})
	app.AddSession("s4", &Session{})
	expireSeconds := 2
	app.MetaMgr.SetSessionInfo(storage.SessionInfo{
		SessionID: "s1",
		CreatedAt: time.Now(),
		Status:    int8(storage.SessionCanceled),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds*10) * time.Second),
	})

	app.MetaMgr.SetSessionInfo(storage.SessionInfo{
		SessionID: "s2",
		CreatedAt: time.Now(),
		Status:    int8(storage.SessionFinished),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
	})
	app.MetaMgr.SetSessionInfo(storage.SessionInfo{
		SessionID: "s3",
		CreatedAt: time.Now(),
		Status:    int8(storage.SessionRunning),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
	})
	app.MetaMgr.SetSessionInfo(storage.SessionInfo{
		SessionID: "s2",
		CreatedAt: time.Now(),
		Status:    int8(storage.SessionFinished),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
	})

	r.Equal(app.Sessions.ItemCount(), 4)

	stop := make(chan bool)
	go StartTask(app.SessionGc, app.Conf.SessionCheckInterval, stop)

	time.Sleep(app.Conf.SessionCheckInterval / 2)
	r.Equal(app.Sessions.ItemCount(), 4)
	time.Sleep(time.Second * time.Duration(expireSeconds) * 2)

	// 2 cleared for expired, 1 cleared for canceled
	r.Equal(app.Sessions.ItemCount(), 1)

	stop <- true
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

	expireSeconds := 3
	err = app.MetaMgr.SetSessionResult(storage.SessionResult{
		SessionID: "s1",
		CreatedAt: time.Now(),
		ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
	})
	r.NoError(err)

	sr, err := app.MetaMgr.GetSessionResult("s1")
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	stop := make(chan bool)
	go StartTask(app.StorageGc, app.Conf.SessionCheckInterval, stop)

	time.Sleep(app.Conf.SessionCheckInterval * 3)
	sr, err = app.MetaMgr.GetSessionResult("s1")
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	time.Sleep(time.Second * time.Duration(expireSeconds) * 3)
	sr, err = app.MetaMgr.GetSessionResult("s1")
	r.Equal(err.Error(), "record not found")

	stop <- true
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
		SessionExpireTime:    100 * time.Second,
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

func StartTask(task TaskFunc, interval time.Duration, stop chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			task()
		case <-stop:
			return
		}
	}
}
