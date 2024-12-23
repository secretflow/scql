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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/storage"
)

type TaskFunc func()

func TestStorageGc(t *testing.T) {
	// if using short flag in go test, skip this case
	// FIXME: fix this case in pipeline
	if testing.Short() {
		t.Skip("Skipping TestStorageGc")
	}
	r := require.New(t)

	app, err := buildTestApp("storage_gc_test")
	r.NoError(err)

	expireSeconds := 3
	manager := app.MetaMgr
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.SetSessionResult(storage.SessionResult{
			SessionID: "s1",
			CreatedAt: time.Now(),
			ExpiredAt: time.Now().Add(time.Duration(expireSeconds) * time.Second),
		})
	})
	r.NoError(err)
	var sr storage.SessionResult
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sr, err = txn.GetSessionResult("s1")
		return err
	})
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	stop := make(chan bool)
	go startTask(app.StorageGc, app.Conf.SessionCheckInterval, stop)

	time.Sleep(app.Conf.SessionCheckInterval * 3)
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sr, err = txn.GetSessionResult("s1")
		return err
	})
	r.NoError(err)
	r.Equal(sr.SessionID, "s1")

	time.Sleep(time.Second * time.Duration(expireSeconds) * 3)
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sr, err = txn.GetSessionResult("s1")
		return err
	})
	r.Equal(err.Error(), "record not found")

	stop <- true
}

func createManager(dbName string) (*storage.MetaManager, error) {
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", dbName)
	db, err := gorm.Open(sqlite.Open(connStr))
	if err != nil {
		return nil, err
	}
	manager := storage.NewMetaManager(db)
	return manager, nil
}

func buildTestApp(testName string) (*App, error) {
	manager, err := createManager(testName)
	if err != nil {
		return nil, err
	}
	err = manager.Bootstrap()
	if err != nil {
		return nil, err
	}
	cfg := &config.Config{
		SessionExpireTime:    100 * time.Second,
		SessionCheckInterval: 10 * time.Millisecond,
	}
	app := &App{
		MetaMgr: manager,
		Conf:    cfg,
	}

	return app, nil
}

func startTask(task TaskFunc, interval time.Duration, stop chan bool) {
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
