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

package executor

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/storage"
)

func TestJobWatcher(t *testing.T) {
	r := require.New(t)
	manager, _, err := dbSetUp()
	r.NoError(err)

	var jobDead atomic.Bool
	cb := func(jobID, reason string) {
		jobDead.Store(true)
	}

	w, err := NewJobWatcher(manager, cb, WithMaxUnavailableDuration(time.Second*3))
	r.NoError(err)
	stopFn, err := StartJobWatcherScheduler(w, time.Second)
	r.NoError(err)
	defer stopFn()
	jobId := "test-job-id"
	// err = w.metaManager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
	// 	return txn.SetSessionInfo(&storage.SessionInfo{SessionID: jobId, Status: int8(storage.SessionRunning), EngineUrl: unreachableAddr, EngineUrlForSelf: unreachableAddr})
	// })
	// r.NoError(err)

	// sleep 5s to wait query job been mark dead, since 5s > 1s*3
	time.Sleep(time.Second * 5)
	r.True(jobDead.Load())
	var sessionInfo *storage.SessionInfo
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sessionInfo, err = txn.GetSessionInfo(jobId)
		return err
	})
	r.NoError(err)
	r.Equal(sessionInfo.Status, int8(storage.SessionFailed))
}

func TestJobWatcherBeforeEngineExecute(t *testing.T) {
	r := require.New(t)
	manager, _, err := dbSetUp()
	r.NoError(err)

	jobId := "test-job-id"
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.UpdateSessionInfoStatusWithCondition(jobId, storage.SessionRunning, storage.SessionSubmitted)
	})
	r.NoError(err)
	var jobDead atomic.Bool
	cb := func(jobID, reason string) {
		jobDead.Store(true)
	}

	w, err := NewJobWatcher(manager, cb, WithMaxUnavailableDuration(time.Second*3))
	r.NoError(err)
	stopFn, err := StartJobWatcherScheduler(w, time.Second)
	r.NoError(err)
	defer stopFn()

	// job isn't watched because the statu is SessionSubmitted
	time.Sleep(time.Second * 5)
	r.False(jobDead.Load())

	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.UpdateSessionInfoStatusWithCondition(jobId, storage.SessionSubmitted, storage.SessionRunning)
	})
	r.NoError(err)
	// sleep 5s to wait query job been mark dead, since 5s > 1s*3
	time.Sleep(time.Second * 5)
	r.True(jobDead.Load())

	var sessionInfo *storage.SessionInfo
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sessionInfo, err = txn.GetSessionInfo(jobId)
		return err
	})
	r.NoError(err)
	r.Equal(sessionInfo.Status, int8(storage.SessionFailed))
}

func TestJobWatcherReStart(t *testing.T) {
	r := require.New(t)
	manager, unreachableAddr, err := dbSetUp()
	r.NoError(err)

	var jobDead atomic.Bool
	cb := func(jobID, reason string) {
		jobDead.Store(true)
	}

	w, err := NewJobWatcher(manager, cb, WithMaxUnavailableDuration(time.Second*3))
	r.NoError(err)

	jobId := "test-job-id"
	err = w.metaManager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.SetSessionInfo(&storage.SessionInfo{SessionID: jobId, Status: int8(storage.SessionRunning), EngineUrl: unreachableAddr, EngineUrlForSelf: unreachableAddr})
	})

	time.Sleep(time.Second)
	stopFn, err := StartJobWatcherScheduler(w, time.Second)
	r.NoError(err)
	defer stopFn()

	// restart
	w, err = NewJobWatcher(manager, cb)
	r.NoError(err)
	stopFn, err = StartJobWatcherScheduler(w, time.Second)
	r.NoError(err)
	defer stopFn()

	// sleep 5s to wait query job been mark dead, since 5s > 1s*3
	time.Sleep(time.Second * 5)
	r.True(jobDead.Load())

	var sessionInfo *storage.SessionInfo
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		sessionInfo, err = txn.GetSessionInfo(jobId)
		return err
	})
	r.NoError(err)
	r.Equal(sessionInfo.Status, int8(storage.SessionFailed))
}

func dbSetUp() (*storage.MetaManager, string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, "", err
	}

	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
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
		return nil, "", err
	}

	manager := storage.NewMetaManager(db)
	err = manager.Bootstrap()
	if err != nil {
		return nil, "", err
	}

	port, err := getFreePort()
	if err != nil {
		return nil, "", err
	}
	unreachableAddr := fmt.Sprintf("127.0.0.1:%d", port)

	sessionInfo := storage.SessionInfo{
		SessionID:        "test-job-id",
		EngineUrlForSelf: unreachableAddr,
		Status:           int8(storage.SessionRunning),
	}
	err = manager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		return txn.SetSessionInfo(&sessionInfo)
	})
	if err != nil {
		return nil, "", err
	}

	return manager, unreachableAddr, nil
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
