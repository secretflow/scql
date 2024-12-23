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

package application

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/broker/storage"
)

func TestSimpleWorker(t *testing.T) {
	r := require.New(t)
	manager, err := dbSetUp("simple_worker")
	r.NoError(err)
	i := 0
	j := 0
	w, err := NewCronJobWorker(time.Second, manager)
	r.NoError(err)
	defer w.stop()
	w.addTask(&task{name: "count", interval: time.Millisecond * 500, fn: func() { i += 1 }})
	w.addTask(&task{name: "count", interval: time.Millisecond * 200, fn: func() { j += 1 }})
	w.start()
	time.Sleep(3*time.Second + 100*time.Millisecond)
	// (3*time.Second - time.Second - time.Millisecond * 500) / (time.Millisecond * 500) + 1 = 4
	r.Equal(4, i)
	// (3*time.Second - time.Second - time.Millisecond * 200) / (time.Millisecond * 200) + 1 = 10
	r.Equal(10, j)
}

func TestClusterWorker(t *testing.T) {
	r := require.New(t)
	manager1, err := dbSetUp("cluster_work")
	r.NoError(err)

	w1, err := NewCronJobWorker(time.Millisecond*100, manager1)
	r.NoError(err)
	w1.addTask(&task{name: "nothing", interval: time.Millisecond * 500, fn: func() {
		// do nothing
	}})
	w1.start()
	defer w1.stop()
	// sleep to wait w1 getting lock
	time.Sleep(150 * time.Millisecond)
	manager2, err := dbSetUp("cluster_work")
	r.NoError(err)
	w2, err := NewCronJobWorker(time.Millisecond*100, manager2)
	r.NoError(err)
	w2.addTask(&task{name: "nothing", interval: time.Millisecond * 500, fn: func() {
		// do nothing
	}})
	w2.start()
	defer w2.stop()

	w1.stop()
	time.Sleep(420 * time.Millisecond)
	owner, err := storage.NewDistributeLockGuard(manager1).GetDistributedLockOwner(storage.DistributedLockID)
	r.NoError(err)
	r.Equal(w2.id, owner)
	r.False(w1.started)
	r.True(w2.started)

}

func dbSetUp(dbName string) (*storage.MetaManager, error) {
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", dbName)
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

	manager := storage.NewMetaManager(db)
	err = manager.Bootstrap()
	if err != nil {
		return nil, err
	}

	return manager, nil
}
