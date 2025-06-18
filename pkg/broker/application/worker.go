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
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

type Role = int32

const (
	Leader Role = iota + 1
	Worker
)

type task struct {
	name     string
	interval time.Duration
	fn       func()
}

type cronJobWorker struct {
	id              string
	interval        time.Duration
	role            atomic.Int32
	tasks           []*task
	started         bool
	taskScheduler   gocron.Scheduler
	leaderScheduler gocron.Scheduler
	manager         *storage.MetaManager
	mu              sync.Mutex
}

func NewCronJobWorker(interval time.Duration, manager *storage.MetaManager) (*cronJobWorker, error) {
	if err := storage.NewDistributeLockGuard(manager).InitDistributedLockIfNecessary(storage.DistributedLockID); err != nil {
		return nil, err
	}
	w := &cronJobWorker{
		id:       os.Getenv("HOSTNAME") + "-" + stringutil.RandString(8),
		interval: interval,
		manager:  manager,
	}
	logrus.Infof("start cron job worker %s", w.id)
	w.role.Store(Worker)
	return w, nil
}

// sacrifice for gocron.Elector interface
func (w *cronJobWorker) IsLeader(context.Context) error {
	if w.isLeader() {
		return nil
	}
	return fmt.Errorf("not leader")
}

func (w *cronJobWorker) isLeader() bool {
	return w.role.Load() == Leader

}

func (w *cronJobWorker) setRole(r Role) {
	w.role.Store(r)
}

func (w *cronJobWorker) addTask(t *task) {
	w.tasks = append(w.tasks, t)
}

func (w *cronJobWorker) startTasks() error {
	s, err := gocron.NewScheduler(gocron.WithDistributedElector(w))
	if err != nil {
		return fmt.Errorf("fail to create scheduler: %v", err)
	}
	for _, task := range w.tasks {
		_, err = s.NewJob(
			gocron.DurationJob(task.interval),
			gocron.NewTask(task.fn),
			gocron.WithName(task.name),
		)
		if err != nil {
			return fmt.Errorf("fail to schedule: %v", err)
		}

	}
	s.Start()
	logrus.Infof("%s start tasks", w.id)
	w.taskScheduler = s
	return nil
}

func (w *cronJobWorker) stopTasks() error {
	return w.taskScheduler.Shutdown()
}

func (w *cronJobWorker) start() error {
	// if already started or no tasks to run, do nothing
	if w.started || len(w.tasks) == 0 {
		return nil
	}
	w.started = true
	// leader scheduler
	s, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("fail to create scheduler: %v", err)
	}
	_, err = s.NewJob(
		gocron.DurationJob(w.interval),
		gocron.NewTask(func() {
			// if query running time is more than w.interval,
			// two task may work concurrently.
			// this lock is to avoid this situation
			w.mu.Lock()
			defer w.mu.Unlock()
			if !w.isLeader() {
				expired, err := storage.NewDistributeLockGuard(w.manager).DistributedLockExpired(storage.DistributedLockID, w.id)
				if err != nil {
					logrus.Warnf("fail to check leader: %v", err)
					return
				}
				if !expired {
					return
				}
				ok, err := storage.NewDistributeLockGuard(w.manager).PreemptDistributedLock(storage.DistributedLockID, w.id, w.interval)
				if err != nil {
					logrus.Infof("current instance failed to preempt lock, continues as follower: %v", err)
					return
				}
				if ok {
					err = w.startTasks()
					if err != nil {
						logrus.Warnf("fail to start tasks: %v", err)
						return
					}
					w.setRole(Leader)
					logrus.Infof("current instance successfully elected, now acting as leader %s", w.id)
				}
				return
			}
			ok, err := storage.NewDistributeLockGuard(w.manager).RenewDistributedLock(storage.DistributedLockID, w.id, w.interval)
			if !ok || err != nil {
				logrus.Warnf("fail to update leader: %v", err)
				err = w.stopTasks()
				if err != nil {
					logrus.Warnf("fail to stop tasks: %v", err)
					return
				}
				w.setRole(Worker)
				logrus.Infof("current instance failed to renew leader lock, now acting as follower: %v", err)
			} else {
				logrus.Infof("leader lock renewed, current instance continues as leader %s", w.id)
			}
		}),
		gocron.WithName("election"),
	)
	if err != nil {
		return fmt.Errorf("fail to schedule: %v", err)
	}

	s.Start()
	w.leaderScheduler = s
	return nil
}

func (w *cronJobWorker) stop() error {
	if !w.started {
		return nil
	}
	w.started = false
	var errs []error
	errs = append(errs, w.leaderScheduler.Shutdown())
	if w.isLeader() {
		errs = append(errs, w.taskScheduler.Shutdown())
	}
	return errors.Join(errs...)
}
