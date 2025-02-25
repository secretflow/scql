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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type OnJobDeadCb func(jobId, reason string)

// tracking job lifecycle and progress
// TODO: implement running progress tracking

type JobWatcher struct {
	onJobDeadCb OnJobDeadCb

	metaManager        *storage.MetaManager
	maxUnavailableTime time.Duration
	mu                 sync.Mutex
}

type JobWatcherOption func(*JobWatcher)

func WithMaxUnavailableDuration(maxUnavailableTime time.Duration) JobWatcherOption {
	return func(w *JobWatcher) {
		w.maxUnavailableTime = maxUnavailableTime
	}
}

func NewJobWatcher(metaManager *storage.MetaManager, cb OnJobDeadCb, opts ...JobWatcherOption) (*JobWatcher, error) {
	w := &JobWatcher{
		onJobDeadCb:        cb,
		metaManager:        metaManager,
		maxUnavailableTime: time.Minute * 3,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

func (w *JobWatcher) collectJobs() []*storage.SessionInfo {
	// update jobStatus from db first
	var jobs []*storage.SessionInfo
	err := w.metaManager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) (err error) {
		jobs, err = txn.GetWatchedJobs()
		return err
	})
	if err != nil {
		logrus.Warningf("JobWatcher collectJobs error: %v", err)
	}
	return jobs
}

func (w *JobWatcher) CheckJobs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	jobs := w.collectJobs()

	var unAliveJob []*storage.SessionInfo
	var unReachedJobs []*storage.SessionInfo
	var aliveJobIds []string
	for _, job := range jobs {
		alive, err := IsJobAlive(job)
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
				unReachedJobs = append(unReachedJobs, job)
			}
			logrus.Warnf("failed to probe job status: jobId=%s, endpoint=%s, err=%v", job.SessionID, job.EngineUrlForSelf, err)
			continue
		}
		if !alive {
			unAliveJob = append(unAliveJob, job)
			continue
		}
		aliveJobIds = append(aliveJobIds, job.SessionID)
	}

	err := w.metaManager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		return txn.UpdateSessionUpdatedAt(aliveJobIds)
	})
	if err != nil {
		logrus.Warnf("failed to update session updated_at: %v", err)
	}

	var failedJobIds []string
	for _, job := range unReachedJobs {
		if job.UpdatedAt.Add(w.maxUnavailableTime).Before(time.Now()) {
			failedJobIds = append(failedJobIds, job.SessionID)
			w.onJobDeadCb(job.SessionID, "job is unavailable, maybe oom or crashed")
		}
	}
	for _, job := range unAliveJob {
		failedJobIds = append(failedJobIds, job.SessionID)
		w.onJobDeadCb(job.SessionID, "job is not found, maybe timeout or engine crashed")
	}

	err = w.metaManager.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		return txn.SetMultipleSessionStatus(failedJobIds, storage.SessionFailed)
	})
	if err != nil {
		logrus.Warnf("failed to update status in session_infos: %v", err)
	}
}

// probe if the job is still alive
func IsJobAlive(j *storage.SessionInfo) (bool, error) {
	conn, err := NewEngineClientConn(j.EngineUrlForSelf, "", nil)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	client := pb.NewSCQLEngineServiceClient(conn)

	req := pb.QueryJobStatusRequest{
		JobId: j.SessionID,
	}
	resp, err := client.QueryJobStatus(context.TODO(), &req)
	if err != nil {
		return false, err
	}

	if resp.GetStatus().GetCode() != int32(pb.Code_OK) {
		if resp.GetStatus().GetCode() == int32(pb.Code_NOT_FOUND) {
			return false, nil
		}
		return false, fmt.Errorf("rpc QueryJobStatus responses error: %v", resp.GetStatus())
	}
	return true, nil
}

func StartJobWatcherScheduler(w *JobWatcher, interval time.Duration) (func() error, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("fail to create jobwatcher scheduler: %v", err)
	}
	_, err = s.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(w.CheckJobs),
		gocron.WithName("job watcher"),
	)
	if err != nil {
		return nil, fmt.Errorf("fail to schedule job status check task: %v", err)
	}
	s.Start()
	return s.Shutdown, nil
}
