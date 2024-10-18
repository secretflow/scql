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

type OnJobDeadCb func(jobId, url, reason string)

// tracking job lifecycle and progress
// TODO: implement running progress tracking

type JobWatcher struct {
	// time interval of polling job status
	interval time.Duration

	onJobDeadCb OnJobDeadCb

	scheduler gocron.Scheduler

	elector *storage.DistributedElector

	metaManager *storage.MetaManager

	mu sync.Mutex

	jobStatus map[string]*storage.WatchedJobStatus
}

type JobWatcherOption func(*JobWatcher)

func WithWatchInterval(t time.Duration) JobWatcherOption {
	return func(w *JobWatcher) {
		w.interval = t
	}
}

func NewJobWatcher(metaManager *storage.MetaManager, partyCode string, cb OnJobDeadCb, opts ...JobWatcherOption) (*JobWatcher, error) {
	w := &JobWatcher{
		interval:    time.Minute, // 1m
		onJobDeadCb: cb,
		metaManager: metaManager,
		jobStatus:   make(map[string]*storage.WatchedJobStatus),
	}

	for _, opt := range opts {
		opt(w)
	}

	err := w.start(partyCode)
	if err != nil {
		return nil, fmt.Errorf("fail to start scheduled jobs: %v", err)
	}

	return w, nil
}

// Add jobId to watch list
func (w *JobWatcher) Watch(jobId, engEndpoint string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, ok := w.jobStatus[jobId]
	if ok {
		return fmt.Errorf("job %s is already been watched", jobId)
	}

	jobStatus := &storage.WatchedJobStatus{
		JobId:             jobId,
		EngineEndpoint:    engEndpoint,
		InaccessibleTimes: 0,
	}
	w.jobStatus[jobId] = jobStatus

	return nil
}

// Remove jobId from watch list
func (w *JobWatcher) Unwatch(jobId string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.jobStatus, jobId)
	return nil
}

func (w *JobWatcher) onJobDead(jobId, url, reason string) error {
	err := w.Unwatch(jobId)
	if err != nil {
		return err
	}

	if w.onJobDeadCb != nil {
		w.onJobDeadCb(jobId, url, reason)
	}

	if w.metaManager.Persistent() {
		return w.metaManager.SetSessionStatus(jobId, storage.SessionFailed)
	}

	return nil
}

func (w *JobWatcher) start(partyCode string) error {
	var s gocron.Scheduler
	var err error

	if w.metaManager.Persistent() {
		w.elector, err = storage.NewDistributedElector(w.metaManager, storage.JobWatcherLockID, partyCode, w.interval)
		if err != nil {
			return fmt.Errorf("fail to create distributed elector: %v", err)
		}
		s, err = gocron.NewScheduler(gocron.WithDistributedElector(w.elector))
	} else {
		s, err = gocron.NewScheduler()
	}
	if err != nil {
		return fmt.Errorf("fail to create jobwatcher scheduler: %v", err)
	}

	w.scheduler = s
	_, err = w.scheduler.NewJob(
		gocron.DurationJob(w.interval),
		gocron.NewTask(w.checkJobs),
		gocron.WithName("checkJobs"),
	)
	if err != nil {
		return fmt.Errorf("fail to schedule job status check task: %v", err)
	}

	w.scheduler.Start()
	return nil
}

func (w *JobWatcher) collectJobs() map[string]*storage.WatchedJobStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	// update jobStatus from db first
	if w.metaManager.Persistent() {
		jobs, err := w.metaManager.GetWatchedJobs()
		if err != nil {
			logrus.Warningf("JobWatcher collectJobs error: %v", err)
		}
		for _, job := range jobs {
			_, ok := w.jobStatus[job.JobId]
			if !ok {
				w.jobStatus[job.JobId] = &job
			}
		}

		jobIDSet := make(map[string]struct{})
		for _, job := range jobs {
			jobIDSet[job.JobId] = struct{}{}
		}
		// remove keys not in jobIDSet, their status is no longer running
		for jobId := range w.jobStatus {
			if _, exists := jobIDSet[jobId]; !exists {
				delete(w.jobStatus, jobId)
			}
		}
	}

	res := make(map[string]*storage.WatchedJobStatus)
	for _, v := range w.jobStatus {
		res[v.JobId] = v
	}
	return res
}

func (w *JobWatcher) checkJobs() {
	jobs := w.collectJobs()

	failedJobReason := make(map[string]string)
	modifiedJobStatus := make(map[string]*storage.WatchedJobStatus)
	for _, job := range jobs {
		alive, err := IsJobAlive(job)
		logrus.Infof("job id: %v, inaccess: %v", job.JobId, job.InaccessibleTimes)
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
				job.InaccessibleTimes++
				modifiedJobStatus[job.JobId] = job
			}
			logrus.Warnf("failed to probe job status: jobId=%s, endpoint=%s, err=%v", job.JobId, job.EngineEndpoint, err)
		} else {
			if job.InaccessibleTimes != 0 {
				job.InaccessibleTimes = 0
				modifiedJobStatus[job.JobId] = job
			}
		}

		if job.InaccessibleTimes >= 3 {
			failedJobReason[job.JobId] = "job has failed in engine, likely due to either engine crash or being OOM killed"
			delete(modifiedJobStatus, job.JobId)
		} else if err == nil && !alive {
			failedJobReason[job.JobId] = "session not found, maybe removed by engine due to session timeout or engine restarted"
			delete(modifiedJobStatus, job.JobId)
		}
	}

	var failedJobIds []string
	// check again, some jobs may already have their status set to finished.
	// TODO:
	// There are still problems with clustering information out of sync, e.g. after collectingJobs, the status of a task changes to finished.
	currentRunningJobs := w.collectJobs()
	for jobId, reason := range failedJobReason {
		// whether job still in running
		_, ok := currentRunningJobs[jobId]
		if ok {
			failedJobIds = append(failedJobIds, jobId)
			w.onJobDead(jobId, jobs[jobId].EngineEndpoint, reason)
		}
	}

	if w.metaManager.Persistent() {
		err := w.metaManager.SetMultipleSessionStatus(failedJobIds, storage.SessionFailed)
		if err != nil {
			logrus.Warnf("failed to update status in session_infos: %v", err)
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, job := range modifiedJobStatus {
		_, ok := w.jobStatus[job.JobId]
		if ok {
			w.jobStatus[job.JobId].InaccessibleTimes = job.InaccessibleTimes
		}
	}
}

// stop all watched jobs
func (w *JobWatcher) Stop() {
	if w.metaManager.Persistent() {
		w.elector.StopRenewLeaderLock()
	}
	w.scheduler.Shutdown()
}

// probe if the job is still alive
func IsJobAlive(j *storage.WatchedJobStatus) (bool, error) {
	conn, err := NewEngineClientConn(j.EngineEndpoint, "", nil)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	client := pb.NewSCQLEngineServiceClient(conn)

	req := pb.QueryJobStatusRequest{
		JobId: j.JobId,
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
