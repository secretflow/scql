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
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type OnJobDeadCb func(jobId string, reason string)

// tracking job lifecycle and progress
// TODO: implement running progress tracking

type JobWatcher struct {
	// time interval of polling job status
	interval time.Duration

	onJobDeadCb OnJobDeadCb

	// mutex
	mu   sync.Mutex
	jobs map[string]*jobStatus

	scheduler gocron.Scheduler
}

type JobWatcherOption func(*JobWatcher)

func WithWatchInterval(t time.Duration) JobWatcherOption {
	return func(w *JobWatcher) {
		w.interval = t
	}
}

func NewJobWatcher(cb OnJobDeadCb, opts ...JobWatcherOption) (*JobWatcher, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("fail to create jobwatcher scheduler: %v", err)
	}

	w := &JobWatcher{
		interval:    time.Minute, // 1m
		onJobDeadCb: cb,
		jobs:        make(map[string]*jobStatus),
		scheduler:   s,
	}

	for _, opt := range opts {
		opt(w)
	}

	err = w.start()
	if err != nil {
		return nil, fmt.Errorf("fail to start scheduled jobs: %v", err)
	}

	return w, nil
}

// Add jobId to watch list
func (w *JobWatcher) Watch(jobId string, engEndpoint string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.jobs[jobId]
	if ok {
		return fmt.Errorf("job %s is already been watched", jobId)
	}
	jobStatus := &jobStatus{
		jobId:          jobId,
		engineEndpoint: engEndpoint,
	}
	w.jobs[jobId] = jobStatus
	return nil
}

// Remove jobId from watch list
func (w *JobWatcher) Unwatch(jobId string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.jobs, jobId)
}

func (w *JobWatcher) onJobDead(jobId string) {
	w.Unwatch(jobId)
	if w.onJobDeadCb != nil {
		w.onJobDeadCb(jobId, "job has failed in engine, likely due to either engine crash or being OOM killed")
	}
}

func (w *JobWatcher) start() error {
	_, err := w.scheduler.NewJob(
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

func (w *JobWatcher) checkJobs() {
	jobs := w.collectJobs()
	for _, job := range jobs {
		alive, err := job.IsJobAlive()
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
				job.inaccessibleTimes.Add(1)
			}
			logrus.Warnf("failed to probe job status: jobId=%s, endpoint=%s, err=%v", job.jobId, job.engineEndpoint, err)
		}

		if job.inaccessibleTimes.Load() >= 3 || (err == nil && !alive) {
			w.onJobDead(job.jobId)
		}
	}
}

func (w *JobWatcher) collectJobs() []*jobStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	res := make([]*jobStatus, 0, len(w.jobs))
	for _, v := range w.jobs {
		res = append(res, v)
	}
	return res
}

// stop all watched jobs
func (w *JobWatcher) Stop() {
	w.scheduler.Shutdown()
}

type jobStatus struct {
	jobId          string
	engineEndpoint string

	// number of engine inaccessible times
	inaccessibleTimes atomic.Int32
}

// probe if the job is still alive
func (j *jobStatus) IsJobAlive() (bool, error) {
	conn, err := NewEngineClientConn(j.engineEndpoint, "", nil)
	if err != nil {
		return false, err
	}
	client := pb.NewSCQLEngineServiceClient(conn)

	req := pb.QueryJobStatusRequest{
		JobId: j.jobId,
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
