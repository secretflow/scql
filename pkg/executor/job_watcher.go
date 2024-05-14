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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sirupsen/logrus"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type OnJobDeadCb func(jobId string, reason string)

// tracking job lifecycle and progress
// TODO: implement running progress tracking

type JobWatcher struct {
	// time interval of polling job status
	interval time.Duration

	stopped chan struct{}

	onJobDeadCb OnJobDeadCb

	// mutex
	mu   sync.Mutex
	jobs map[string]*jobStatus
}

type JobWatcherOption func(*JobWatcher)

func WithWatchInterval(t time.Duration) JobWatcherOption {
	return func(w *JobWatcher) {
		w.interval = t
	}
}

func NewJobWatcher(cb OnJobDeadCb, opts ...JobWatcherOption) *JobWatcher {
	w := &JobWatcher{
		interval:    time.Minute, // 1m
		stopped:     make(chan struct{}),
		onJobDeadCb: cb,
		jobs:        make(map[string]*jobStatus),
	}

	for _, opt := range opts {
		opt(w)
	}

	go w.run()
	return w
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

func (w *JobWatcher) run() {
LOOP:
	for {
		select {
		case <-w.stopped:
			break LOOP
		case <-time.After(w.interval):
			{
				jobs := w.collectJobs()
				for _, job := range jobs {
					alive, err := job.IsJobAlive()
					if err != nil {
						if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
							job.inaccessibleTimes += 1
						}
						logrus.Warnf("failed to probe job status: jobId=%s, err=%v", job.jobId, err)
					}

					if job.inaccessibleTimes >= 3 || (err == nil && !alive) {
						w.onJobDead(job.jobId)
					}
				}
			}
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

// stop watcher thread
func (w *JobWatcher) Stop() {
	close(w.stopped)
}

type jobStatus struct {
	jobId          string
	engineEndpoint string

	// number of engine inaccessible times
	inaccessibleTimes int32
}

// probe if the job is still alive
func (j *jobStatus) IsJobAlive() (bool, error) {
	conn, err := NewEngineClientConn(j.engineEndpoint, "")
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
