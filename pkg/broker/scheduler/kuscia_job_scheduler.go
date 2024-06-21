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

package scheduler

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kuscia "github.com/secretflow/kuscia/pkg/crd/apis/kuscia/v1alpha1"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	kusciaTokenHeader        = "token"
	engineDeployTemplateName = "engine"
	enginePort               = "engineport"
	linkPort                 = "linkport" // linkport is port of link service, used for peer
)

type kusciaEnginePod struct {
	kusciaClient kusciaapi.JobServiceClient
	// exports fields for marshaling
	// query job id
	JobId string `json:"job_id"`
	// kuscia job id
	KusciaJobID string `json:"kuscia_job_id"`
	// kuscia task id
	KusciaTaskID string `json:"kuscia_task_id"`
	// engine endpoint
	Endpoint        string `json:"endpoint"`
	EndpointForPeer string `json:"endpoint_for_peer"`

	KeepAliveForDebug bool `json:"keep_alive_for_debug"`
}

type kusciaJobScheduler struct {
	client          kusciaapi.JobServiceClient
	selfPartyID     string
	appImage        string
	taskInputConfig string

	// add jobIdSuffix to avoid kuscia job id conflicts in kuscia master-lite mode.
	jobIdSuffix string

	maxPollTimes int // max number to poll QueryJob API
	pollInterval time.Duration
	// wait job to become running
	maxWaitTime time.Duration

	keepJobAliveForDebug bool
}

type KusciaJobSchedulerOption func(*kusciaJobScheduler)

func WithAppImage(image string) KusciaJobSchedulerOption {
	return func(k *kusciaJobScheduler) {
		k.appImage = image
	}
}

func WithMaxPollTimes(times int) KusciaJobSchedulerOption {
	return func(k *kusciaJobScheduler) {
		k.maxPollTimes = times
	}
}

func WithMaxWaitTime(t time.Duration) KusciaJobSchedulerOption {
	return func(k *kusciaJobScheduler) {
		k.maxWaitTime = t
	}
}

func WithPollInterval(t time.Duration) KusciaJobSchedulerOption {
	return func(k *kusciaJobScheduler) {
		k.pollInterval = t
	}
}

func WithKeepJobAliveForDebug(b bool) KusciaJobSchedulerOption {
	return func(k *kusciaJobScheduler) {
		k.keepJobAliveForDebug = b
	}
}

// NewKusciaJobScheduler create KusciaJobScheduler with grpc client connection
func NewKusciaJobScheduler(conn grpc.ClientConnInterface, selfPartyID string, options ...KusciaJobSchedulerOption) (*kusciaJobScheduler, error) {
	client := kusciaapi.NewJobServiceClient(conn)

	hs := md5.Sum([]byte(selfPartyID))
	jobIdSuffix := hex.EncodeToString(hs[:4])

	scheduler := &kusciaJobScheduler{
		client:               client,
		selfPartyID:          selfPartyID,
		jobIdSuffix:          jobIdSuffix,
		appImage:             "scql",
		maxPollTimes:         20,
		pollInterval:         time.Second * 3,
		maxWaitTime:          time.Minute,
		keepJobAliveForDebug: false,
	}

	for _, opt := range options {
		opt(scheduler)
	}

	return scheduler, nil
}

func (s *kusciaJobScheduler) ParseEngineInstance(jobInfo []byte) (EngineInstance, error) {
	var pod kusciaEnginePod
	err := json.Unmarshal(jobInfo, &pod)
	pod.kusciaClient = s.client // client is not in job info
	return &pod, err
}

func (s *kusciaJobScheduler) Schedule(jobID string) (EngineInstance, error) {
	var kusciaJobID string
	// create kuscia job
	{

		kusciaJobID = fmt.Sprintf("%s-%s", jobID, s.jobIdSuffix)
		req := s.buildCreateJobRequest(kusciaJobID)
		resp, err := s.client.CreateJob(context.Background(), req)
		if err != nil {
			return nil, err
		}
		if resp.GetStatus().GetCode() != 0 {
			return nil, fmt.Errorf("failed to create kuscia job, code=%d, message=%s", resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
		}
		// kuscia may overwrite job id
		kusciaJobID = resp.GetData().GetJobId()
	}
	// polling kuscia job status
	pod, err := s.waitJobRunningAndGetEnginePod(kusciaJobID)
	if err != nil {
		if !s.keepJobAliveForDebug {
			logrus.Infof("failed to wait for kuscia job %s tobe running, it will be deleted...", kusciaJobID)
			if err := deleteKusciaJob(context.TODO(), s.client, kusciaJobID); err != nil {
				logrus.Warnf("failed to delete kuscia job %s: %v", kusciaJobID, err)
			}
		}
		return nil, fmt.Errorf("failed to wait for kuscia job %s tobe running", kusciaJobID)
	}
	pod.JobId = jobID
	logrus.Debugf("pod endpoint_for_self: %v, endpoint_for_peer: %v", pod.Endpoint, pod.EndpointForPeer)
	return pod, nil
}

func (s *kusciaJobScheduler) waitJobRunningAndGetEnginePod(kusciaJobID string) (*kusciaEnginePod, error) {
	req := s.buildQueryJobRequest(kusciaJobID)

	var jobStatus string
	timer := time.NewTimer(s.maxWaitTime)
	// NOTE: First set ticker to very small duration, let the first query job request be sent immediately
	ticker := time.NewTicker(time.Nanosecond)
	defer ticker.Stop()
	for i := 0; i < s.maxPollTimes; i++ {
		select {
		case <-timer.C:
			return nil, fmt.Errorf("timeout to wait kuscia job %s running, job status %s", kusciaJobID, jobStatus)
		case <-ticker.C:
			resp, err := s.client.QueryJob(context.Background(), req)
			if err != nil {
				return nil, fmt.Errorf("invoke QueryJob rpc error: %w", err)
			}
			logrus.Debugf("QueryJob Response: %v", resp)
			var ready bool
			ready, jobStatus, err = readyToGetEndpoint(resp)
			if err != nil {
				return nil, fmt.Errorf("kuscia job %s error: %w", kusciaJobID, err)
			}
			if !ready {
				ticker.Reset(s.pollInterval)
				continue
			}
			pod := &kusciaEnginePod{
				kusciaClient:      s.client,
				KusciaJobID:       kusciaJobID,
				KusciaTaskID:      resp.GetData().GetStatus().GetTasks()[0].GetTaskId(),
				KeepAliveForDebug: s.keepJobAliveForDebug,
			}

			found := false
			for _, ep := range resp.GetData().GetStatus().GetTasks()[0].GetParties()[0].GetEndpoints() {
				if ep.GetPortName() == enginePort {
					found = true
					pod.Endpoint = ep.GetEndpoint()
				} else if ep.GetPortName() == linkPort {
					pod.EndpointForPeer = ep.GetEndpoint()
				}
			}
			if !found {
				return nil, fmt.Errorf("endpoint not found for port %s in kuscia task %s", enginePort, kusciaJobID)
			}
			logrus.Debugf("scheduled pod: %+v, resp=%+v", pod, resp.String())
			return pod, nil
		}
	}

	return nil, fmt.Errorf("timeout to wait kuscia job %s running, exceed max poll times, job status %s", kusciaJobID, jobStatus)
}

// If kuscia job/task is ready, it returns (true, nil), otherwise returns (false, nil)
// It will return (false, error) if job failed
func readyToGetEndpoint(resp *kusciaapi.QueryJobResponse) (bool, string, error) {
	if resp.GetStatus().GetCode() != 0 {
		return false, "", fmt.Errorf("failed to query job: code=%d, message=%s", resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
	}
	switch strings.ToLower(resp.GetData().GetStatus().GetState()) {
	case strings.ToLower(string(kuscia.KusciaJobPending)):
		return false, "", nil
	case strings.ToLower(string(kuscia.KusciaJobFailed)):
		// NOTE: Maybe in next kuscia version, we can get errMsg directly.
		errMsg := resp.GetData().GetStatus().GetErrMsg()
		if len(errMsg) == 0 && len(resp.GetData().GetStatus().GetTasks()) == 1 && len(resp.GetData().GetStatus().GetTasks()[0].GetParties()) == 1 {
			errMsg = resp.GetData().GetStatus().GetTasks()[0].GetParties()[0].GetErrMsg()
		}
		return false, "", fmt.Errorf("kuscia job failed, err_msg=%s", errMsg)
	case strings.ToLower(string(kuscia.KusciaJobSucceeded)):
		return false, "", fmt.Errorf("unexpected job status, it already finished")
	case strings.ToLower(string(kuscia.KusciaJobRunning)):
		{
			// get task status
			// NOTE: we expect that there is only one task in a job since we only request one task in CreateJob request
			// wait until there is one task in status.tasks
			if len(resp.GetData().GetStatus().GetTasks()) < 1 {
				return false, "", nil
			}

			taskState := resp.GetData().GetStatus().GetTasks()[0].GetState()
			switch strings.ToLower(taskState) {
			case strings.ToLower(string(kuscia.TaskPending)):
				if len(resp.GetData().GetStatus().GetTasks()[0].GetParties()) < 1 {
					return false, taskState, nil
				}
				jobStatus := taskState + ", err_msg=" + resp.GetData().GetStatus().GetTasks()[0].GetParties()[0].GetErrMsg()
				return false, jobStatus, nil
			case strings.ToLower(string(kuscia.TaskRunning)):
				return true, taskState, nil
			default:
				return false, taskState, fmt.Errorf("unexpected task state: %s", taskState)
			}
		}
	default:
		return false, "", fmt.Errorf("unknown job state: %s", resp.GetData().GetStatus().GetState())
	}
}

func (s *kusciaJobScheduler) buildCreateJobRequest(jobID string) *kusciaapi.CreateJobRequest {
	req := &kusciaapi.CreateJobRequest{
		JobId:          jobID,
		Initiator:      s.selfPartyID,
		MaxParallelism: 1,
		Tasks: []*kusciaapi.Task{
			{
				AppImage: s.appImage,
				Alias:    "Start-SCQLEngine",
				// let task id empty, kuscia task controller will generate unique task id for us.
				TaskId: "",
				Parties: []*kusciaapi.Party{
					{
						DomainId: s.selfPartyID,
						Role:     engineDeployTemplateName,
					},
				},
				TaskInputConfig: s.taskInputConfig,
			},
		},
	}
	return req
}

func (s *kusciaJobScheduler) buildQueryJobRequest(jobID string) *kusciaapi.QueryJobRequest {
	req := &kusciaapi.QueryJobRequest{
		JobId: jobID,
	}
	return req
}

func (p *kusciaEnginePod) GetEndpointForPeer() string {
	if len(p.EndpointForPeer) > 0 {
		return p.EndpointForPeer
	}
	return p.Endpoint
}

func (p *kusciaEnginePod) GetEndpointForSelf() string {
	return p.Endpoint
}

func (p *kusciaEnginePod) MarshalToText() ([]byte, error) {
	return json.Marshal(p)
}

func (p *kusciaEnginePod) Stop() error {
	if p.KeepAliveForDebug {
		logrus.Warnf("kuscia job %s will not be stopped for debugging purpose, please stop it manually", p.KusciaJobID)
		return nil
	}
	logrus.Infof("kuscia job %s will be deleted...", p.KusciaJobID)
	// NOTE: kuscia stop job request will not release/clean job resources(pods, services...)
	// So we use delete job here
	return deleteKusciaJob(context.TODO(), p.kusciaClient, p.KusciaJobID)
}

func deleteKusciaJob(ctx context.Context, client kusciaapi.JobServiceClient, jobID string) error {
	req := &kusciaapi.DeleteJobRequest{
		JobId: jobID,
	}
	resp, err := client.DeleteJob(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("failed to delete kuscia job: code=%d, msg=%s", resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
	}
	return nil
}
