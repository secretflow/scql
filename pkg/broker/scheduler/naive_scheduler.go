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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/secretflow/scql/pkg/broker/config"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

const (
	stopJobPath = "/SCQLEngineService/StopJob"
)

// naiveScheduler schedule job to resident engine services. It use round-robin algorithm to pick a engine for query job
type naiveScheduler struct {
	engine config.EngineConfig

	round atomic.Uint64
}

type residentEngine struct {
	SelfUri string `json:"self_uri"`
	PeerUri string `json:"peer_uri"`
	JobID   string `json:"job_id"`

	postUri       string        // no need to marshal to text
	clientTimeout time.Duration // no need to marshal to text
}

func NewNaiveScheduler(engine config.EngineConfig) (*naiveScheduler, error) {
	if len(engine.Uris) == 0 {
		return nil, fmt.Errorf("invalid arguments: uris empty")
	}
	return &naiveScheduler{
		engine: engine,
	}, nil
}

func (s *naiveScheduler) Schedule(jobID string) (EngineInstance, error) {
	if len(s.engine.Uris) == 0 {
		return nil, fmt.Errorf("uris empty")
	}

	r := s.round.Add(1)

	idx := (r - 1) % uint64(len(s.engine.Uris))

	selfUri := s.engine.Uris[idx].ForSelf
	if selfUri == "" {
		// if self uri is empty, we should use peer uri instead
		selfUri = s.engine.Uris[idx].ForPeer
	}
	url := url.URL{
		Scheme: s.engine.Protocol,
		Host:   selfUri,
		Path:   stopJobPath,
	}

	return &residentEngine{
		SelfUri:       selfUri,
		PeerUri:       s.engine.Uris[idx].ForPeer,
		JobID:         jobID,
		postUri:       url.String(),
		clientTimeout: s.engine.ClientTimeout,
	}, nil
}

func (s *naiveScheduler) ParseEngineInstance(jobInfo []byte) (EngineInstance, error) {
	var engine residentEngine
	err := json.Unmarshal(jobInfo, &engine)
	if err != nil {
		return nil, err
	}
	url := url.URL{
		Scheme: s.engine.Protocol,
		Host:   engine.SelfUri,
		Path:   stopJobPath,
	}
	engine.postUri = url.String()
	engine.clientTimeout = s.engine.ClientTimeout
	return &engine, err
}

func (eng residentEngine) GetEndpointForSelf() string {
	return eng.SelfUri
}

func (eng residentEngine) GetEndpointForPeer() string {
	return eng.PeerUri
}

func (eng residentEngine) MarshalToText() ([]byte, error) {
	return json.Marshal(eng)
}

func (eng residentEngine) Stop() error {
	httpClient := &http.Client{Timeout: eng.clientTimeout}
	reqStr := fmt.Sprintf(`{"job_id": "%s"}`, eng.JobID)
	resp, err := httpClient.Post(eng.postUri,
		"application/json", strings.NewReader(reqStr))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stop failed, http status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	var response pb.Status
	_, err = message.DeserializeFrom(resp.Body, &response)
	if err != nil {
		return err
	}
	if response.Code != int32(pb.Code_OK) {
		return fmt.Errorf("stop failed, response: %s", response.String())
	}

	return nil
}
