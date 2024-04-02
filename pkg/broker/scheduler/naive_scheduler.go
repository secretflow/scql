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
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/config"
)

// naiveScheduler schedule job to resident engine services. It use round-robin algorithm to pick a engine for query job
type naiveScheduler struct {
	engines []config.EngineUri

	round atomic.Uint64
}

type residentEngine struct {
	Uri   config.EngineUri `json:"uri"`
	JobID string           `json:"job_id"`
}

func NewNaiveScheduler(engines []config.EngineUri) (*naiveScheduler, error) {
	if len(engines) == 0 {
		return nil, fmt.Errorf("invalid arguments: engines is empty")
	}
	return &naiveScheduler{
		engines: engines,
	}, nil
}

func (s *naiveScheduler) Schedule(jobID string) (EngineInstance, error) {
	if len(s.engines) == 0 {
		return nil, fmt.Errorf("engines is empty")
	}

	r := s.round.Add(1)

	idx := (r - 1) % uint64(len(s.engines))
	engineURI := s.engines[idx]

	return &residentEngine{
		Uri:   engineURI,
		JobID: jobID,
	}, nil
}

func (s *naiveScheduler) ParseEngineInstance(jobInfo []byte) (EngineInstance, error) {
	var engine residentEngine
	err := json.Unmarshal(jobInfo, &engine)
	return &engine, err
}

func (eng residentEngine) GetEndpointForSelf() string {
	if eng.Uri.ForSelf == "" {
		return eng.Uri.ForPeer
	}
	return eng.Uri.ForSelf
}

func (eng residentEngine) GetEndpointForPeer() string {
	return eng.Uri.ForPeer
}

func (eng residentEngine) MarshalToText() ([]byte, error) {
	return json.Marshal(eng)
}

func (eng residentEngine) Stop() error {
	// TODO: issue stop session rpc to engine
	logrus.Warn("residentEngine not support yet, just ignore and return")
	return nil
}
