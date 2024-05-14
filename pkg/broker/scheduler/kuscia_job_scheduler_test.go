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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKusciaScheduler(t *testing.T) {
	r := require.New(t)
	// test pod marshal unmarshal
	pod := kusciaEnginePod{
		KusciaJobID:       "kuscia_job_id",
		KusciaTaskID:      "kuscia_task_id",
		Endpoint:          "endpoint",
		KeepAliveForDebug: true,
	}
	jobInfo, err := pod.MarshalToText()
	r.NoError(err)

	scheduler := &kusciaJobScheduler{}
	newPod, err := scheduler.ParseEngineInstance(jobInfo)
	r.NoError(err)
	r.Equal(&pod, newPod)

}
