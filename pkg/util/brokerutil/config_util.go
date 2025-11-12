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

// Due to circular references, please avoid moving this file to testutil.
package brokerutil

import (
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func UpdateJobConfig(jobConfig *pb.JobConfig, projectConfig *pb.ProjectConfig) *pb.JobConfig {
	// if the job config is null, assign the default value to it.
	if jobConfig == nil {
		jobConfig = &pb.JobConfig{}
	}

	if projectConfig == nil {
		return jobConfig
	}
	// reflection may look like a more elegant way, but it is more explicit for default value handling,
	// so here not to use the reflection
	if jobConfig.SessionExpireSeconds == 0 {
		jobConfig.SessionExpireSeconds = projectConfig.SessionExpireSeconds
	}

	// psi part
	if jobConfig.PsiCurveType == 0 {
		jobConfig.PsiCurveType = projectConfig.PsiCurveType
	}

	// link part
	if jobConfig.HttpMaxPayloadSize == 0 {
		jobConfig.HttpMaxPayloadSize = projectConfig.HttpMaxPayloadSize
	}

	if jobConfig.LinkRecvTimeoutSec == 0 {
		jobConfig.LinkRecvTimeoutSec = projectConfig.LinkRecvTimeoutSec
	}

	if jobConfig.LinkThrottleWindowSize == 0 {
		jobConfig.LinkThrottleWindowSize = projectConfig.LinkThrottleWindowSize
	}

	if jobConfig.LinkChunkedSendParallelSize == 0 {
		jobConfig.LinkChunkedSendParallelSize = projectConfig.LinkChunkedSendParallelSize
	}

	if jobConfig.Batched == nil {
		jobConfig.Batched = projectConfig.Batched
	}

	if jobConfig.EnableSessionLoggerSeparation == nil {
		jobConfig.EnableSessionLoggerSeparation = projectConfig.EnableSessionLoggerSeparation
	}

	if jobConfig.GetRr22Mode() == scql.Rr22Mode_UNDEFINED {
		jobConfig.Rr22Mode = projectConfig.Rr22Mode
	}

	if jobConfig.PsiType == scql.PsiAlgorithmType_AUTO {
		jobConfig.PsiType = projectConfig.PsiType
	}

	if jobConfig.TimeZone == "" {
		jobConfig.TimeZone = projectConfig.TimeZone
	}

	return jobConfig
}
