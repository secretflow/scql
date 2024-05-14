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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJobWatcher(t *testing.T) {
	r := require.New(t)

	jobDead := false
	cb := func(jobID string, reason string) {
		jobDead = true
	}

	w := NewJobWatcher(cb, WithWatchInterval(time.Second))
	defer w.Stop()

	port, err := getFreePort()
	r.NoError(err)
	unreachableAddr := fmt.Sprintf("127.0.0.1:%d", port)
	err = w.Watch("test-job-id", unreachableAddr)
	r.NoError(err)
	// sleep 5s to wait query job been mark dead, since 5s > 1s*3
	time.Sleep(time.Second * 5)
	r.True(jobDead)
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
