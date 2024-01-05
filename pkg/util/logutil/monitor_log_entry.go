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

package logutil

import (
	"fmt"
	"time"
)

// MonitorLogEntry is Log entry struct builder adapter for log-based based system
type MonitorLogEntry struct {
	RequestID  string
	SessionID  string
	ActionName string
	CostTime   time.Duration
	Reason     string
	ErrorMsg   string
	RawRequest string
}

func (b MonitorLogEntry) String() string {
	return fmt.Sprintf("|RequestID:%v|SessionID:%v|ActionName:%v|CostTime:%v|Reason:%v|ErrorMsg:%v|Request:%v",
		b.RequestID, b.SessionID, b.ActionName, b.CostTime, b.Reason, b.ErrorMsg, b.RawRequest)
}

type BrokerMonitorLogEntry struct {
	RequestID    string
	RequestParty string
	JobID        string
	ActionName   string
	CostTime     time.Duration
	Reason       string
	ErrorMsg     string
	RawRequest   string
}

func (b BrokerMonitorLogEntry) String() string {
	return fmt.Sprintf("|RequestID:%v|RequestParty:%v|SessionID:%v|ActionName:%v|CostTime:%v|Reason:%v|ErrorMsg:%v|Request:%v",
		b.RequestID, b.RequestParty, b.JobID, b.ActionName, b.CostTime, b.Reason, b.ErrorMsg, b.RawRequest)
}
