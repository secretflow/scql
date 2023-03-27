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
	"testing"
	"time"
)

func TestToString(t *testing.T) {
	// GIVEN
	b := MonitorLogEntry{
		RequestID:  "id",
		SessionID:  "session",
		ActionName: "ac",
		CostTime:   1 * time.Millisecond,
		Reason:     "None",
		ErrorMsg:   "msg",
		RawRequest: "xxx",
	}
	// WHEN
	actual := fmt.Sprint(b)
	expected := "|RequestID:id|SessionID:session|ActionName:ac|CostTime:1ms|Reason:None|ErrorMsg:msg|Request:xxx"
	// THEN
	if expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
