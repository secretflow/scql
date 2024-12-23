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

package logutil

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

var _ logrus.Formatter = &CustomMonitorFormatter{}

// custom monitor formatter, e.g.: "2020-07-14 16:59:47.7144 INFO main.go:107 |msg"
type CustomMonitorFormatter struct {
	TimestampFormat string
}

func NewCustomMonitorFormatter(timestampFormat string) *CustomMonitorFormatter {
	return &CustomMonitorFormatter{
		TimestampFormat: timestampFormat,
	}
}

func (f *CustomMonitorFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var fileWithLine string
	if entry.HasCaller() {
		fileWithLine = fmt.Sprintf("%s:%d", filepath.Base(entry.Caller.File), entry.Caller.Line)
	} else {
		fileWithLine = ":"
	}
	return []byte(fmt.Sprintf("%s %s %s %s\n", entry.Time.Format(f.TimestampFormat),
		strings.ToUpper(entry.Level.String()), fileWithLine, entry.Message)), nil
}
