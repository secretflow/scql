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

package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/secretflow/scql/contrib/agent"
	"github.com/secretflow/scql/contrib/agent/config"
	"github.com/secretflow/scql/pkg/util/logutil"
)

var version = "agent version"

const (
	LogFileName                 = "logs/agent.log"
	LogOptionMaxSizeInMegaBytes = 500
	LogOptionMaxBackupsCount    = 10
	LogOptionMaxAgeInDays       = 0
	LogOptionCompress           = false
)

func main() {
	confFile := flag.String("config", "./cmd/agent/config.yaml", "Path to agent configuration file")
	showVersion := flag.Bool("version", false, "Print version information")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	logrus.SetReportCaller(true)
	logrus.SetFormatter(logutil.NewCustomMonitorFormatter("2006-01-02 15:04:05.123"))
	rollingLogger := &lumberjack.Logger{
		Filename:   LogFileName,
		MaxSize:    LogOptionMaxSizeInMegaBytes, // megabytes
		MaxBackups: LogOptionMaxBackupsCount,
		MaxAge:     LogOptionMaxAgeInDays, //days
		Compress:   LogOptionCompress,
	}
	mOut := io.MultiWriter(os.Stdout, rollingLogger)
	logrus.SetOutput(mOut)

	logrus.Infof("Read config file: %s", *confFile)
	config, err := config.NewConfig(*confFile)
	if err != nil {
		logrus.Fatalf("Create config failed: %v", err)
	}

	agent := agent.NewAgent(config)
	if err := agent.Run(); err != nil {
		logrus.Fatalf("Run agent failed: %v", err)
	}
	logrus.Infof("Run agent successfully")
}
