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

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/server"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/logutil"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultConfigPath = "cmd/scdbserver/config.yml"
)

const (
	LogFileName                 = "logs/scdbserver.log"
	LogOptionMaxSizeInMegaBytes = 500
	LogOptionMaxBackupsCount    = 10
	LogOptionMaxAgeInDays       = 0
	LogOptionCompress           = false
)

var version = "scql version"

func main() {
	confFile := flag.String("config", defaultConfigPath, "Path to scdb server configuration file")
	showVersion := flag.Bool("version", false, "Print version information")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	log.SetReportCaller(true)
	log.SetFormatter(logutil.NewCustomMonitorFormatter("2006-01-02 15:04:05.123"))
	rollingLogger := &lumberjack.Logger{
		Filename:   LogFileName,
		MaxSize:    LogOptionMaxSizeInMegaBytes, // megabytes
		MaxBackups: LogOptionMaxBackupsCount,
		MaxAge:     LogOptionMaxAgeInDays, //days
		Compress:   LogOptionCompress,
	}
	mout := io.MultiWriter(os.Stdout, rollingLogger)
	log.SetOutput(mout)

	gin.SetMode(gin.ReleaseMode)

	log.Infof("SCDB version: %s", version)
	log.Infof("Starting to read config file: %s", *confFile)
	cfg, err := config.NewConfig(*confFile)
	if err != nil {
		log.Fatalf("Failed to create config from %s: %v", *confFile, err)
	}

	// set log level if defined
	if cfg.LogLevel != "" {
		if lvl, err := log.ParseLevel(cfg.LogLevel); err == nil {
			log.SetLevel(lvl)
		}
	}

	log.Info("Starting to connect to database and do bootstrap if necessary...")
	storage.InitPasswordValidation(cfg.PasswordCheck)
	store, err := server.NewDbConnWithBootstrap(&cfg.Storage)
	if err != nil {
		log.Fatalf("Failed to connect to database and bootstrap it: %v", err)
	}

	engineClient := executor.NewEngineClient(cfg.Engine.ClientMode, cfg.Engine.ClientTimeout*time.Second, nil, cfg.Engine.ContentType, cfg.Engine.Protocol)
	svr, err := server.NewServer(cfg, store, engineClient)
	if err != nil {
		log.Fatalf("Failed to create scdb server: %v", err)
	}

	if cfg.Protocol == "https" {
		if cfg.TlsConfig.CertFile == "" || cfg.TlsConfig.KeyFile == "" {
			log.Fatalf("Could't start https service without cert file")
		}
		log.Info("Starting to serve request with https...")
		if err := svr.ListenAndServeTLS(cfg.TlsConfig.CertFile, cfg.TlsConfig.KeyFile); err != nil {
			log.Fatalf("Something bad happens to server: %v", err)
		}
	} else {
		log.Info("Starting to serve request with http...")
		if err := svr.ListenAndServe(); err != nil {
			log.Fatalf("Something bad happens to server: %v", err)
		}
	}
}
