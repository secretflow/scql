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
	"net/http"
	"os"
	"strings"
	"time"

	gormlog "gorm.io/gorm/logger"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/config"
	"github.com/secretflow/scql/pkg/broker/partymgr"
	"github.com/secretflow/scql/pkg/broker/server"
	"github.com/secretflow/scql/pkg/broker/services/auth"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/util/kusciaclient"
	"github.com/secretflow/scql/pkg/util/logutil"
)

const (
	defaultConfigPath = "cmd/broker/config.yml"
)

const (
	LogFileName                 = "logs/broker.log"
	LogOptionMaxSizeInMegaBytes = 500
	LogOptionMaxBackupsCount    = 10
	LogOptionMaxAgeInDays       = 0
	LogOptionCompress           = false
)

var version = "scql version"

func main() {
	confFile := flag.String("config", defaultConfigPath, "Path to broker configuration file")
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

	logrus.Infof("Broker version: %s", version)
	logrus.Infof("Starting to read config file: %s", *confFile)
	cfg, err := config.NewConfig(*confFile)
	if err != nil {
		logrus.Fatalf("Failed to create config from %s: %v", *confFile, err)
	}

	// set log level if defined
	if cfg.LogLevel != "" {
		if lvl, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
			logrus.SetLevel(lvl)
		}
	}

	var partyMgr partymgr.PartyMgr

	switch strings.ToLower(cfg.Discovery.Type) {
	case "", "file":
		path := cfg.Discovery.File
		if len(path) == 0 {
			path = cfg.PartyInfoFile
		}
		partyMgr, err = partymgr.NewFilePartyMgr(path)
		if err != nil {
			logrus.Fatalf("Failed to create file partyMgr: %v", err)
		}
	case "consul":
		auth, err := auth.NewAuth(cfg)
		if err != nil {
			logrus.Fatalf("Failed to create auth from config: %v", err)
		}
		pubKey, err := auth.GetPubKey()
		if err != nil {
			logrus.Fatalf("Failed to get pubkey from auth: %v", err)
		}
		partyMgr, err = partymgr.NewConsulPartyMgr(cfg, pubKey)
		if err != nil {
			logrus.Fatalf("Failed to create consul partyMgr: %v", err)
		}
	case "kuscia":
		if cfg.Discovery.Kuscia == nil {
			logrus.Fatal("Missing kuscia discovery config while discovery type is kuscia")
		}
		kuscia := cfg.Discovery.Kuscia
		conn, err := kusciaclient.NewKusciaClientConn(kuscia.Endpoint, kuscia.TLSMode, kuscia.Cert, kuscia.Key, kuscia.CaCert, kuscia.Token)
		if err != nil {
			logrus.Fatalf("Failed to create kuscia client connection: %v", err)
		}
		partyMgr, err = partymgr.NewKusciaPartyMgr(conn, kuscia.ClusterDefine)
		if err != nil {
			logrus.Fatalf("Failed to create kuscia partyMgr: %v", err)
		}
	default:
		logrus.Fatalf("unsupported discovery type: %s", cfg.Discovery.Type)
	}

	gin.SetMode(gin.ReleaseMode)

	db, err := newDb(&cfg.Storage)
	if err != nil {
		logrus.Fatalf("Failed to create broker db: %v", err)
	}

	metaMgr := storage.NewMetaManager(db)
	if metaMgr.NeedBootstrap() {
		logrus.Info("Start to bootstrap meta manager...")
		err = metaMgr.Bootstrap()
		if err != nil {
			logrus.Fatalf("Failed to boot strap meta manager: %v", err)
		}
	}

	if err := storage.CheckStorage(db); err != nil {
		logrus.Fatalf("Failed to check storage: %v", err)
	}

	app, err := application.NewApp(partyMgr, metaMgr, cfg)
	if err != nil {
		logrus.Fatalf("Failed to create app: %v", err)
	}

	intraSvr, err := server.NewIntraServer(app)
	if err != nil {
		logrus.Fatalf("Failed to create broker intra server: %v", err)
	}

	interSvr, err := server.NewInterServer(app)
	if err != nil {
		logrus.Fatalf("Failed to create broker inter server: %v", err)
	}

	go startService(intraSvr, cfg.IntraServer)

	startService(interSvr, cfg.InterServer)
}

func startService(svr *http.Server, cfg config.ServerConfig) {
	if cfg.Protocol == "https" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			logrus.Fatal("Could't start https service without cert_file or key_file")
		}
		logrus.Infof("Starting to serve request on %v with https...", svr.Addr)
		if err := svr.ListenAndServeTLS(cfg.CertFile, cfg.KeyFile); err != nil {
			logrus.Fatalf("Server with tls err: %v", err)
		}
		return
	} else {
		// default http
		logrus.Infof("Starting to serve request on %v with http...", svr.Addr)
		if err := svr.ListenAndServe(); err != nil {
			logrus.Fatalf("Server err: %v", err)
		}
	}

}

func newDb(conf *config.StorageConf) (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	gormConfig := &gorm.Config{
		SkipDefaultTransaction: true,
		// Reference gormlog.Default
		Logger: gormlog.New(
			logrus.StandardLogger(),
			gormlog.Config{
				SlowThreshold: 200 * time.Millisecond,
				Colorful:      false,
				LogLevel:      gormlog.Warn,
			}),
	}

	switch conf.Type {
	case config.StorageTypeSQLite:
		db, err = gorm.Open(sqlite.Open(conf.ConnStr), gormConfig)
	case config.StorageTypeMySQL:
		db, err = gorm.Open(mysql.Open(conf.ConnStr), gormConfig)
	case config.StorageTypePostgres:
		db, err = gorm.Open(postgres.Open(conf.ConnStr), gormConfig)
	default:
		return nil, fmt.Errorf("newDb: invalid config.StorageType %s, should be one of {sqlite, mysql, postgres}", conf.Type)
	}
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
	sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
	sqlDB.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)

	return db, nil
}
