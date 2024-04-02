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

package server

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/storage"
	prom "github.com/secretflow/scql/pkg/util/prometheus"
)

const (
	engineCallbackPath = "/cb/engine"
	healthCheckPath    = "/health_check"
	fetchResultPath    = "/fetch_result"
	submitQueryPath    = "/submit_query"
	submitAndGetPath   = "/submit_and_get"
)

type LogFormatter struct {
}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var fileWithLine string
	if entry.HasCaller() {
		fileWithLine = fmt.Sprintf("%s:%d", filepath.Base(entry.Caller.File), entry.Caller.Line)
	} else {
		fileWithLine = ":"
	}
	return []byte(fmt.Sprintf("%s %s %s %s\n", entry.Time.Format("2006-01-02 15:04:05.123"),
		strings.ToUpper(entry.Level.String()), fileWithLine, entry.Message)), nil
}

func NewServer(conf *config.Config, storage *gorm.DB, engineClient executor.EngineClient) (*http.Server, error) {
	app, err := NewApp(conf, storage, engineClient)
	if err != nil {
		return nil, err
	}

	router := gin.New()
	router.Use(ginLogger())
	router.Use(gin.RecoveryWithWriter(logrus.StandardLogger().Out))
	router.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:80"},
		AllowMethods: []string{"GET", "PUT", "POST", "OPTIONS"},
		AllowHeaders: []string{
			"Origin", "Authorization", "Content-Type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           50 * time.Second,
	}))
	router.Use(prom.GetMonitor().Middleware)
	router.GET(prom.MetricsPath, func(ctx *gin.Context) {
		promhttp.Handler().ServeHTTP(ctx.Writer, ctx.Request)
	})

	public := router.Group("/public")
	public.GET(healthCheckPath, app.HealthHandler)
	public.POST(submitQueryPath, app.SubmitHandler)
	public.POST(fetchResultPath, app.FetchHandler)
	public.POST(submitAndGetPath, app.SubmitAndGetHandler)
	// NOTE: path `/cb/*` is for SCQL internal use only, should not publish to end-user.
	router.POST(engineCallbackPath, app.EngineHandler)

	return &http.Server{
		Addr:           fmt.Sprintf(":%v", app.Port()),
		Handler:        router,
		ReadTimeout:    30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}, nil
}

func NewDbConnWithBootstrap(conf *config.StorageConf) (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	gormConfig := &gorm.Config{
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
	default:
		return nil, fmt.Errorf("newApp: invalid config.StorageType %s, should be one of {sqlite,mysql}", conf.Type)
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

	// NOTE: init db only when the db is empty
	if storage.NeedBootstrap(db) {
		if err := storage.Bootstrap(db); err != nil {
			return nil, err
		}
	}
	if err := storage.CheckStorage(db); err != nil {
		return nil, err
	}
	return db, nil
}

// ginLogger creates a gin logger middleware
func ginLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		// Process request
		c.Next()

		end := time.Now()
		latency := end.Sub(start)
		status := c.Writer.Status()

		msg := fmt.Sprintf("|GIN|status=%v|method=%v|path=%v|ip=%v|latency=%v|%s", status, c.Request.Method, path, c.ClientIP(), latency, c.Errors.String())
		if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
			logrus.Warn(msg)
		} else if status >= http.StatusInternalServerError {
			logrus.Error(msg)
		} else {
			logrus.Info(msg)
		}
	}
}
