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
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/constant"
	"github.com/secretflow/scql/pkg/broker/services/inter"
	"github.com/secretflow/scql/pkg/broker/services/intra"
	prom "github.com/secretflow/scql/pkg/util/prometheus"
)

type Server struct {
}

func NewIntraServer(app *application.App) (*http.Server, error) {
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

	intraSvc := intra.NewIntraSvc(app)
	router.GET(constant.HealthPath, intraSvc.HealthHandler)
	router.POST(constant.DoQueryPath, intraSvc.DoQueryHandler)
	router.POST(constant.SubmitQueryPath, intraSvc.SubmitQueryHandler)
	router.POST(constant.FetchResultPath, intraSvc.FetchResultHandler)
	router.POST(constant.ExplainQueryPath, intraSvc.ExplainQueryHandler)
	router.POST(constant.CancelQueryPath, intraSvc.CancelQueryHandler)
	router.POST(constant.CreateProjectPath, intraSvc.CreateProjectHandler)
	router.POST(constant.ListProjectsPath, intraSvc.ListProjectsHandler)
	router.POST(constant.InviteMemberPath, intraSvc.InviteMemberHandler)
	router.POST(constant.ListInvitationsPath, intraSvc.ListInvitationsHandler)
	router.POST(constant.ProcessInvitationPath, intraSvc.ProcessInvitationHandler)
	router.POST(constant.CreateTablePath, intraSvc.CreateTableHandler)
	router.POST(constant.ListTablesPath, intraSvc.ListTablesHandler)
	router.POST(constant.DropTablePath, intraSvc.DropTableHandler)
	router.POST(constant.GrantCCLPath, intraSvc.GrantCCLHandler)
	router.POST(constant.RevokeCCLPath, intraSvc.RevokeCCLHandler)
	router.POST(constant.ShowCCLPath, intraSvc.ShowCCLHandler)
	router.POST(constant.EngineCallbackPath, intraSvc.EngineCallbackHandler)
	router.POST(constant.CheckAndUpdateStatusPath, intraSvc.CheckAndUpdateStatusHandler)

	return &http.Server{
		Addr:           fmt.Sprintf("%s:%v", app.Conf.IntraServer.Host, app.Conf.IntraServer.Port),
		Handler:        router,
		ReadTimeout:    30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}, nil
}

func NewInterServer(
	app *application.App) (*http.Server, error) {
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

	interSvc := inter.NewInterSvc(app)
	router.POST(constant.InviteToProjectPath, interSvc.InviteToProjectHandler)
	router.POST(constant.ReplyInvitationPath, interSvc.ReplyInvitationHandler)
	router.POST(constant.SyncInfoPath, interSvc.SyncInfoHandler)
	router.POST(constant.AskInfoPath, interSvc.AskInfoHandler)
	router.POST(constant.DistributeQueryPath, interSvc.DistributedQueryHandler)
	router.POST(constant.CancelQueryJobPath, interSvc.CancelQueryJobHandler)
	router.POST(constant.ExchangeJobInfoPath, interSvc.ExchangeJobInfoHandler)

	return &http.Server{
		Addr:           fmt.Sprintf("%s:%v", app.Conf.InterServer.Host, app.Conf.InterServer.Port),
		Handler:        router,
		ReadTimeout:    30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}, nil
}

// ginLogger creates a gin logger middleware
func ginLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		logrus.Infof("GIN | Start handling request method=%s path=%s client=%s", c.Request.Method, path, c.ClientIP())
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
