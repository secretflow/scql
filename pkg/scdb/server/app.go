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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/auth"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/logutil"
)

const (
	defaultCallbackQueueSize = 10
)

type App struct {
	config             *config.Config
	engineClient       executor.EngineClient
	storage            *gorm.DB
	sessions           *cache.Cache
	queryDoneChan      chan string
	partyAuthenticator *auth.PartyAuthenticator
}

type LogicalPlanInfo struct {
	lp          core.LogicalPlan
	issuer      string
	engineInfos *graph.EnginesInfo
	ccls        []*scql.SecurityConfig_ColumnControl
}

type ExecutionPlanInfo struct {
	parties []*scql.JobStartParams_Party
	graph   *graph.Graph
	attr    *graph.Attribute
}

func NewApp(conf *config.Config, storage *gorm.DB, engineClient executor.EngineClient) (*App, error) {
	app := &App{
		config:  conf,
		storage: storage,
		sessions: cache.New(time.Duration(conf.SessionExpireTime),
			time.Duration(conf.SessionCheckInterval)),
		queryDoneChan: make(chan string, defaultCallbackQueueSize),
	}
	app.startQueryJobFinishHandler()
	app.engineClient = engineClient
	app.partyAuthenticator = auth.NewPartyAuthenticator(conf.PartyAuth)
	return app, nil
}

func (app *App) Port() int {
	return app.config.Port
}

func (app *App) HealthHandler(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

func (app *App) notifyQueryJobDone(sid string) {
	go func() {
		app.queryDoneChan <- sid
	}()
}

// startQueryJobFinishHandler starts a background goroutine to handle finished query job
func (app *App) startQueryJobFinishHandler() {
	go func() {
		for {
			// quit the forloop when server is about to shutdown
			select {
			case sid := <-app.queryDoneChan:
				app.onQueryJobDone(sid)
			}
		}
	}()
}

func (app *App) getSession(sid string) (*session, bool) {
	value, exists := app.sessions.Get(sid)
	if !exists {
		return nil, false
	}
	if result, ok := value.(*session); ok {
		return result, exists
	}
	return nil, false
}

func (app *App) onQueryJobDone(sid string) {
	session, exists := app.getSession(sid)
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  sid,
		ActionName: constant.ActionNameSCDBQueryJobDone,
	}
	if !exists {
		logEntry.Reason = constant.ReasonSessionNotFound
		logEntry.ErrorMsg = fmt.Sprintf("App.onQueryJobDone: sessionID=%s not found in sessions", sid)
		logrus.Error(logEntry)
		return
	}

	logEntry.RequestID = session.request.BizRequestId
	logEntry.CostTime = time.Since(session.createdAt)
	if session.queryResultCbURL == "" {
		logEntry.ErrorMsg = "Session queryResultCbURL is empty"
		logrus.Warn(logEntry)
		return
	}

	timeout := time.Duration(app.config.QueryResultCbTimeout) * time.Millisecond
	if err := invokeQueryResultCb(session.queryResultCbURL, session.result, timeout); err != nil {
		logEntry.Reason = constant.ReasonCallbackFrontendFail
		logEntry.ErrorMsg = fmt.Sprintf("Failed to request query result callback URL=%s: %v", session.queryResultCbURL, err)
		logrus.Error(logEntry)
		return
	}
	logrus.Info(logEntry)
}

func (app *App) extractDataSourcesInDQL(ctx context.Context, s *session) ([]*core.DataSource, error) {
	is, err := storage.QueryDBInfoSchema(s.GetSessionVars().Storage, s.request.GetDbName())
	if err != nil {
		return nil, err
	}

	// collect datasources which are referenced by the query via analyzing the logical plan
	return datasourcesInDQL(s.request.GetQuery(), s.request.GetDbName(), is)
}

func (app *App) buildCatalog(store *gorm.DB, dbName string, tableNames []string) (*scql.Catalog, error) {
	tableSchemas, err := QueryTableSchemas(store, dbName, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query referenced table schemas: %v", err)
	}

	return &scql.Catalog{
		Tables: tableSchemas,
	}, nil
}
