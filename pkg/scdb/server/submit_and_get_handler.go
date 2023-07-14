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
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/interpreter/optimizer"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

func (app *App) SubmitAndGetHandler(c *gin.Context) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "SCDBSubmitAndGetHandler", c.FullPath()),
	}

	request := &scql.SCDBQueryRequest{}
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, request)
	if err != nil {
		logEntry.Reason = constant.ReasonInvalidRequestFormat
		logEntry.ErrorMsg = err.Error()
		logEntry.CostTime = time.Since(timeStart)
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		c.String(http.StatusOK, errorResponse(scql.Code_BAD_REQUEST, "invalid request body", message.EncodingTypeJson))
		audit.RecordUncategorizedEvent(status.New(scql.Code_BAD_REQUEST, "invalid request body"), c.ClientIP(), "submitAndGet")
		return
	}

	resp := app.submitAndGet(c.Request.Context(), request)
	body, _ := message.SerializeTo(resp, inputEncodingType)
	c.String(http.StatusOK, body)

	logEntry.RequestID = request.BizRequestId
	logEntry.SessionID = resp.ScdbSessionId
	logEntry.RawRequest = SCDBQueryRequestToLogString(request)
	logEntry.CostTime = time.Since(timeStart)

	audit.RecordRunSyncQueryEvent(request, resp, timeStart, c.ClientIP())
	if resp.Status.Code != int32(scql.Code_OK) {
		logEntry.Reason = constant.ReasonInvalidRequest
		logEntry.ErrorMsg = resp.Status.Message
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		return
	}
	logrus.Infof("%v|ClientIP:%v", logEntry, c.ClientIP())
}

// submitAndGet implements core logic of SubmitAndGetHandler
func (app *App) submitAndGet(ctx context.Context, req *scql.SCDBQueryRequest) *scql.SCDBQueryResultResponse {
	session, err := newSession(ctx, req, app.storage, app.grmClient)
	if err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_INTERNAL, err.Error())
	}

	// authentication
	if err = session.authenticateUser(req.User); err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_UNAUTHENTICATED, err.Error())
	}

	isDQL, err := isDQL(req.Query)
	if err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_SQL_PARSE_ERROR, err.Error())
	}
	session.isDQLRequest = isDQL

	if isDQL {
		return app.submitAndGetDQL(ctx, session)
	}
	app.runSQL(session)
	return session.result
}

// submitAndGetDQL executes query and gets result back
func (app *App) submitAndGetDQL(ctx context.Context, s *session) *scql.SCDBQueryResultResponse {
	s.engineStub = executor.NewEngineStub(s.id,
		app.config.SCDBHost,
		engineCallbackPath,
		app.engineClient,
		app.config.Engine.Protocol,
		app.config.Engine.ContentType,
	)

	lpInfo, err := app.compilePrepare(ctx, s)
	if err != nil {
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}

	s.fillPartyInfo(lpInfo.engineInfos)

	epInfo, err := app.compile(lpInfo, s)
	if err != nil {
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}

	// Build session start parameters.
	spuRuntimeCfg, err := NewSpuRuntimeCfg(app.config.Engine.SpuRuntimeCfg)
	if err != nil {
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}
	sessionStartParams := &scql.SessionStartParams{
		SessionId:     s.id,
		Parties:       epInfo.parties,
		SpuRuntimeCfg: spuRuntimeCfg,
	}

	mapper := optimizer.NewGraphMapper(epInfo.graph, epInfo.subDAGs)
	mapper.Map()
	pbRequests := mapper.CodeGen(sessionStartParams)

	syncExecutor, err := executor.NewSyncExecutor(pbRequests, epInfo.graph.OutputNames, s.engineStub, s.id, epInfo.graph.PartyInfo)
	if err != nil {
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}
	resp, err := syncExecutor.RunExecutionPlan(ctx)
	if err != nil {
		var st *status.Status
		if errors.As(err, &st) {
			return newErrorFetchResponse(s.id, st.Code(), st.Message())
		}
		return newErrorFetchResponse(s.id, scql.Code_INTERNAL, err.Error())
	}

	s.setResultWithOutputColumnsAndAffectedRows(resp.OutColumns, resp.AffectedRows)

	return s.result
}
