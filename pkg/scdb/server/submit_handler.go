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
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	scdbexecutor "github.com/secretflow/scql/pkg/scdb/executor"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

func newErrorSCDBSubmitResponse(code scql.Code, errorMsg string) *scql.SCDBSubmitResponse {
	return &scql.SCDBSubmitResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: errorMsg,
		},
	}
}

func newSuccessSCDBSubmitResponse(sessionID string) *scql.SCDBSubmitResponse {
	return &scql.SCDBSubmitResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "OK",
		},
		ScdbSessionId: sessionID,
	}
}

func (app *App) SubmitHandler(c *gin.Context) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "SCDBQueryHandler", c.FullPath()),
	}

	request := &scql.SCDBQueryRequest{}
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, request, c.Request.Header.Get("Content-Type"))
	if err != nil {
		logEntry.Reason = constant.ReasonInvalidRequestFormat
		logEntry.ErrorMsg = err.Error()
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		c.String(http.StatusOK, errorResponse(scql.Code_BAD_REQUEST, "invalid request body", message.EncodingTypeJson))
		audit.RecordUncategorizedEvent(status.New(scql.Code_BAD_REQUEST, "invalid request body"), c.ClientIP(), "submit")
		return
	}
	resp := app.submit(c.Request.Context(), request)
	body, _ := message.SerializeTo(resp, inputEncodingType)

	logEntry.RequestID = request.BizRequestId
	logEntry.SessionID = resp.ScdbSessionId
	logEntry.CostTime = time.Since(timeStart)
	logEntry.RawRequest = SCDBQueryRequestToLogString(request)
	audit.RecordRunASyncQueryEvent(request, resp, timeStart, c.ClientIP())

	c.String(http.StatusOK, body)
	if resp.Status.Code != int32(scql.Code_OK) {
		logEntry.Reason = constant.ReasonInvalidRequest
		logEntry.ErrorMsg = resp.Status.Message
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		return
	}

	logrus.Infof("%v|ClientIP:%v", logEntry, c.ClientIP())
}

// submit implements core logic of SubmitHandler
func (app *App) submit(ctx context.Context, req *scql.SCDBQueryRequest) *scql.SCDBSubmitResponse {
	session, err := newSession(ctx, req, app.storage)
	if err != nil {
		return newErrorSCDBSubmitResponse(scql.Code_INTERNAL, err.Error())
	}
	// authentication
	if err = session.authenticateUser(req.User); err != nil {
		return newErrorSCDBSubmitResponse(scql.Code_UNAUTHENTICATED, err.Error())
	}

	// if DQL, submit to interpreter core
	isDQL, err := isDQL(req.Query)
	if err != nil {
		return newErrorSCDBSubmitResponse(scql.Code_SQL_PARSE_ERROR, err.Error())
	}
	session.isDQLRequest = isDQL

	app.sessions.SetDefault(session.id, session)
	if isDQL {
		return app.submitDQL(ctx, session)
	}
	go func() {
		app.runSQL(session)
		app.notifyQueryJobDone(session.id)
	}()
	return newSuccessSCDBSubmitResponse(session.id)
}

func isDQL(sql string) (bool, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return false, err
	}

	switch stmt.(type) {
	case *ast.SelectStmt, *ast.UnionStmt:
		return true, nil
	}

	return false, nil
}

// compile and run dql
func (app *App) submitDQL(ctx context.Context, session *session) *scql.SCDBSubmitResponse {
	_, err := app.runDQL(ctx, session, true)
	if err != nil {
		var status *status.Status
		if errors.As(err, &status) {
			return newErrorSCDBSubmitResponse(status.Code(), status.Message())
		}
		return newErrorSCDBSubmitResponse(scql.Code_INTERNAL, err.Error())
	}
	return newSuccessSCDBSubmitResponse(session.id)
}

func isQueryNeedInfoSchema(query string) (bool, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return false, err
	}
	switch stmt.(type) {
	case *ast.CreateUserStmt, *ast.DropTableStmt:
		return false, nil
	default:
		return true, nil
	}
}

// runSQL run DDL/DCL
func (app *App) runSQL(s *session) {
	var err error = nil
	rt := []*scql.Tensor{}
	defer func() {
		if err != nil {
			var status *status.Status
			if errors.As(err, &status) {
				s.setStatus(int32(status.Code()), status.Message())
			} else {
				s.setStatus(int32(scql.Code_INTERNAL), err.Error())
			}
			logrus.Errorf("Failed to runSQL, sessionid: %v, err: %v", s.id, err.Error())
		} else {
			s.setResult(rt)
		}
	}()
	needSchema, err := isQueryNeedInfoSchema(s.request.Query)
	if err != nil {
		return
	}
	var is infoschema.InfoSchema
	// skip query infoschema from storage for create user statement
	if needSchema {
		is, err = storage.QueryDBInfoSchema(s.GetSessionVars().Storage, s.request.DbName)
		if err != nil {
			return
		}
	}

	rt, err = scdbexecutor.Run(s, s.request.Query, is)
}

func (s *session) setStatus(code int32, errMsg string) {
	s.result = &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    code,
			Message: errMsg,
		},
		OutColumns:    nil,
		ScdbSessionId: s.id,
	}
}

func (s *session) setResult(result []*scql.Tensor) {
	s.result = &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "",
		},
		OutColumns:    result,
		ScdbSessionId: s.id,
	}
}
