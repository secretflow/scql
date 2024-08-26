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
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/audit"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/logutil"
	"github.com/secretflow/scql/pkg/util/message"
)

func (app *App) FetchHandler(c *gin.Context) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "FetchHandler", c.FullPath()),
	}

	request := &scql.SCDBFetchRequest{}
	inputEncodingType, err := message.DeserializeFrom(c.Request.Body, request, c.Request.Header.Get("Content-Type"))
	if err != nil {
		logEntry.Reason = constant.ReasonInvalidRequestFormat
		logEntry.ErrorMsg = err.Error()
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		body, _ := message.SerializeTo(newErrorFetchResponse("", scql.Code_BAD_REQUEST, "invalid request body"), message.EncodingTypeJson)
		c.String(http.StatusOK, body)
		audit.RecordUncategorizedEvent(status.New(scql.Code_BAD_REQUEST, "invalid request body"), c.ClientIP(), "fetchResult")
		return
	}
	logEntry.RawRequest = SCDBFetchRequestToLogString(request)

	ctx := c.Request.Context()
	resp := app.queryResultHandlerCore(ctx, request)
	logEntry.CostTime = time.Since(timeStart)
	logEntry.SessionID = resp.ScdbSessionId
	body, _ := message.SerializeTo(resp, inputEncodingType)
	c.String(http.StatusOK, body)
	audit.RecordFetchResultEvent(request, resp, c.ClientIP())
	if resp.Status.Code != int32(scql.Code_OK) {
		logEntry.Reason = constant.ReasonInvalidRequest
		logEntry.ErrorMsg = resp.Status.Message
		logrus.Errorf("%v|ClientIP:%v", logEntry, c.ClientIP())
		return
	}
	logrus.Infof("%v|RowsAffected:%v|ClientIP:%v", logEntry, resp.AffectedRows, c.ClientIP())
}

func (app *App) queryResultHandlerCore(c context.Context, request *scql.SCDBFetchRequest) *scql.SCDBQueryResultResponse {
	session, ok := app.getSession(request.ScdbSessionId)
	if !ok {
		return newErrorSCDBQueryResultResponse(scql.Code_SESSION_NOT_FOUND, fmt.Sprintf("session %s doesn't exists", request.ScdbSessionId))
	}

	// authentication
	if err := session.authenticateUser(request.User); err != nil {
		return newErrorSCDBQueryResultResponse(scql.Code_UNAUTHENTICATED, err.Error())
	}

	// check session owner
	if userName := request.User.User.GetNativeUser().Name; session.createdBy != userName {
		return newErrorSCDBQueryResultResponse(scql.Code_INVALID_ARGUMENT, fmt.Sprintf("user %s doesn't own the session", userName))
	}
	if session.result == nil {
		return newNotReadyFetchResponse(request.ScdbSessionId)
	}

	return session.result
}

func newErrorSCDBQueryResultResponse(code scql.Code, errMsg string) *scql.SCDBQueryResultResponse {
	return &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: errMsg,
		},
	}
}

func newNotReadyFetchResponse(sessionId string) *scql.SCDBQueryResultResponse {
	return newErrorFetchResponse(sessionId, scql.Code_NOT_READY, "result not ready, please retry later")
}

func newErrorFetchResponse(sessionID string, code scql.Code, errMsg string) *scql.SCDBQueryResultResponse {
	return &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: errMsg,
		},
		ScdbSessionId: sessionID,
	}
}
