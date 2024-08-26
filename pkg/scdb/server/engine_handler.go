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
	"strings"
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

func (app *App) EngineHandler(c *gin.Context) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		ActionName: fmt.Sprintf("%v@%v", "SCDBEngineHandler", c.FullPath()),
	}
	report, err := engineHandlerCore(app, c)
	logEntry.CostTime = time.Since(timeStart)
	logEntry.SessionID = report.GetJobId()
	if err != nil {
		logEntry.ErrorMsg = err.Error()
		logrus.Errorf("%v|PartyCode:%v|SessionId:%v|ClientIP:%v", logEntry, report.GetPartyCode(), report.GetJobId(), c.ClientIP())
		return
	}
	logrus.Infof("%v|PartyCode:%v|SessionId:%v|ClientIP:%v", logEntry, report.GetPartyCode(), report.GetJobId(), c.ClientIP())
}

func engineHandlerCore(app *App, c *gin.Context) (report *scql.ReportRequest, err error) {
	if c.Request == nil || c.Request.Body == nil {
		errMsg := "invalid request"
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return &scql.ReportRequest{}, fmt.Errorf(errMsg)
	}
	request := &scql.ReportRequest{}
	_, err = message.DeserializeFrom(c.Request.Body, request, c.Request.Header.Get("Content-Type"))
	if err != nil {
		return &scql.ReportRequest{}, err
	}

	session, ok := app.getSession(request.GetJobId())
	if !ok {
		err = fmt.Errorf("session %v not found", request.GetJobId())
		// ignore session deleted, still marked as OK
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return request, err
	}

	// response OK
	c.JSON(http.StatusOK, gin.H{})

	finished := session.executor.HandleResultCallback(request)

	if !finished {
		return request, nil
	}

	result, err := session.executor.MergeQueryResults()
	if err != nil {
		var st *status.Status
		if errors.As(err, &st) {
			result = newErrorCallbackResult(session.id, st.Code(), st)
		} else {
			result = newErrorCallbackResult(session.id, scql.Code_INTERNAL, err)
		}
		app.finishSession(session, result, constant.ReasonSessionAbnormalQuit)
		return request, err
	}

	app.finishSession(session, result, constant.ReasonSessionNormalQuit)
	return request, nil
}

func newErrorCallbackResult(sessionId string, code scql.Code, err error) *scql.SCDBQueryResultResponse {
	return &scql.SCDBQueryResultResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: err.Error(),
		},
		ScdbSessionId: sessionId,
	}
}

func (app *App) finishSession(session *session, result *scql.SCDBQueryResultResponse, sessionDestroyReason string) {
	audit.RecordAsyncCompleteEvent(result)
	if session.queryResultCbURL != "" {
		_, err := callbackFrontend(session.ctx, result, session.queryResultCbURL)
		// rewrite local copy of sessionDestroyReason even on callback error
		if constant.ReasonSessionNormalQuit == sessionDestroyReason && err != nil {
			sessionDestroyReason = constant.ReasonSessionAbnormalQuit
		}
	} else {
		resultTableShape, _ := getTableShape(result)
		logrus.Infof("|ExecutionPlanId:%v|TableShape:%v|AffectedRows:%v", session.id, resultTableShape, result.AffectedRows)
	}
	session.result = result
}

func callbackFrontend(ctx context.Context, engineReq *scql.SCDBQueryResultResponse, cbURL string) (reason string, err error) {
	timeStart := time.Now()
	logEntry := &logutil.MonitorLogEntry{
		SessionID:  engineReq.ScdbSessionId,
		ActionName: "callbackFrontend",
	}
	resultTableShape, err := getTableShape(engineReq)
	if err != nil {
		logrus.Errorf("Unexpected invalid table shape, err:%v", err)
	}
	reason, err = callbackFrontendCore(ctx, engineReq, cbURL)
	logEntry.CostTime = time.Since(timeStart)
	if err != nil {
		logEntry.Reason = reason
		logEntry.ErrorMsg = err.Error()
		logrus.Errorf("%v|TableShape:%v", logEntry, resultTableShape)
		return reason, err
	}

	logrus.Infof("%v|TableShape:%v", logEntry, resultTableShape)
	return "", nil
}

func callbackFrontendCore(ctx context.Context, engineReq *scql.SCDBQueryResultResponse, cbURL string) (reason string, err error) {
	if engineReq == nil {
		return constant.ReasonInvalidRequest, fmt.Errorf("nil engine request")
	}
	client := &http.Client{}

	body, err := message.SerializeTo(engineReq, message.EncodingTypeJson)
	if err != nil {
		logrus.Errorf("Error when serialize req: %v", err)
		return constant.ReasonInvalidRequest, err
	}
	req, err := http.NewRequest("POST", cbURL, strings.NewReader(body))
	if err != nil {
		logrus.Errorf("Error when constructing frontend req: %v", err)
		return constant.ReasonInvalidRequest, err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		logrus.Errorf("Error when submitting request to frontend: %v", err)
		return constant.ReasonCallbackFrontendFail, err
	}
	defer res.Body.Close()
	return "", nil
}

func getTableShape(queryResult *scql.SCDBQueryResultResponse) (string, error) {
	if queryResult == nil {
		return "", fmt.Errorf("unexpected nil queryResult")
	}
	columnSize := len(queryResult.GetOutColumns())
	if columnSize == 0 {
		return fmt.Sprintf("%v*%v", 0, columnSize), nil
	}
	column0 := queryResult.GetOutColumns()[0]
	if column0 == nil {
		return fmt.Sprintf("%v*%v", 0, columnSize), nil
	}
	column0Shape := column0.GetShape()
	if column0Shape == nil || len(column0Shape.GetDim()) == 0 {
		return fmt.Sprintf("%v*%v", 0, columnSize), fmt.Errorf("unexpected nil TensorShape")
	}
	// only use the first dim for current implementation
	column0Dim0 := column0Shape.GetDim()[0]
	rowSize := column0Dim0.GetDimValue()
	return fmt.Sprintf("%v*%v", rowSize, columnSize), nil
}
