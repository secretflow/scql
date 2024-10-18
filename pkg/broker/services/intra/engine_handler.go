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

package intra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/executor"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

var _ pb.EngineResultCallbackServer = &grpcIntraSvc{}

func (svc *grpcIntraSvc) Report(ctx context.Context, request *pb.ReportRequest) (*emptypb.Empty, error) {
	if request == nil || request.GetJobId() == "" {
		return nil, errors.New("Report: illegal request: no session id")
	}

	logrus.Debugf("Receive report request: %s", request.String())

	app := svc.app
	info, err := app.GetSessionInfo(request.GetJobId())
	if err != nil {
		return nil, fmt.Errorf("session %v not found: %v", request.GetJobId(), err)
	}

	switch info.Status {
	case int8(storage.SessionFinished):
		return nil, nil
	case int8(storage.SessionRunning):
		break // check passed
	case int8(storage.SessionSubmitted):
		break // check passed
	default:
		return nil, fmt.Errorf("session status %d is not allowed to set result", info.Status)
	}

	result := &pb.QueryResponse{}
	defer func() {
		// stop engine whether persist failed or successfully
		if err := app.PersistSessionResult(request.GetJobId(), result, info.ExpiredAt); err != nil {
			logrus.Warnf("failed to persist session result: %v", err)
		}
		session, ok := app.GetSession(request.GetJobId())
		if ok {
			session.SetResultSafely(result)
			session.App.JobWatcher.Unwatch(request.GetJobId())
		}

		if engine, err := app.Scheduler.ParseEngineInstance(info.JobInfo); err != nil {
			logrus.Warnf("failed to get engine from info: %v", err)
		} else if err := engine.Stop(); err != nil {
			logrus.Warnf("failed to stop query job: %v", err)
		}
		logrus.Infof("success to stop engine for job: %v", request.GetJobId())
	}()

	if request.GetStatus().GetCode() != int32(pb.Code_OK) {
		result.Status = request.GetStatus()
		return nil, nil
	}

	err = checkColumnsShape(request.GetOutColumns())
	if err != nil {
		result.Status = &pb.Status{
			Code:    int32(pb.Code_ENGINE_RUNSQL_ERROR),
			Message: err.Error(),
		}
		return nil, err
	}

	var outputNames []string
	if info.OutputNames != "" {
		outputNames = strings.Split(info.OutputNames, ";")
	}
	if err := executor.CheckResultSchemas(request.GetOutColumns(), outputNames); err != nil {
		err = fmt.Errorf("received error query result report: %v", err)
		result.Status = &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: err.Error(),
		}
		return nil, err
	}
	var warn pb.Warning
	if err := message.ProtoUnmarshal([]byte(info.Warning), &warn); err != nil {
		err = fmt.Errorf("unmarshal warning err: %v", err)
		result.Status = &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: err.Error(),
		}
		return nil, err
	}

	result.Status = request.GetStatus()
	result.Result = &pb.QueryResult{
		OutColumns:   request.GetOutColumns(),
		AffectedRows: request.GetNumRowsAffected(),
		CostTimeS:    time.Since(info.CreatedAt).Seconds(),
	}

	if warn.MayAffectedByGroupThreshold {
		groupByThreshold := constant.DefaultGroupByThreshold
		if app.Conf.SecurityCompromise.GroupByThreshold > 0 {
			groupByThreshold = app.Conf.SecurityCompromise.GroupByThreshold
		}

		reason := fmt.Sprintf("for safety, we filter the results for groups which contain less than %d items.", groupByThreshold)
		logrus.Infof("Report: %v", reason)
		result.Result.Warnings = append(result.Result.Warnings, &pb.SQLWarning{Reason: reason})
	}

	return nil, nil
}

// check columns share the same shape
func checkColumnsShape(columns []*pb.Tensor) error {
	var dimValue int64 = 0
	isFirstCol := true
	for _, col := range columns {
		colShape := col.GetShape()
		if colShape == nil || len(colShape.GetDim()) == 0 {
			return fmt.Errorf("unexpected nil TensorShape")
		}
		if isFirstCol {
			dimValue = colShape.GetDim()[0].GetDimValue()
			isFirstCol = false
		} else if dimValue != colShape.GetDim()[0].GetDimValue() {
			return fmt.Errorf("dim shape not matched, pre shape value=%v, cur shape value=%v", dimValue, colShape.GetDim()[0].GetDimValue())
		}
	}

	return nil
}
