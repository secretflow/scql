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
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/executor"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var _ pb.EngineResultCallbackServer = &grpcIntraSvc{}

func (svc *grpcIntraSvc) Report(ctx context.Context, request *pb.ReportRequest) (*emptypb.Empty, error) {
	if request == nil || request.GetSessionId() == "" {
		return nil, errors.New("Report: illegal request: no session id")
	}

	logrus.Debugf("Receive report request: %s", request.String())

	app := svc.app
	session, ok := app.GetSession(request.GetSessionId())
	if !ok {
		return nil, fmt.Errorf("session %v not found", request.GetSessionId())
	}

	result := &pb.QueryResponse{}
	defer session.SetResultSafely(result)

	if request.GetStatus().GetCode() != int32(pb.Code_OK) {
		result.Status = request.GetStatus()
		return nil, nil
	}

	err := checkColumnsShape(request.GetOutColumns())
	if err != nil {
		result.Status = &pb.Status{
			Code:    int32(pb.Code_ENGINE_RUNSQL_ERROR),
			Message: err.Error(),
		}
		return nil, err
	}

	if err := executor.CheckResultSchemas(request.GetOutColumns(), session.OutputNames); err != nil {
		err = fmt.Errorf("received error query result report: %+v", err)
		result.Status = &pb.Status{
			Code:    int32(pb.Code_INTERNAL),
			Message: err.Error(),
		}
		return nil, err
	}

	result.Status = request.GetStatus()
	result.OutColumns = request.GetOutColumns()
	result.AffectedRows = request.GetNumRowsAffected()
	result.CostTimeS = time.Since(session.CreatedAt).Seconds()
	if session.GetSessionVars().AffectedByGroupThreshold {
		reason := "for safety, we filter the results for groups which contain less than 4 items."
		logrus.Infof("Report: %v", reason)
		result.Warnings = append(result.Warnings, &pb.SQLWarning{Reason: reason})
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
