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

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/util/message"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

func datasourcesInDQL(stmt string, db string, is infoschema.InfoSchema) ([]*core.DataSource, error) {
	// Parsing
	p := parser.New()
	ast, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}

	// Planning
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	sctx.GetSessionVars().CurrentDB = db

	builder := core.NewPlanBuilder(sctx, is)
	plan, err := builder.Build(context.Background(), ast)
	if err != nil {
		return nil, err
	}
	lp, ok := plan.(core.LogicalPlan)
	if !ok {
		return nil, fmt.Errorf("assert failed while selectionNode buildCCL, expected: core.LogicalPlan, actual: %T", plan)
	}
	return datasourcesInLogicPlan(lp), nil
}

func datasourcesInLogicPlan(lp core.LogicalPlan) []*core.DataSource {
	var result []*core.DataSource
	for _, p := range lp.Children() {
		dsList := datasourcesInLogicPlan(p)
		result = append(result, dsList...)
	}

	if len(lp.Children()) != 0 {
		return result
	}

	ds, ok := lp.(*core.DataSource)

	if !ok {
		return result
	}

	result = append(result, ds)
	return result
}

func errorResponse(code scql.Code, errorMsg string, encodingType message.ContentEncodingType) string {
	response := scql.SCDBSubmitResponse{
		Status: &scql.Status{
			Code:    int32(code),
			Message: errorMsg,
		},
	}
	body, _ := message.SerializeTo(&response, encodingType)
	return body
}

func SCDBQueryRequestToLogString(req *scql.SCDBQueryRequest) string {
	// Remove sensitive info in the log string
	newReq := &scql.SCDBQueryRequest{
		Header: req.Header,
		User: &scql.SCDBCredential{
			User: &scql.User{
				AccountSystemType: scql.User_NATIVE_USER,
				User: &scql.User_NativeUser_{
					NativeUser: &scql.User_NativeUser{
						Name: req.GetUser().GetUser().GetNativeUser().GetName(),
					},
				},
			},
		},
		Query:                  stringutil.RemoveSensitiveInfo(req.Query),
		QueryResultCallbackUrl: req.QueryResultCallbackUrl,
		BizRequestId:           req.BizRequestId,
		DbName:                 req.DbName,
	}
	return newReq.String()
}

func SCDBFetchRequestToLogString(req *scql.SCDBFetchRequest) string {
	// Remove sensitive info in the log string
	newReq := &scql.SCDBFetchRequest{
		Header: req.Header,
		User: &scql.SCDBCredential{
			User: &scql.User{
				AccountSystemType: scql.User_NATIVE_USER,
				User: &scql.User_NativeUser_{
					NativeUser: &scql.User_NativeUser{
						Name: req.GetUser().GetUser().GetNativeUser().GetName(),
					},
				},
			},
		},
		ScdbSessionId: req.ScdbSessionId,
	}
	return newReq.String()
}
