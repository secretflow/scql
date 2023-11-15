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

package svc

import (
	"context"

	"github.com/secretflow/scql/pkg/interpreter"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type RpcSvc struct {
	intr *interpreter.Interpreter
}

var _ pb.InterpreterServiceServer = &RpcSvc{}

func NewRpcSvc() *RpcSvc {
	return &RpcSvc{
		intr: interpreter.NewInterpreter(),
	}
}

func (svc *RpcSvc) CompileQuery(ctx context.Context, req *pb.CompileQueryRequest) (*pb.CompileQueryResponse, error) {

	compiledPlan, err := svc.intr.Compile(ctx, req)

	if err != nil {
		return nil, err
	}

	return &pb.CompileQueryResponse{
		Status: &pb.Status{
			Code:    0,
			Message: "ok",
		},
		Plan: compiledPlan,
	}, nil
}
