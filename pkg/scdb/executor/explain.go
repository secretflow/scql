// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023
package executor

import (
	"context"

	"github.com/secretflow/scql/pkg/interpreter"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/chunk"
)

type ExplainExec struct {
	baseExecutor
	query        string
	dbName       string
	issuer       string
	catalog      interface{}
	securityConf interface{}
	compileOpts  interface{}
	done         bool
}

func (e *ExplainExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	compileReq := &proto.CompileQueryRequest{
		Query:  e.query,
		DbName: e.dbName,
		Issuer: &proto.PartyId{
			Code: e.issuer,
		},
		Catalog:      e.catalog.(*proto.Catalog),
		SecurityConf: e.securityConf.(*proto.SecurityConfig),
		CompileOpts:  e.compileOpts.(*proto.CompileOptions),
	}
	intrpr := interpreter.NewInterpreter()
	compiledPlan, err := intrpr.Compile(ctx, compileReq)
	if err != nil {
		return err
	}

	exeGraphDot := compiledPlan.GetExplain().GetExeGraphDot()

	req.AppendString(0, exeGraphDot)

	return nil
}
