// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023

package executor

import (
	"context"
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/execdetails"
)

const (
	ResultMaxRows = 1024
	InitChunkSize = 1024
	MaxChunkSize  = 1024
)

// Run runs an DDL/DCL statement on SCDB
func Run(ctx sessionctx.Context, stmt string, is infoschema.InfoSchema) ([]*scql.Tensor, error) {
	// Step 1: Parsing
	p := parser.New()
	ast, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}
	if err := core.Preprocess(ctx, ast, is); err != nil {
		return nil, err
	}

	// Step 2: Planning
	lp, _, err := core.BuildLogicalPlan(context.Background(), ctx, ast, is)
	if err != nil {
		return nil, err
	}

	// Step 3: Executing
	eb := newExecutorBuilder(ctx, is)
	exec := eb.build(lp)
	if eb.err != nil {
		return nil, eb.err
	}
	if err := exec.Open(context.Background()); err != nil {
		return nil, err
	}

	retTypes := []*types.FieldType{}
	for _, c := range lp.Schema().Columns {
		retTypes = append(retTypes, c.RetType)
	}

	ck := chunk.New(retTypes, ResultMaxRows, ResultMaxRows)
	var result []*scql.Tensor
	for {
		if err := exec.Next(context.Background(), ck); err != nil {
			return nil, err
		}
		if result, err = mergeResultFromChunk(ck, lp, result); err != nil {
			return nil, err
		}
		if ck.NumRows() == 0 {
			break
		}
	}
	return result, err
}

func mergeResultFromChunk(ck *chunk.Chunk, p core.Plan, tensors []*scql.Tensor) ([]*scql.Tensor, error) {
	for i := 0; i < ck.NumCols(); i++ {
		eType := p.Schema().Columns[i].RetType.Tp
		if eType != mysql.TypeVarString && eType != mysql.TypeVarchar {
			return nil, fmt.Errorf("unsupported RetType: %v", p.Schema().Columns[i].RetType)
		}

		var ss []string
		for j := 0; j < ck.NumRows(); j++ {
			ss = append(ss, ck.Column(i).GetString(j))
		}

		tName := p.OutputNames()[i].String()
		if i < len(tensors) {
			if tName == tensors[i].GetName() {
				tensors[i].StringData = append(tensors[i].StringData, ss...)
				tensors[i].Shape.Dim[0].Value = &scql.TensorShape_Dimension_DimValue{
					DimValue: int64(len(tensors[i].GetStringData())),
				}
			} else {
				return nil, fmt.Errorf("unexpected tensor, expected: %v, but get: %v, ", tensors[i].GetName(), tName)
			}
		} else {
			tShap := &scql.TensorShape{
				Dim: []*scql.TensorShape_Dimension{
					{Value: &scql.TensorShape_Dimension_DimValue{DimValue: int64(len(ss))}},
					{Value: &scql.TensorShape_Dimension_DimValue{DimValue: 1}},
				},
			}
			tensors = append(tensors, &scql.Tensor{
				Name:       tName,
				ElemType:   scql.PrimitiveDataType_STRING,
				StringData: ss,
				Shape:      tShap,
			})
		}
	}
	return tensors, nil
}

type baseExecutor struct {
	ctx           sessionctx.Context
	id            fmt.Stringer
	schema        *expression.Schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.RuntimeStats
}

// base returns the baseExecutor of an executor, don't override this method!
func (e *baseExecutor) base() *baseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id fmt.Stringer, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      InitChunkSize,
		maxChunkSize: MaxChunkSize,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id != nil {
			e.runtimeStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.id.String())
		}
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
}

// newFirstChunk creates a new chunk to buffer current executor's result.
func newFirstChunk(e Executor) *chunk.Chunk {
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}
