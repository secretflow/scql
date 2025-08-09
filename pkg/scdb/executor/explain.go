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

	"github.com/secretflow/scql/pkg/interpreter"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/util/chunk"
)

type ExplainExec struct {
	baseExecutor
	TargetPlan core.Plan
	Format     string
	Analyze    bool
	done       bool
}

func (e *ExplainExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true
	interpreterInstance := interpreter.NewInterpreter()

	// 调用解释器编译，这里需要根据 Compile 方法的实际参数进行调整
	// 假设 Compile 方法需要的参数可以从上下文或者其他地方获取，以下为示例
	// 实际使用时需要根据真实情况补充完整参数
	compiledPlan, err := interpreterInstance.Compile(ctx, nil) // 这里的参数需要根据实际情况修改
	if err != nil {
		return err
	}

	if compiledPlan.Explain == nil || compiledPlan.Explain.ExeGraphDot == "" {
		return nil
	}
	dotInfo := compiledPlan.Explain.ExeGraphDot

	req.Reset()

	if req.NumCols() == 0 {
		return fmt.Errorf("chunk has no columns")
	}

	col := req.Column(0)
	col.AppendString(dotInfo)

	return nil
}
