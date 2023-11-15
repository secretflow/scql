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
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/util/chunk"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	logrus.Infof("SetExecutor do1")
	for _, v := range e.vars {
		name := strings.ToLower(v.Name)
		if name != "time_zone" {
			return core.ErrUnsupportedType.GenWithStack("Unsupported set type")
		}
		logrus.Infof("SetExecutor do2")
		value, err := v.Expr.Eval(chunk.Row{})
		if err != nil {
			return err
		}
		svalue, err := value.ToString()
		if err != nil {
			return err
		}
		// FIXME(zhihe): every request will new session, here set not valid for next request
		sessionVars.SetTimeZone(svalue)
	}
	logrus.Infof("SetExecutor do3 : %s", sessionVars.GetTimeZone())
	return nil
}
