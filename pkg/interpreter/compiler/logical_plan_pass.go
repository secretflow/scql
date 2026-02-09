// Copyright 2026 Ant Group Co., Ltd.
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

package compiler

import (
	"fmt"

	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
)

// LogicalPlanPass converts AST to logical plan
type LogicalPlanPass struct{}

// NewLogicalPlanPass creates a new logical plan pass
func NewLogicalPlanPass() *LogicalPlanPass {
	return &LogicalPlanPass{}
}

// Name returns the pass name
func (p *LogicalPlanPass) Name() string {
	return "LogicalPlanPass"
}

// Run builds logical plan from AST
func (p *LogicalPlanPass) Run(c *CompileContext) error {
	// 1. Create and configure session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	sctx.GetSessionVars().CurrentDB = c.Request.GetDb()

	// Set groupby threshold
	var groupbyThreshold uint64
	if c.Request.SecurityConfig.ResultSecurityConf == nil {
		groupbyThreshold = 4
	} else {
		if c.Request.SecurityConfig.ResultSecurityConf.GroupbyThreshold <= 0 {
			return fmt.Errorf("groupby threshold must be greater than 0")
		}
		groupbyThreshold = uint64(c.Request.SecurityConfig.ResultSecurityConf.GroupbyThreshold)
	}
	sctx.GetSessionVars().GroupByThreshold = groupbyThreshold
	c.SessionCtx = sctx

	// 2. Get statement from AST
	if c.AST == nil {
		return fmt.Errorf("no AST found in context")
	}
	stmt := c.AST

	// 3. Build logical plan
	lp, _, err := core.BuildLogicalPlanWithOptimization(c.Ctx, sctx, stmt, c.InfoSchema)
	if err != nil {
		return fmt.Errorf("failed to build logical plan: %v", err)
	}

	// Store logical plan in context
	c.LogicalPlan = lp

	return nil
}
