// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"fmt"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/privilege"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

const (
	flagPrunColumns uint64 = 1 << iota
	flagBuildKeyInfo
	flagDecorrelate
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagPushDownAgg
	flagPushDownTopN
	flagJoinReOrder
	flagDoubleCheckEliminateProjection
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&optPlaceHolder{},
	&optPlaceHolder{},
	&optPlaceHolder{},
	&projectionEliminator{},
	&optPlaceHolder{},
	&ppdSolver{},
	&optPlaceHolder{},
	&optPlaceHolder{},
	&aggregationPushDownSolver{},
	&optPlaceHolder{},
	&optPlaceHolder{},
	&projectionEliminator{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(context.Context, LogicalPlan) (LogicalPlan, error)
	name() string
}

// NOTE(yang.y): the order of applying the optimization rule matters, so
// we should create temporary place holders for optimizers that yet to be
// implemented.
type optPlaceHolder struct{}

func (optPlaceHolder) optimize(_ context.Context, lp LogicalPlan) (LogicalPlan, error) {
	return lp, nil
}

func (optPlaceHolder) name() string { return "opt_place_holder" }

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, types.NameSlice, error) {
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder := NewPlanBuilder(sctx, is)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}

	// QUESTION(shunde.csd): should we check privileges here
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, err
		}
	}

	return p, p.OutputNames(), err
}

// BuildLogicalPlanWithOptimization used to build logical plan from ast.Node and optimize it on logical level.
func BuildLogicalPlanWithOptimization(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (LogicalPlan, types.NameSlice, error) {
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder := NewPlanBuilder(sctx, is)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}

	lp, err := logicalOptimize(ctx, builder.optFlag|flagDoubleCheckEliminateProjection, p.(LogicalPlan))
	if err != nil {
		return nil, nil, err
	}

	return lp, lp.OutputNames(), err
}

func logicalOptimize(ctx context.Context, flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		logic, err = rule.optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}
	return logic, err
}

func isLogicalRuleDisabled(_ logicalOptRule) bool {
	// TODO(yang.y): make this configurable
	return false
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		ok, err := pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege)
		if err != nil {
			return fmt.Errorf("checkPrivilege: %v", err)
		}
		if !ok {
			if v.err == nil {
				return ErrPrivilegeCheckFail
			}
			return v.err
		}
	}
	return nil
}
