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

package executor

import (
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	startTS uint64 // cached when the first time getStartTS() is called
	// err is set when there is error happened during Executor building process.
	err               error
	isSelectForUpdate bool
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p core.Plan) Executor {
	switch v := p.(type) {
	case *core.Simple:
		return b.buildSimple(v)
	case *core.DDL:
		return b.buildDDL(v)
	case *core.LogicalShow:
		return b.buildShow(v)
	case *core.Set:
		return b.buildSet(v)
	default:
		b.err = ErrUnknownPlan.GenWithStack("unsupported Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildSimple(v *core.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	grantStmtLabel := stringutil.StringerStr("GrantStmt")
	e := &GrantExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, grantStmtLabel),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		TLSOptions:   grant.TLSOptions,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildShow(v *core.LogicalShow) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Column:       v.Column,
		IndexName:    v.IndexName,
		Flag:         v.Flag,
		Roles:        v.Roles,
		User:         v.User,
		is:           b.is,
		Full:         v.Full,
		IfNotExists:  v.IfNotExists,
		GlobalScope:  v.GlobalScope,
		Extended:     v.Extended,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		// The input is a "show grants" statement, fulfill the user and roles field.
		// Note: "show grants" result are different from "show grants for current_user",
		// The former determine privileges with roles, while the later doesn't.
		vars := e.ctx.GetSessionVars()
		e.User = &auth.UserIdentity{Username: vars.User.Username, Hostname: vars.User.Hostname}
		e.Roles = vars.ActiveRoles
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	revokeStmtLabel := stringutil.StringerStr("RevokeStmt")
	e := &RevokeExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, revokeStmtLabel),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *core.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *core.Set) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}
