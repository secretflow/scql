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

package core

import (
	"fmt"
	"strconv"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

type Plan interface {
	// Get the schema.
	Schema() *expression.Schema

	// Get the ID.
	ID() int

	// TP get the plan type.
	TP() string

	// Get the ID in explain statement
	ExplainID() fmt.Stringer

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// replaceExprColumns replace all the column reference in the plan's expression node.
	replaceExprColumns(replace map[string]*expression.Column)

	SCtx() sessionctx.Context

	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)

	SelectBlockOffset() int
}

type InsertTableOption struct {
	TableName string
	Columns   []string
}

type LogicalPlan interface {
	Plan

	// HashCode encodes a LogicalPlan to fast compare whether a LogicalPlan equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an expression slice, and return the expressions that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan)

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column) error

	// BuildKeyInfo will collect the information of unique keys into schema.
	BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema)

	extractCorrelatedCols() []*expression.CorrelatedColumn

	// Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)

	SqlStmt(Dialect) (*runSqlCtx, error)

	// Added for SCQL
	SetIntoOpt(option *ast.SelectIntoOption)
	IntoOpt() *ast.SelectIntoOption
	SetInsertTableOpt(option *InsertTableOption)
	InsertTableOpt() *InsertTableOption
}

type basePlan struct {
	tp             string
	id             int
	ctx            sessionctx.Context
	blockOffset    int
	intoOpt        *ast.SelectIntoOption
	insertTableOpt *InsertTableOption
}

func (p *basePlan) TP() string {
	return p.tp
}

func (p *basePlan) ID() int {
	return p.id
}

func (p *basePlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// ExplainInfo implements Plan interface.
func (p *basePlan) ExplainInfo() string {
	return "N/A"
}

func (p *basePlan) SCtx() sessionctx.Context {
	return p.ctx
}

func (p *basePlan) SelectBlockOffset() int {
	return p.blockOffset
}

func (p *baseLogicalPlan) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.children))
	for _, child := range p.children {
		corCols = append(corCols, child.extractCorrelatedCols()...)
	}
	return corCols
}

func newBasePlan(ctx sessionctx.Context, tp string, offset int) basePlan {
	ctx.GetSessionVars().PlanID++
	id := ctx.GetSessionVars().PlanID
	return basePlan{
		tp:          tp,
		id:          id,
		ctx:         ctx,
		blockOffset: offset,
	}
}

func newBaseLogicalPlan(ctx sessionctx.Context, tp string, self LogicalPlan, offset int) baseLogicalPlan {
	return baseLogicalPlan{
		basePlan: newBasePlan(ctx, tp, offset),
		self:     self,
	}
}

type baseLogicalPlan struct {
	basePlan

	self     LogicalPlan
	children []LogicalPlan
}

func (p *baseLogicalPlan) ExplainInfo() string {
	return ""
}

func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

func (p *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	p.children = children
}

func (p *baseLogicalPlan) SetChild(i int, child LogicalPlan) {
	p.children[i] = child
}

// Schema implements Plan Schema interface.
func (p *baseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *baseLogicalPlan) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

func (p *baseLogicalPlan) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
}

func (p *basePlan) replaceExprColumns(replace map[string]*expression.Column) {
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *baseLogicalPlan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	// TODO(teng.t): Decide whether we need MaxOneRow function here.
	// childMaxOneRow := make([]bool, len(p.children))
	// for i := range p.children {
	// 	childMaxOneRow[i] = p.children[i].MaxOneRow()
	// }
	// p.maxOneRow = HasMaxOneRow(p.self, childMaxOneRow)
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column) error {
	if len(p.children) == 0 {
		return nil
	}
	return p.children[0].PruneColumns(parentUsedCols)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *logicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
}

func (p *basePlan) SetIntoOpt(option *ast.SelectIntoOption) {
	p.intoOpt = option
}

func (p *basePlan) IntoOpt() *ast.SelectIntoOption {
	return p.intoOpt
}

func (p *basePlan) SetInsertTableOpt(option *InsertTableOption) {
	p.insertTableOpt = option
}

func (p *basePlan) InsertTableOpt() *InsertTableOption {
	return p.insertTableOpt
}

func (p *baseLogicalPlan) SqlStmt(d Dialect) (*runSqlCtx, error) {
	return nil, fmt.Errorf("unsupported logical plan baseLogicalPlan")
}
