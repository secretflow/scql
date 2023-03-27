// Copyright 2017 PingCAP, Inc.
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
	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/types"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

// WindowFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to WindowFuncExpr and collects WindowFuncExpr.
type WindowFuncExtractor struct {
	// WindowFuncs is the collected WindowFuncExprs.
	windowFuncs []*ast.WindowFuncExpr
}

// Enter implements Visitor interface.
func (a *WindowFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *WindowFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.WindowFuncExpr:
		a.windowFuncs = append(a.windowFuncs, v)
	}
	return n, true
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	baseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

func (s *logicalSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *logicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func buildLogicalJoinSchema(joinType JoinType, join LogicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return leftSchema.Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == LeftOuterJoin {
		resetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == RightOuterJoin {
		resetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}

// baseSchemaProducer stores the schema for the base plans who can produce schema directly.
type baseSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	basePlan
}

// OutputNames returns the outputting names of each column.
func (s *baseSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *baseSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// Schema implements the Plan.Schema interface.
func (s *baseSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *baseSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *baseSchemaProducer) setSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}
