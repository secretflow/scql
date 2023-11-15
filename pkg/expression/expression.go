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

package expression

import (
	"fmt"

	"github.com/secretflow/scql/pkg/util/chunk"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/opcode"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

type Expression interface {
	fmt.Stringer

	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error)

	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx sessionctx.Context, e Expression) bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no tables.
	// refers no correlated column.
	// refers no subqueries that refers any tables.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	// refers no param markers when prepare plan cache is enabled.
	ConstItem(sc *stmtctx.StatementContext) bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

func isColumnInOperand(c *Column) bool {
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return false
	}
	cols := make([]*Column, 0, 1)
	cols = ExtractColumnsFromExpressions(cols, sf.GetArgs(), isColumnInOperand)
	return len(cols) > 0
}

type CNFExprs []Expression

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	switch v := onExpr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == funcName {
			var ret []Expression
			for _, arg := range v.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = EvaluateExprWithNull(ctx, schema, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType, args...)
	case *Column:
		if !schema.Contains(x) {
			return x
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		//if x.DeferredExpr != nil {
		//	return FoldConstant(x)
		//}
	}
	return expr
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx sessionctx.Context, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
// TODO: remove this function. ScalarFunction should be newed in one place.
func wrapWithIsTrue(ctx sessionctx.Context, keepNull bool, arg Expression) (Expression, error) {
	if arg.GetType().EvalType() == types.ETInt {
		return arg, nil
	}
	fc := &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruth, 1, 1}, opcode.IsTruth, keepNull}
	f, err := fc.getFunction(ctx, []Expression{arg})
	if err != nil {
		return nil, err
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}
	return sf, nil
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	con, ok := expr.(*Constant)
	return ok && con.Value.Kind() == types.KindBinaryLiteral
}

func analyzeScalarFunctionParams(sf *ScalarFunction) (
	varAndConst bool, allVars bool, allConst bool) {
	hasVar := false
	hasConst := false
	for _, expr := range sf.GetArgs() {
		switch expr.(type) {
		case *Constant:
			hasConst = true
		case *Column, *CorrelatedColumn:
			hasVar = true
		case *ScalarFunction:
			vc, av, ac := analyzeScalarFunctionParams(
				expr.(*ScalarFunction))
			if vc {
				return true, false, false
			}
			hasVar = hasVar || av
			hasConst = hasConst || ac
		}
	}
	return hasVar && hasConst, hasVar && !hasConst, hasConst && !hasVar
}
