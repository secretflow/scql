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
	"bytes"
	"fmt"

	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/codec"
	"github.com/secretflow/scql/pkg/util/hack"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/terror"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

// error definitions.
var (
	ErrNoDB = terror.ClassOptimizer.New(mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName model.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType  *types.FieldType
	Function builtinFunc
	hashcode []byte
}

func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(ctx sessionctx.Context, e Expression) bool {
	fun, ok := e.(*ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	return sf.Function.equal(fun.Function)
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() sessionctx.Context {
	return sf.Function.getCtx()
}

// ConstItem implements Expression interface.
func (sf *ScalarFunction) ConstItem(sc *stmtctx.StatementContext) bool {
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false
	}
	for _, arg := range sf.GetArgs() {
		if !arg.ConstItem(sc) {
			return false
		}
	}
	return true
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.FuncName.L)
	for i, arg := range sf.GetArgs() {
		buffer.WriteString(arg.String())
		if i+1 != len(sf.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// typeInferForNull infers the NULL constants field type and set the field type
// of NULL constant same as other non-null operands.
func typeInferForNull(args []Expression) {
	if len(args) < 2 {
		return
	}
	var isNull = func(expr Expression) bool {
		cons, ok := expr.(*Constant)
		return ok && cons.RetType.Tp == mysql.TypeNull && cons.Value.IsNull()
	}
	// Infer the actual field type of the NULL constant.
	var retFieldTp *types.FieldType
	var hasNullArg bool
	for _, arg := range args {
		isNullArg := isNull(arg)
		if !isNullArg && retFieldTp == nil {
			retFieldTp = arg.GetType()
		}
		hasNullArg = hasNullArg || isNullArg
		// Break if there are both NULL and non-NULL expression
		if hasNullArg && retFieldTp != nil {
			break
		}
	}
	if !hasNullArg || retFieldTp == nil {
		return
	}
	for _, arg := range args {
		if isNull(arg) {
			*arg.GetType() = *retFieldTp
		}
	}
}

// newFunctionImpl creates a new scalar function or constant.
func newFunctionImpl(ctx sessionctx.Context, fold bool, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	if retType == nil {
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction")
	}
	if funcName == ast.Cast {
		return BuildCastFunction(ctx, args[0], retType), nil
	}
	fc, ok := funcs[funcName]
	if !ok {
		db := ctx.GetSessionVars().CurrentDB
		if db == "" {
			return nil, errors.Trace(ErrNoDB)
		}

		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", db+"."+funcName)
	}
	/* NOTE(yang.y): not sure we need flag EnableNoopFuncs
	if !ctx.GetSessionVars().EnableNoopFuncs {
		if _, ok := noopFuncs[funcName]; ok {
			return nil, ErrFunctionsNoopImpl.GenWithStackByArgs(funcName)
		}
	} */
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	typeInferForNull(funcArgs)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.Tp != mysql.TypeUnspecified || retType.Tp == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	/* NOTE(yang.y): not sure we need fold constant
	if fold {
		return FoldConstant(sf), nil
	} */
	return sf, nil
}

func TransferDateFuncIntervalToSeconds(funcArgs []Expression) ([]Expression, error) {
	res := make([]Expression, len(funcArgs)-1)
	copy(res, funcArgs[:len(funcArgs)-1])
	baseDateArithmitical := newDateArighmeticalUtil()
	intervalEvalTp := funcArgs[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}
	unit, isNull, err := funcArgs[2].EvalString(nil, chunk.Row{})
	if isNull || err != nil {
		return nil, err
	}
	getIntervalFunction := baseDateArithmitical.getIntervalFromString
	switch intervalEvalTp {
	case types.ETString:
	case types.ETInt:
		getIntervalFunction = baseDateArithmitical.getIntervalFromInt
	case types.ETDecimal:
		getIntervalFunction = baseDateArithmitical.getIntervalFromDecimal
	case types.ETReal:
		getIntervalFunction = baseDateArithmitical.getIntervalFromReal
	}
	interval, isNull, err := getIntervalFunction(nil, funcArgs, chunk.Row{}, unit)
	if isNull || err != nil {
		return nil, err
	}
	sec, err := baseDateArithmitical.returnIntervalSeconds(nil, interval, unit)
	if err != nil {
		return nil, err
	}
	res[1] = &Constant{
		Value:   types.NewIntDatum(sec),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	return res, nil
}

// NewFunction creates a new scalar function or constant via a constant folding.
func NewFunction(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, true, funcName, retType, args...)
}

// NewFunctionBase creates a new scalar function with no constant folding.
func NewFunctionBase(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, false, funcName, retType, args...)
}

// ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// NewFunctionInternal is similar to NewFunction, but do not returns error, should only be used internally.
func NewFunctionInternal(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) Expression {
	expr, err := NewFunction(ctx, funcName, retType, args...)
	terror.Log(err)
	return expr
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schema *Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.GetArgs()[i] = arg.Decorrelate(schema)
	}
	return sf
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	if f, ok := sf.Function.(builtinFuncNew); ok {
		return f.evalIntWithCtx(ctx, row)
	}
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	return sf.Function.evalString(row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	return sf.Function.evalTime(row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	return sf.Function.evalDuration(row)
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	return &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(sf.hashcode) > 0 {
		return sf.hashcode
	}
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, hack.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		sf.hashcode = append(sf.hashcode, arg.HashCode(sc)...)
	}
	return sf.hashcode
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row chunk.Row) (d types.Datum, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch tp, evalType := sf.GetType(), sf.GetType().EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(sf.GetCtx(), row)
		if mysql.HasUnsignedFlag(tp.Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(sf.GetCtx(), row)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(sf.GetCtx(), row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(sf.GetCtx(), row)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(sf.GetCtx(), row)
	// case types.ETJson:
	// 	res, isNull, err = sf.EvalJSON(sf.GetCtx(), row)
	case types.ETString:
		res, isNull, err = sf.EvalString(sf.GetCtx(), row)
	}

	if isNull || err != nil {
		d.SetValue(nil)
		return d, err
	}
	d.SetValue(res)
	return
}
