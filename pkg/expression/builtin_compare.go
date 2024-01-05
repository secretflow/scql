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

package expression

import (
	"fmt"
	"math"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/opcode"
	"github.com/secretflow/scql/pkg/parser/terror"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
)

var (
	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNEStringSig{}
)

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx sessionctx.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	//if err = c.verifyArgs(rawArgs); err != nil {
	//	return nil, err
	//}
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sessionctx.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, tp, tp)
	bf.tp.Flen = 1
	bf.tp.Flag |= mysql.IsBooleanFlag
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
		// TODO(yang.y): add more cases
		default:
			return nil, fmt.Errorf("generateCmpSigs: unsupported op type %v. Please contact yang.y for details", c.op)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
		case opcode.LE:
			sig = &builtinLERealSig{bf}
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
		case opcode.GE:
			sig = &builtinGERealSig{bf}
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
		case opcode.NE:
			sig = &builtinNERealSig{bf}
		// TODO(yang.y): add more cases
		default:
			return nil, fmt.Errorf("generateCmpSigs: unsupported op type %v. Please contact yang.y for details", c.op)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			// TODO(qunshan.huangqs): add more cases
			// case opcode.NullEQ:
			// 	sig = &builtinNullEQDecimalSig{bf}
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
		// TODO(yang.y): add more cases
		default:
			return nil, fmt.Errorf("generateCmpSigs: unsupported op type %v. Please contact yang.y for details", c.op)
		}
	// TODO(yang.y): add more cases
	default:
		return nil, fmt.Errorf("generateCmpSigs: unsupported type %v. Please contact yang.y for details", tp)
	}
	return
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return types.ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
		return types.ETInt
	} else if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
		return types.ETDecimal
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	return cmpType
	// TODO(yang.y): revisit the original logic
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinLTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLTRealSig) Clone() builtinFunc {
	newSig := &builtinLTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDecimalSig) Clone() builtinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLERealSig struct {
	baseBuiltinFunc
}

func (b *builtinLERealSig) Clone() builtinFunc {
	newSig := &builtinLERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDecimalSig) Clone() builtinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGTRealSig) Clone() builtinFunc {
	newSig := &builtinGTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDecimalSig) Clone() builtinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGERealSig struct {
	baseBuiltinFunc
}

func (b *builtinGERealSig) Clone() builtinFunc {
	newSig := &builtinGERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDecimalSig) Clone() builtinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinEQRealSig) Clone() builtinFunc {
	newSig := &builtinEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNERealSig struct {
	baseBuiltinFunc
}

func (b *builtinNERealSig) Clone() builtinFunc {
	newSig := &builtinNERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDecimalSig) Clone() builtinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

// CompareReal compares two float-point values.
func CompareReal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareInt compares two integers.
func CompareInt(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().Flag), mysql.HasUnsignedFlag(rhsArg.GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

// CompareString compares two strings.
func CompareString(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1)), false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// TODO(teng.t, yang.y): Add more operators like builtin[GT|EQ|LT|...][Real|Int|...]Sig

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int c     onstant).
func tryToConvertConstantInt(ctx sessionctx.Context, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType().EvalType() == types.ETInt {
		return con, false
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	dt, err = dt.ConvertTo(sc, targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   dt,
				RetType: targetFieldType,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:   dt,
		RetType: targetFieldType,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int c     onstant).
func RefineComparedConstant(ctx sessionctx.Context, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	if targetFieldType.Tp == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	intDatum, err = dt.ConvertTo(sc, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   intDatum,
				RetType: &targetFieldType,
			}, true
		}
		return con, false
	}
	c, err := intDatum.CompareDatum(sc, &con.Value)
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:   intDatum,
			RetType: &targetFieldType,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType().EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(sc, types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if c, err = doubleDatum.CompareDatum(sc, &intDatum); err != nil {
				return con, false
			}
			if c != 0 {
				return con, true
			}
			return &Constant{
				Value:   intDatum,
				RetType: &targetFieldType,
			}, false
		}
	}
	return con, false
}

// refineArgs will rewrite the arguments if the compare expression is `int column <cmp> non-int constant` or
// `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
func (c *compareFunctionClass) refineArgs(ctx sessionctx.Context, args []Expression) []Expression {
	return args
	// TODO(yang.y): revisit the original logic
}

func aggregateType(args []Expression) *types.FieldType {
	fieldTypes := make([]*types.FieldType, len(args))
	for i := range fieldTypes {
		fieldTypes[i] = args[i].GetType()
	}
	return types.AggFieldType(fieldTypes)
}

// resolveType4Extremum gets compare type for GREATEST and LEAST and BETWEEN (mainly for datetime).
func resolveType4Extremum(args []Expression) types.EvalType {
	aggType := aggregateType(args)

	var temporalItem *types.FieldType
	if aggType.EvalType().IsStringKind() {
		for i := range args {
			item := args[i].GetType()
			// Find the temporal value in the arguments but prefer DateTime value.
			if types.IsTypeTemporal(item.Tp) {
				if temporalItem == nil || item.Tp == mysql.TypeDatetime {
					temporalItem = item
				}
			}
		}

		if !types.IsTypeTemporal(aggType.Tp) && temporalItem != nil {
			aggType.Tp = temporalItem.Tp
		}
		// TODO: String charset, collation checking are needed.
	}
	return aggType.EvalType()
}

// unsupportedJSONComparison reports warnings while there is a JSON type in least/greatest function's arguments
func unsupportedJSONComparison(ctx sessionctx.Context, args []Expression) {
	for _, arg := range args {
		tp := arg.GetType().Tp
		if tp == mysql.TypeJSON {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedJSONComparison)
			break
		}
	}
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp := resolveType4Extremum(args)
	cmpAsDatetime := false
	if tp == types.ETDatetime || tp == types.ETTimestamp {
		cmpAsDatetime = true
		tp = types.ETString
	} else if tp == types.ETDuration {
		tp = types.ETString
	} else if tp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinGreatestIntSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_GreatestInt)
	case types.ETReal:
		sig = &builtinGreatestRealSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_GreatestReal)
	case types.ETDecimal:
		sig = &builtinGreatestDecimalSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_GreatestDecimal)
	case types.ETString:
		sig = &builtinGreatestStringSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_GreatestString)
	default:
		//sig = &builtinGreatestTimeSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_GreatestTime)
		return nil, fmt.Errorf("unsupported data type %d", tp)
	}
	sig.getRetTp().Flen, sig.getRetTp().Decimal = fixFlenAndDecimalForGreatestAndLeast(args)
	return sig, nil
}

func fixFlenAndDecimalForGreatestAndLeast(args []Expression) (flen, decimal int) {
	for _, arg := range args {
		argFlen, argDecimal := arg.GetType().Flen, arg.GetType().Decimal
		if argFlen > flen {
			flen = argFlen
		}
		if argDecimal > decimal {
			decimal = argDecimal
		}
	}
	return flen, decimal
}

type builtinGreatestIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestIntSig) Clone() builtinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(row chunk.Row) (max int64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestRealSig) Clone() builtinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(row chunk.Row) (max float64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestDecimalSig) Clone() builtinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(row chunk.Row) (max *types.MyDecimal, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v.Compare(max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestStringSig) Clone() builtinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(row chunk.Row) (max string, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if types.CompareString(v, max) > 0 {
			max = v
		}
	}
	return
}

// type builtinGreatestTimeSig struct {
// 	baseBuiltinFunc
// }

// func (b *builtinGreatestTimeSig) Clone() builtinFunc {
// 	newSig := &builtinGreatestTimeSig{}
// 	newSig.cloneFrom(&b.baseBuiltinFunc)
// 	return newSig
// }

// // evalString evals a builtinGreatestTimeSig.
// // See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
// func (b *builtinGreatestTimeSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
// 	var (
// 		strRes  string
// 		timeRes types.Time
// 	)
// 	sc := b.ctx.GetSessionVars().StmtCtx
// 	for i := 0; i < len(b.args); i++ {
// 		v, isNull, err := b.args[i].EvalString(b.ctx, row)
// 		if isNull || err != nil {
// 			return "", true, err
// 		}
// 		t, err := types.ParseDatetime(sc, v)
// 		if err != nil {
// 			if err = handleInvalidTimeError(b.ctx, err); err != nil {
// 				return v, true, err
// 			}
// 		} else {
// 			v = t.String()
// 		}
// 		// In MySQL, if the compare result is zero, than we will try to use the string comparison result
// 		if i == 0 || strings.Compare(v, strRes) > 0 {
// 			strRes = v
// 		}
// 		if i == 0 || t.Compare(timeRes) > 0 {
// 			timeRes = t
// 		}
// 	}
// 	if timeRes.IsZero() {
// 		res = strRes
// 	} else {
// 		res = timeRes.String()
// 	}
// 	return res, false, nil
// }

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp := resolveType4Extremum(args)
	cmpAsDatetime := false
	if tp == types.ETDatetime || tp == types.ETTimestamp {
		cmpAsDatetime = true
		tp = types.ETString
	} else if tp == types.ETDuration {
		tp = types.ETString
	} else if tp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinLeastIntSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_LeastInt)
	case types.ETReal:
		sig = &builtinLeastRealSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_LeastReal)
	case types.ETDecimal:
		sig = &builtinLeastDecimalSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_LeastDecimal)
	case types.ETString:
		sig = &builtinLeastStringSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_LeastString)
	default:
		//sig = &builtinLeastTimeSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_LeastTime)
		return nil, fmt.Errorf("unsupported data type %d", tp)
	}
	sig.getRetTp().Flen, sig.getRetTp().Decimal = fixFlenAndDecimalForGreatestAndLeast(args)
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastIntSig) Clone() builtinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_least
func (b *builtinLeastIntSig) evalInt(row chunk.Row) (min int64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastRealSig) Clone() builtinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(row chunk.Row) (min float64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastDecimalSig) Clone() builtinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(row chunk.Row) (min *types.MyDecimal, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v.Compare(min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastStringSig) Clone() builtinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(row chunk.Row) (min string, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if types.CompareString(v, min) < 0 {
			min = v
		}
	}
	return
}

// type builtinLeastTimeSig struct {
// 	baseBuiltinFunc
// }

// func (b *builtinLeastTimeSig) Clone() builtinFunc {
// 	newSig := &builtinLeastTimeSig{}
// 	newSig.cloneFrom(&b.baseBuiltinFunc)
// 	return newSig
// }

// // evalString evals a builtinLeastTimeSig.
// // See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
// func (b *builtinLeastTimeSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
// 	var (
// 		// timeRes will be converted to a strRes only when the arguments is a valid datetime value.
// 		strRes  string     // Record the strRes of each arguments.
// 		timeRes types.Time // Record the time representation of a valid arguments.
// 	)
// 	sc := b.ctx.GetSessionVars().StmtCtx
// 	for i := 0; i < len(b.args); i++ {
// 		v, isNull, err := b.args[i].EvalString(b.ctx, row)
// 		if isNull || err != nil {
// 			return "", true, err
// 		}
// 		t, err := types.ParseDatetime(sc, v)
// 		if err != nil {
// 			if err = handleInvalidTimeError(b.ctx, err); err != nil {
// 				return v, true, err
// 			}
// 		} else {
// 			v = t.String()
// 		}
// 		if i == 0 || strings.Compare(v, strRes) < 0 {
// 			strRes = v
// 		}
// 		if i == 0 || t.Compare(timeRes) < 0 {
// 			timeRes = t
// 		}
// 	}

// 	if timeRes.IsZero() {
// 		res = strRes
// 	} else {
// 		res = timeRes.String()
// 	}
// 	return res, false, nil
// }

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	fieldTps := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		fieldTps = append(fieldTps, arg.GetType())
	}

	// Use the aggregated field type as retType.
	resultFieldType := types.AggFieldType(fieldTps)
	resultEvalType := types.AggregateEvalType(fieldTps, &resultFieldType.Flag)
	retEvalTp := resultFieldType.EvalType()

	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, retEvalTp, fieldEvalTps...)
	bf.tp.Flag |= resultFieldType.Flag
	resultFieldType.Flen, resultFieldType.Decimal = 0, types.UnspecifiedLength

	// Set retType to BINARY(0) if all arguments are of type NULL.
	if resultFieldType.Tp == mysql.TypeNull {
		types.SetBinChsClnFlag(bf.tp)
	} else {
		maxIntLen := 0
		maxFlen := 0

		// Find the max length of field in `maxFlen`,
		// and max integer-part length in `maxIntLen`.
		for _, argTp := range fieldTps {
			if argTp.Decimal > resultFieldType.Decimal {
				resultFieldType.Decimal = argTp.Decimal
			}
			argIntLen := argTp.Flen
			if argTp.Decimal > 0 {
				argIntLen -= argTp.Decimal + 1
			}

			// Reduce the sign bit if it is a signed integer/decimal
			if !mysql.HasUnsignedFlag(argTp.Flag) {
				argIntLen--
			}
			if argIntLen > maxIntLen {
				maxIntLen = argIntLen
			}
			if argTp.Flen > maxFlen || argTp.Flen == types.UnspecifiedLength {
				maxFlen = argTp.Flen
			}
		}
		// For integer, field length = maxIntLen + (1/0 for sign bit)
		// For decimal, field length = maxIntLen + maxDecimal + (1/0 for sign bit)
		if resultEvalType == types.ETInt || resultEvalType == types.ETDecimal {
			resultFieldType.Flen = maxIntLen + resultFieldType.Decimal
			if resultFieldType.Decimal > 0 {
				resultFieldType.Flen++
			}
			if !mysql.HasUnsignedFlag(resultFieldType.Flag) {
				resultFieldType.Flen++
			}
			bf.tp = resultFieldType
		} else {
			bf.tp.Flen = maxFlen
		}
		// Set the field length to maxFlen for other types.
		if bf.tp.Flen > mysql.MaxDecimalWidth {
			bf.tp.Flen = mysql.MaxDecimalWidth
		}
	}

	switch retEvalTp {
	case types.ETInt:
		sig = &builtinCoalesceIntSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		sig = &builtinCoalesceRealSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		sig = &builtinCoalesceDecimalSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		sig = &builtinCoalesceStringSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_CoalesceString)
		// case types.ETDatetime, types.ETTimestamp:
		// 	sig = &builtinCoalesceTimeSig{bf}
		// 	//sig.setPbCode(tipb.ScalarFuncSig_CoalesceTime)
		// case types.ETDuration:
		// 	bf.tp.Decimal, err = getExpressionFsp(ctx, args[0])
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	sig = &builtinCoalesceDurationSig{bf}
		// 	//sig.setPbCode(tipb.ScalarFuncSig_CoalesceDuration)
		// case types.ETJson:
		// 	sig = &builtinCoalesceJSONSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_CoalesceJson)
	}

	return sig, nil
}

// builtinCoalesceIntSig is builtin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceIntSig) Clone() builtinFunc {
	newSig := &builtinCoalesceIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalInt(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceRealSig is builtin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceRealSig) Clone() builtinFunc {
	newSig := &builtinCoalesceRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalReal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDecimalSig is builtin function coalesce signature which return type Decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDecimalSig) Clone() builtinFunc {
	newSig := &builtinCoalesceDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDecimal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceStringSig is builtin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceStringSig) Clone() builtinFunc {
	newSig := &builtinCoalesceStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalString(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceTimeSig is builtin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
// type builtinCoalesceTimeSig struct {
// 	baseBuiltinFunc
// }

// func (b *builtinCoalesceTimeSig) Clone() builtinFunc {
// 	newSig := &builtinCoalesceTimeSig{}
// 	newSig.cloneFrom(&b.baseBuiltinFunc)
// 	return newSig
// }

// func (b *builtinCoalesceTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
// 	for _, a := range b.getArgs() {
// 		res, isNull, err = a.EvalTime(b.ctx, row)
// 		if err != nil || !isNull {
// 			break
// 		}
// 	}
// 	return res, isNull, err
// }

// builtinCoalesceDurationSig is builtin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
// type builtinCoalesceDurationSig struct {
// 	baseBuiltinFunc
// }

// func (b *builtinCoalesceDurationSig) Clone() builtinFunc {
// 	newSig := &builtinCoalesceDurationSig{}
// 	newSig.cloneFrom(&b.baseBuiltinFunc)
// 	return newSig
// }

// func (b *builtinCoalesceDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
// 	for _, a := range b.getArgs() {
// 		res, isNull, err = a.EvalDuration(b.ctx, row)
// 		if err != nil || !isNull {
// 			break
// 		}
// 	}
// 	return res, isNull, err
// }

// builtinCoalesceJSONSig is builtin function coalesce signature which return type json.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
// type builtinCoalesceJSONSig struct {
// 	baseBuiltinFunc
// }

// func (b *builtinCoalesceJSONSig) Clone() builtinFunc {
// 	newSig := &builtinCoalesceJSONSig{}
// 	newSig.cloneFrom(&b.baseBuiltinFunc)
// 	return newSig
// }

// func (b *builtinCoalesceJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
// 	for _, a := range b.getArgs() {
// 		res, isNull, err = a.EvalJSON(b.ctx, row)
// 		if err != nil || !isNull {
// 			break
// 		}
// 	}
// 	return res, isNull, err
// }

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		return CompareString
		// case types.ETDuration:
		// 	return CompareDuration
		// case types.ETDatetime, types.ETTimestamp:
		// 	return CompareTime
		// case types.ETJson:
		// 	return CompareJSON
	}
	return nil
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		return false
	}
	return true
}
