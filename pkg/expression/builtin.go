// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"fmt"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/opcode"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
)

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type baseBuiltinFunc struct {
	args []Expression
	ctx  sessionctx.Context
	tp   *types.FieldType
	//pbCode tipb.ScalarFuncSig
}

//func (b *baseBuiltinFunc) setPbCode(c tipb.ScalarFuncSig) {
//	b.pbCode = c
//}

func newBaseBuiltinFunc(ctx sessionctx.Context, args []Expression) baseBuiltinFunc {
	if ctx == nil {
		panic("ctx should not be nil")
	}
	return baseBuiltinFunc{
		args: args,
		ctx:  ctx,
		tp:   types.NewFieldType(mysql.TypeUnspecified),
	}
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

func (b *baseBuiltinFunc) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalTime() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDuration() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalJSON() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalInt(row chunk.Row) (int64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalReal(row chunk.Row) (float64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalString(row chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	return nil, false, errors.Errorf("baseBuiltinFunc.evalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroTime, false, errors.Errorf("baseBuiltinFunc.evalTime() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.Duration{}, false, errors.Errorf("baseBuiltinFunc.evalDuration() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) getRetTp() *types.FieldType {
	return b.tp
}

func (b *baseBuiltinFunc) equal(fun builtinFunc) bool {
	funArgs := fun.getArgs()
	if len(funArgs) != len(b.args) {
		return false
	}
	for i := range b.args {
		if !b.args[i].Equal(b.ctx, funArgs[i]) {
			return false
		}
	}
	return true
}

func (b *baseBuiltinFunc) getCtx() sessionctx.Context {
	return b.ctx
}

func (b *baseBuiltinFunc) cloneFrom(from *baseBuiltinFunc) {
	b.args = make([]Expression, 0, len(b.args))
	for _, arg := range from.args {
		b.args = append(b.args, arg.Clone())
	}
	b.ctx = from.ctx
	b.tp = from.tp
}

func (b *baseBuiltinFunc) setDecimalAndFlenForDatetime(fsp int) {
	b.tp.Decimal = fsp
	b.tp.Flen = mysql.MaxDatetimeWidthNoFsp + fsp
	if fsp > 0 {
		// Add the length for `.`.
		b.tp.Flen++
	}
}

func (b *baseBuiltinFunc) setDecimalAndFlenForDate() {
	b.tp.Decimal = 0
	b.tp.Flen = mysql.MaxDateWidth
	b.tp.Tp = mysql.TypeDate
}

func (b *baseBuiltinFunc) setDecimalAndFlenForTime(fsp int) {
	b.tp.Decimal = fsp
	b.tp.Flen = mysql.MaxDurationWidthNoFsp + fsp
	if fsp > 0 {
		// Add the length for `.`.
		b.tp.Flen++
	}
}

type builtinFunc interface {
	// evalInt evaluates int result of builtinFunc by given row.
	evalInt(row chunk.Row) (val int64, isNull bool, err error)
	// evalReal evaluates real representation of builtinFunc by given row.
	evalReal(row chunk.Row) (val float64, isNull bool, err error)
	// evalString evaluates string representation of builtinFunc by given row.
	evalString(row chunk.Row) (val string, isNull bool, err error)
	// evalDecimal evaluates decimal representation of builtinFunc by given row.
	evalDecimal(row chunk.Row) (val *types.MyDecimal, isNull bool, err error)
	// evalTime evaluates DATE/DATETIME/TIMESTAMP representation of builtinFunc by given row.
	evalTime(row chunk.Row) (val types.Time, isNull bool, err error)
	// evalDuration evaluates duration representation of builtinFunc by given row.
	evalDuration(row chunk.Row) (val types.Duration, isNull bool, err error)
	// getArgs returns the arguments expressions.
	getArgs() []Expression
	// equal check if this function equals to another function.
	equal(builtinFunc) bool
	// getCtx returns this function's context.
	getCtx() sessionctx.Context
	// getRetTp returns the return type of the built-in function.
	getRetTp() *types.FieldType
	//// setPbCode sets pbCode for signature.
	//setPbCode(tipb.ScalarFuncSig)

	// Clone returns a copy of itself.
	Clone() builtinFunc
}

type builtinFuncNew interface {
	evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)
}

// functionClass is the interface for a function which may contains multiple functions.
type functionClass interface {
	// getFunction gets a function signature by the types and the counts of given arguments.
	getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error)
}

// baseFunctionClass will be contained in every struct that implement functionClass interface.
type baseFunctionClass struct {
	funcName string
	minArgs  int
	maxArgs  int
}

func (b *baseFunctionClass) verifyArgs(args []Expression) error {
	l := len(args)
	if l < b.minArgs || (b.maxArgs != -1 && l > b.maxArgs) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(b.funcName)
	}
	return nil
}

// newBaseBuiltinFuncWithTp creates a built-in function signature with specified types of arguments and the return type of the function.
// argTps indicates the types of the args, retType indicates the return type of the built-in function.
// Every built-in function needs determined argTps and retType when we create it.
func newBaseBuiltinFuncWithTp(ctx sessionctx.Context, args []Expression, retType types.EvalType, argTps ...types.EvalType) (bf baseBuiltinFunc) {
	if len(args) != len(argTps) {
		panic("unexpected length of args and argTps")
	}
	if ctx == nil {
		panic("ctx should not be nil")
	}

	/* NOTE(yang.y): disable type casting
	for i := range args {
		switch argTps[i] {
		case types.ETInt:
			args[i] = WrapWithCastAsInt(ctx, args[i])
		case types.ETReal:
			args[i] = WrapWithCastAsReal(ctx, args[i])
		case types.ETDecimal:
			args[i] = WrapWithCastAsDecimal(ctx, args[i])
		case types.ETString:
			args[i] = WrapWithCastAsString(ctx, args[i])
		case types.ETDatetime:
			args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeDatetime))
		case types.ETTimestamp:
			args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeTimestamp))
		case types.ETDuration:
			args[i] = WrapWithCastAsDuration(ctx, args[i])
		case types.ETJson:
			args[i] = WrapWithCastAsJSON(ctx, args[i])
		}
	}
	*/

	var fieldType *types.FieldType
	switch retType {
	case types.ETInt:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETString:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
		}
	case types.ETDecimal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeNewDecimal,
			Flen:    11,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETReal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDouble,
			Flen:    mysql.MaxRealWidth,
			Decimal: types.UnspecifiedLength,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDatetime:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDatetime,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETTimestamp:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeTimestamp,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDuration:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDuration,
			Flen:    mysql.MaxDurationWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	// TODO(yang.y): add more cases
	default:
		panic(fmt.Sprintf("generateCmpSigs: unsupported op type %v. Please contact yang.y for details", retType))
	}
	bf = baseBuiltinFunc{
		args: args,
		ctx:  ctx,
		tp:   fieldType,
	}
	return bf
}

// funcs holds all registered builtin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]functionClass{
	// common functions
	ast.IsNull: &isNullFunctionClass{baseFunctionClass{ast.IsNull, 1, 1}},

	// string functions
	ast.Concat: &concatFunctionClass{baseFunctionClass{ast.Concat, 1, -1}},

	// control functions
	ast.If:       &ifFunctionClass{baseFunctionClass{ast.If, 3, 3}},
	ast.Ifnull:   &ifNullFunctionClass{baseFunctionClass{ast.Ifnull, 2, 2}},
	ast.Greatest: &greatestFunctionClass{baseFunctionClass{ast.Greatest, 2, -1}},
	ast.Least:    &leastFunctionClass{baseFunctionClass{ast.Least, 2, -1}},

	ast.LogicAnd:   &logicAndFunctionClass{baseFunctionClass{ast.LogicAnd, 2, 2}},
	ast.LogicOr:    &logicOrFunctionClass{baseFunctionClass{ast.LogicOr, 2, 2}},
	ast.LogicXor:   &logicXorFunctionClass{baseFunctionClass{ast.LogicXor, 2, 2}},
	ast.GE:         &compareFunctionClass{baseFunctionClass{ast.GE, 2, 2}, opcode.GE},
	ast.LE:         &compareFunctionClass{baseFunctionClass{ast.LE, 2, 2}, opcode.LE},
	ast.NE:         &compareFunctionClass{baseFunctionClass{ast.NE, 2, 2}, opcode.NE},
	ast.EQ:         &compareFunctionClass{baseFunctionClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.LT:         &compareFunctionClass{baseFunctionClass{ast.LT, 2, 2}, opcode.LT},
	ast.GT:         &compareFunctionClass{baseFunctionClass{ast.GT, 2, 2}, opcode.GT},
	ast.UnaryNot:   &unaryNotFunctionClass{baseFunctionClass{ast.UnaryNot, 1, 1}},
	ast.UnaryMinus: &unaryMinusFunctionClass{baseFunctionClass{ast.UnaryMinus, 1, 1}},
	ast.Case:       &caseWhenFunctionClass{baseFunctionClass{ast.Case, 1, -1}},
	ast.Like:       &likeFunctionClass{baseFunctionClass{ast.Like, 3, 3}},
	ast.Regexp:     &regexpFunctionClass{baseFunctionClass{ast.Regexp, 2, 2}},
	ast.Plus:       &arithmeticPlusFunctionClass{baseFunctionClass{ast.Plus, 2, 2}},
	ast.Minus:      &arithmeticMinusFunctionClass{baseFunctionClass{ast.Minus, 2, 2}},
	ast.Div:        &arithmeticDivideFunctionClass{baseFunctionClass{ast.Div, 2, 2}},
	ast.Mul:        &arithmeticMultiplyFunctionClass{baseFunctionClass{ast.Mul, 2, 2}},
	ast.IntDiv:     &arithmeticIntDivideFunctionClass{baseFunctionClass{ast.IntDiv, 2, 2}},
	ast.Mod:        &arithmeticModFunctionClass{baseFunctionClass{ast.Mod, 2, 2}},

	ast.In:        &inFunctionClass{baseFunctionClass{ast.In, 2, -1}},
	ast.Substring: &substringFunctionClass{baseFunctionClass{ast.Substring, 2, 3}},
	ast.Lower:     &lowerFunctionClass{baseFunctionClass{ast.Lower, 1, 1}},
	ast.Upper:     &upperFunctionClass{baseFunctionClass{ast.Upper, 1, 1}},
	ast.Coalesce:  &coalesceFunctionClass{baseFunctionClass{ast.Coalesce, 1, -1}},
	ast.Length:    &lengthFunctionClass{baseFunctionClass{ast.Length, 1, 1}},
	ast.Replace:   &replaceFunctionClass{baseFunctionClass{ast.Replace, 3, 3}},
	ast.Trim:      &trimFunctionClass{baseFunctionClass{ast.Trim, 1, 1}},

	// math functions
	ast.Abs:      &absFunctionClass{baseFunctionClass{ast.Abs, 1, 1}},
	ast.Acos:     &acosFunctionClass{baseFunctionClass{ast.Acos, 1, 1}},
	ast.Asin:     &asinFunctionClass{baseFunctionClass{ast.Asin, 1, 1}},
	ast.Atan:     &atanFunctionClass{baseFunctionClass{ast.Atan, 1, 2}},
	ast.Atan2:    &atanFunctionClass{baseFunctionClass{ast.Atan2, 2, 2}},
	ast.Ceil:     &ceilFunctionClass{baseFunctionClass{ast.Ceil, 1, 1}},
	ast.Ceiling:  &ceilFunctionClass{baseFunctionClass{ast.Ceiling, 1, 1}},
	ast.Conv:     &convFunctionClass{baseFunctionClass{ast.Conv, 3, 3}},
	ast.Cos:      &cosFunctionClass{baseFunctionClass{ast.Cos, 1, 1}},
	ast.Cot:      &cotFunctionClass{baseFunctionClass{ast.Cot, 1, 1}},
	ast.CRC32:    &crc32FunctionClass{baseFunctionClass{ast.CRC32, 1, 1}},
	ast.Degrees:  &degreesFunctionClass{baseFunctionClass{ast.Degrees, 1, 1}},
	ast.Exp:      &expFunctionClass{baseFunctionClass{ast.Exp, 1, 1}},
	ast.Floor:    &floorFunctionClass{baseFunctionClass{ast.Floor, 1, 1}},
	ast.Ln:       &logFunctionClass{baseFunctionClass{ast.Ln, 1, 1}},
	ast.Log:      &logFunctionClass{baseFunctionClass{ast.Log, 1, 2}},
	ast.Log2:     &log2FunctionClass{baseFunctionClass{ast.Log2, 1, 1}},
	ast.Log10:    &log10FunctionClass{baseFunctionClass{ast.Log10, 1, 1}},
	ast.PI:       &piFunctionClass{baseFunctionClass{ast.PI, 0, 0}},
	ast.Pow:      &powFunctionClass{baseFunctionClass{ast.Pow, 2, 2}},
	ast.Power:    &powFunctionClass{baseFunctionClass{ast.Power, 2, 2}},
	ast.Radians:  &radiansFunctionClass{baseFunctionClass{ast.Radians, 1, 1}},
	ast.Rand:     &randFunctionClass{baseFunctionClass{ast.Rand, 0, 1}},
	ast.Round:    &roundFunctionClass{baseFunctionClass{ast.Round, 1, 2}},
	ast.Sign:     &signFunctionClass{baseFunctionClass{ast.Sign, 1, 1}},
	ast.Sin:      &sinFunctionClass{baseFunctionClass{ast.Sin, 1, 1}},
	ast.Sqrt:     &sqrtFunctionClass{baseFunctionClass{ast.Sqrt, 1, 1}},
	ast.Tan:      &tanFunctionClass{baseFunctionClass{ast.Tan, 1, 1}},
	ast.Truncate: &truncateFunctionClass{baseFunctionClass{ast.Truncate, 2, 2}},
	ast.Instr:    &instrFunctionClass{baseFunctionClass{ast.Instr, 2, 2}},

	// time
	ast.DateDiff:    &dateDiffFunctionClass{baseFunctionClass{ast.DateDiff, 2, 2}},
	ast.AddDate:     &addDateFunctionClass{baseFunctionClass{ast.AddDate, 3, 3}},
	ast.DateAdd:     &addDateFunctionClass{baseFunctionClass{ast.DateAdd, 3, 3}},
	ast.SubDate:     &subDateFunctionClass{baseFunctionClass{ast.SubDate, 3, 3}},
	ast.DateSub:     &subDateFunctionClass{baseFunctionClass{ast.DateSub, 3, 3}},
	ast.AddTime:     &addTimeFunctionClass{baseFunctionClass{ast.AddTime, 2, 2}},
	ast.SubTime:     &subTimeFunctionClass{baseFunctionClass{ast.SubTime, 2, 2}},
	ast.TimeDiff:    &timeDiffFunctionClass{baseFunctionClass{ast.TimeDiff, 2, 2}},
	ast.DateFormat:  &dateFormatFunctionClass{baseFunctionClass{ast.DateFormat, 2, 2}},
	ast.StrToDate:   &strToDateFunctionClass{baseFunctionClass{ast.StrToDate, 2, 2}},
	ast.Curdate:     &currentDateFunctionClass{baseFunctionClass{ast.Curdate, 0, 0}},
	ast.CurrentDate: &currentDateFunctionClass{baseFunctionClass{ast.CurrentDate, 0, 0}},
	ast.Curtime:     &currentTimeFunctionClass{baseFunctionClass{ast.Curtime, 0, 1}},
	ast.Now:         &nowFunctionClass{baseFunctionClass{ast.Now, 0, 1}},
	ast.LastDay:     &lastDayFunctionClass{baseFunctionClass{ast.LastDay, 1, 1}},

	//built-in
	ast.GeoDist: &builtinGeoDistFunctionClass{baseFunctionClass{ast.GeoDist, 4, 5}},
}

// baseBuiltinCastFunc will be contained in every struct that implement cast builtinFunc.
type baseBuiltinCastFunc struct {
	baseBuiltinFunc

	// inUnion indicates whether cast is in union context.
	inUnion bool
}

func (b *baseBuiltinCastFunc) cloneFrom(from *baseBuiltinCastFunc) {
	b.baseBuiltinFunc.cloneFrom(&from.baseBuiltinFunc)
	b.inUnion = from.inUnion
}

func newBaseBuiltinCastFunc(builtinFunc baseBuiltinFunc, inUnion bool) baseBuiltinCastFunc {
	return baseBuiltinCastFunc{
		baseBuiltinFunc: builtinFunc,
		inUnion:         inUnion,
	}
}

type builtinSubstring2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring2ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length := int64(len(str))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return str[pos:], false, nil
}

type builtinSubstring2ArgsUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	length := int64(len(runes))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return string(runes[pos:]), false, nil
}

type builtinSubstring3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring3ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLen := int64(len(str))
	if pos < 0 {
		pos += byteLen
	} else {
		pos--
	}
	if pos > byteLen || pos < 0 {
		pos = byteLen
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < byteLen {
		return str[pos:end], false, nil
	}
	return str[pos:], false, nil
}

type builtinSubstring3ArgsUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	numRunes := int64(len(runes))
	if pos < 0 {
		pos += numRunes
	} else {
		pos--
	}
	if pos > numRunes || pos < 0 {
		pos = numRunes
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < numRunes {
		return string(runes[pos:end]), false, nil
	}
	return string(runes[pos:]), false, nil
}
