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
	"strings"

	"unicode/utf8"

	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/variable"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &instrFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstring2ArgsUTF8Sig{}
	_ builtinFunc = &builtinSubstring3ArgsUTF8Sig{}
	_ builtinFunc = &builtinInstrUTF8Sig{}
	_ builtinFunc = &builtinInstrSig{}
)

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.Flag) || !types.IsNonBinaryStr(argTp) {
		resTp.Flag |= mysql.BinaryFlag
	}
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for i := range args {
		argType := args[i].GetType()
		SetBinFlagOrBinStr(argType, bf.tp)

		if argType.Flen < 0 {
			bf.tp.Flen = mysql.MaxBlobWidth
			log.Errorf("unexpected `Flen` value(-1) in CONCAT's args: arg's index %d", i)
		}
		bf.tp.Flen += argType.Flen
	}
	if bf.tp.Flen >= mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}

	maxAllowedPacket := variable.DefMaxAllowedPacket

	sig := &builtinConcatSig{bf, maxAllowedPacket}
	// sig.setPbCode(tipb.ScalarFuncSig_Concat)
	return sig, nil
}

type builtinConcatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatSig) Clone() builtinFunc {
	newSig := &builtinConcatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinConcatSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(b.ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		if uint64(len(s)+len(d)) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("concat", b.maxAllowedPacket))
			return "", true, nil
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString, types.ETInt}
	if len(args) == 3 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)

	var sig builtinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		sig = &builtinSubstring3ArgsSig{bf}
	case len(args) == 2 && types.IsBinaryStr(argType):
		sig = &builtinSubstring2ArgsSig{bf}
	case len(args) == 3:
		sig = &builtinSubstring3ArgsUTF8Sig{bf}
	case len(args) == 2:
		sig = &builtinSubstring2ArgsUTF8Sig{bf}
	default:
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	return sig, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinUpperSig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_Upper)
	} else {
		sig = &builtinUpperUTF8Sig{bf}
		//sig.setPbCode(tipb.ScalarFuncSig_UpperUTF8)
	}
	return sig, nil
}

type builtinUpperUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinUpperUTF8Sig) Clone() builtinFunc {
	newSig := &builtinUpperUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperUTF8Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return strings.ToUpper(d), false, nil
}

type builtinUpperSig struct {
	baseBuiltinFunc
}

func (b *builtinUpperSig) Clone() builtinFunc {
	newSig := &builtinUpperSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinLowerSig{bf}
	//sig.setPbCode(tipb.ScalarFuncSig_Lower)
	return sig, nil
}

type builtinLowerSig struct {
	baseBuiltinFunc
}

func (b *builtinLowerSig) Clone() builtinFunc {
	newSig := &builtinLowerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	if types.IsBinaryStr(b.args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToLower(d), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = c.fixLength(args)
	for _, a := range args {
		SetBinFlagOrBinStr(a.GetType(), bf.tp)
	}
	sig := &builtinReplaceSig{bf}
	//sig.setPbCode(tipb.ScalarFuncSig_Replace)
	return sig, nil
}

// fixLength calculate the Flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	charLen := args[0].GetType().Flen
	oldStrLen := args[1].GetType().Flen
	diff := args[2].GetType().Flen - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseBuiltinFunc
}

func (b *builtinReplaceSig) Clone() builtinFunc {
	newSig := &builtinReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	oldStr, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	newStr, isNull, err = b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinLengthSig{bf}
	//sig.setPbCode(tipb.ScalarFuncSig_Length)
	return sig, nil
}

type builtinLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinLengthSig) Clone() builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 11
	if types.IsBinaryStr(bf.args[0].GetType()) || types.IsBinaryStr(bf.args[1].GetType()) {
		sig := &builtinInstrSig{bf}
		return sig, nil
	}
	sig := &builtinInstrUTF8Sig{bf}
	return sig, nil
}

type builtinInstrUTF8Sig struct{ baseBuiltinFunc }

func (b *builtinInstrUTF8Sig) Clone() builtinFunc {
	newSig := &builtinInstrUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinInstrSig struct{ baseBuiltinFunc }

func (b *builtinInstrSig) Clone() builtinFunc {
	newSig := &builtinInstrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals INSTR(str,substr), case insensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrUTF8Sig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	str = strings.ToLower(str)

	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	substr = strings.ToLower(substr)

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(idx + 1), false, nil
}

type builtinTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinTrimSig) Clone() builtinFunc {
	newFunction := &builtinTrimSig{}
	newFunction.cloneFrom(&b.baseBuiltinFunc)
	return newFunction
}

type trimFunctionClass struct {
	baseFunctionClass
}

func (c *trimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinTrimSig{bf}
	return sig, nil
}
