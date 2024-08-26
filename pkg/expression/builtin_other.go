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
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

var (
	_ functionClass = &inFunctionClass{}
)

var (
	_ builtinFunc = &builtinInIntSig{}
	_ builtinFunc = &builtinInStringSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType().EvalType()
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	bf.tp.Flen = 1
	bf.tp.Flag |= mysql.IsBooleanFlag
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		sig = &builtinInIntSig{baseBuiltinFunc: bf}
	case types.ETString:
		sig = &builtinInStringSig{baseBuiltinFunc: bf}
	}
	return sig, nil
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseBuiltinFunc
}

func (b *builtinInIntSig) Clone() builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// builtinInStringSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseBuiltinFunc
}

func (b *builtinInStringSig) Clone() builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinGeoDist struct {
	baseBuiltinFunc
}

func (b *builtinGeoDist) Clone() builtinFunc {
	newFunction := &builtinGeoDist{}
	newFunction.cloneFrom(&b.baseBuiltinFunc)
	return newFunction
}

type builtinGeoDistFunctionClass struct {
	baseFunctionClass
}

func (c *builtinGeoDistFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var bf baseBuiltinFunc
	if len(args) == 4 {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal, types.ETReal, types.ETReal)
	} else {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal, types.ETReal, types.ETReal, types.ETReal)
	}

	sig := &builtinGeoDist{bf}
	return sig, nil
}
