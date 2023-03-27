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
	"unicode"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	driver "github.com/secretflow/scql/pkg/types/parser_driver"
	"github.com/secretflow/scql/pkg/util/chunk"
)

// cowExprRef is a copy-on-write slice ref util using in `ColumnSubstitute`
// to reduce unnecessary allocation for Expression arguments array
type cowExprRef struct {
	refExpr []Expression
	newExpr []Expression
}

// Set will allocate new array if changed flag true
func (c *cowExprRef) Set(i int, changed bool, val Expression) {
	if c.newExpr != nil {
		c.newExpr[i] = val
		return
	}
	if !changed {
		return
	}
	c.newExpr = make([]Expression, len(c.refExpr))
	copy(c.newExpr, c.refExpr[:i])
	c.newExpr[i] = val
}

// Result return the final reference
func (c *cowExprRef) Result() []Expression {
	if c.newExpr != nil {
		return c.newExpr
	}
	return c.refExpr
}

func extractColumns(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumns(result, arg, filter)
		}
	}
	return result
}

func ExtractColumns(expr Expression) []*Column {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*Column, 0, 8)
	return extractColumns(result, expr, nil)
}

// ExtractColumnsFromExpressions is a more efficient version of ExtractColumns for batch operation.
// filter can be nil, or a function to filter the result column.
// It's often observed that the pattern of the caller like this:
//
// cols := ExtractColumns(...)
//
//	for _, col := range cols {
//	    if xxx(col) {...}
//	}
//
// Provide an additional filter argument, this can be done in one step.
// To avoid allocation for cols that not need.
func ExtractColumnsFromExpressions(result []*Column, exprs []Expression, filter func(*Column) bool) []*Column {
	for _, expr := range exprs {
		result = extractColumns(result, expr, filter)
	}
	return result
}

// GetRowLen gets the length if the func is row, returns 1 if not row.
func GetRowLen(e Expression) int {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.GetArgs())
	}
	return 1
}

// CheckArgsNotMultiColumnRow checks the args are not multi-column row.
func CheckArgsNotMultiColumnRow(args ...Expression) error {
	for _, arg := range args {
		if GetRowLen(arg) != 1 {
			return ErrOperandColumns.GenWithStackByArgs(1)
		}
	}
	return nil
}

// GetFuncArg gets the argument of the function at idx.
func GetFuncArg(e Expression, idx int) Expression {
	if f, ok := e.(*ScalarFunction); ok {
		return f.GetArgs()[idx]
	}
	return nil
}

// PopRowFirstArg pops the first element and returns the rest of row.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func PopRowFirstArg(ctx sessionctx.Context, e Expression) (ret Expression, err error) {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1], nil
		}
		ret, err = NewFunction(ctx, ast.RowFunc, f.GetType(), args[1:]...)
		return ret, err
	}
	return
}

// ExtractCorColumns extracts correlated column from given expression.
func ExtractCorColumns(expr Expression) (cols []*CorrelatedColumn) {
	switch v := expr.(type) {
	case *CorrelatedColumn:
		return []*CorrelatedColumn{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, ExtractCorColumns(arg)...)
		}
	}
	return
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema *Schema, newExprs []Expression) Expression {
	_, resExpr := ColumnSubstituteImpl(expr, schema, newExprs)
	return resExpr
}

func setExprColumnInOperand(expr Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		col := v.Clone().(*Column)
		col.InOperand = true
		return col
	case *ScalarFunction:
		args := v.GetArgs()
		for i, arg := range args {
			args[i] = setExprColumnInOperand(arg)
		}
	}
	return expr
}

// ColumnSubstituteImpl tries to substitute column expr using newExprs,
// the newFunctionInternal is only called if its child is substituted
func ColumnSubstituteImpl(expr Expression, schema *Schema, newExprs []Expression) (bool, Expression) {
	switch v := expr.(type) {
	case *Column:
		id := schema.ColumnIndex(v)
		if id == -1 {
			return false, v
		}
		newExpr := newExprs[id]
		if v.InOperand {
			newExpr = setExprColumnInOperand(newExpr)
		}
		return true, newExpr
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			_, newFunc.GetArgs()[0] = ColumnSubstituteImpl(newFunc.GetArgs()[0], schema, newExprs)
			return true, newFunc
		}
		// cowExprRef is a copy-on-write util, args array allocation happens only
		// when expr in args is changed
		refExprArr := cowExprRef{v.GetArgs(), nil}
		substituted := false
		for idx, arg := range v.GetArgs() {
			changed, newFuncExpr := ColumnSubstituteImpl(arg, schema, newExprs)
			refExprArr.Set(idx, changed, newFuncExpr)
			if changed {
				substituted = true
			}
		}
		if substituted {
			return true, NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, refExprArr.Result()...)
		}
	}
	return false, expr
}

// ExtractFiltersFromDNFs checks whether the cond is DNF. If so, it will get the extracted part and the remained part.
// The original DNF will be replaced by the remained part or just be deleted if remained part is nil.
// And the extracted part will be appended to the end of the original slice.
func ExtractFiltersFromDNFs(ctx sessionctx.Context, conditions []Expression) []Expression {
	// NOTE(yang.y): ExtractFiltersFromDNFs is an optimization techniques to transform
	// DNF like
	//   (p1 and p2) or (p1 and p3) or (p1 and p4)
	// to
	//   (p2 or p3 or p4) and p1
	//
	// We skip this type of optimization for now
	return conditions
}

// IsMutableEffectsExpr checks if expr contains function which is mutable or has side effects.
func IsMutableEffectsExpr(expr Expression) bool {
	// NOTE(yang.y): Mutable functions list https://github.com/pingcap/tidb/blob/6cb4230c8f2e59bf46497738c99cfacd2e967850/expression/function_traits.go#L126-L163
	// contains functions like time() and rand(), which currently is not supported in SCQL.
	return false
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mutable or have side effects, we cannot remove it even if it has duplicates.
func RemoveDupExprs(ctx sessionctx.Context, exprs []Expression) []Expression {
	res := make([]Expression, 0, len(exprs))
	exists := make(map[string]struct{}, len(exprs))
	sc := ctx.GetSessionVars().StmtCtx
	for _, expr := range exprs {
		key := string(expr.HashCode(sc))
		if _, ok := exists[key]; !ok || IsMutableEffectsExpr(expr) {
			res = append(res, expr)
			exists[key] = struct{}{}
		}
	}
	return res
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c < upper {
				validLen = i + 1
			} else {
				break Loop
			}
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// GetUint64FromConstant gets a uint64 from constant expression.
func GetUint64FromConstant(expr Expression) (uint64, bool, bool) {
	con, ok := expr.(*Constant)
	if !ok {
		return 0, false, false
	}
	dt := con.Value
	if con.ParamMarker != nil {
		dt = con.ParamMarker.GetUserVar()
	} else if con.DeferredExpr != nil {
		var err error
		dt, err = con.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			return 0, false, false
		}
	}
	switch dt.Kind() {
	case types.KindNull:
		return 0, true, true
	case types.KindInt64:
		val := dt.GetInt64()
		if val < 0 {
			return 0, false, false
		}
		return uint64(val), false, true
	case types.KindUint64:
		return dt.GetUint64(), false, true
	}
	return 0, false, false
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	return &ast.PositionExpr{P: p}
}
