// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"math"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
	// UseAsThreshold represents whether the aggregation function(count) uses as a threshold
	UseAsThreshold bool
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression, hasDistinct bool) (*AggFuncDesc, error) {
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &AggFuncDesc{baseFuncDesc: b, HasDistinct: hasDistinct}, nil
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx sessionctx.Context, other *AggFuncDesc) bool {
	if a.HasDistinct != other.HasDistinct {
		return false
	}
	return a.baseFuncDesc.equal(ctx, &other.baseFuncDesc)
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	clone.baseFuncDesc = *a.baseFuncDesc.clone()
	clone.UseAsThreshold = false
	return &clone
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching row for the inner table of an outer join,
// an aggregation function only involves constant and/or columns belongs to the inner table
// will be set to the null value.
// The input stands for the schema of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Table t with only one row:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Table s which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.evalNullValueInOuterJoin4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstRow:
		return a.evalNullValueInOuterJoin4Sum(ctx, schema)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		return types.Datum{}, false
	case ast.AggFuncBitAnd:
		return a.evalNullValueInOuterJoin4BitAnd(ctx, schema)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.evalNullValueInOuterJoin4BitOr(ctx, schema)
	default:
		panic("unsupported agg function")
	}
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	for _, arg := range a.Args {
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			return types.Datum{}, ok
		}
	}
	return types.NewDatum(1), true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.Datum{}, ok
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitAnd(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(uint64(math.MaxUint64)), true
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitOr(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(0), true
	}
	return con.Value, true
}
