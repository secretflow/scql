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

	"github.com/secretflow/scql/pkg/parser/terror"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/util/codec"

	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
)

var (
	// One stands for a number 1.
	One = &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Zero stands for a number 0.
	Zero = &Constant{
		Value:   types.NewDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Null stands for null constant.
	Null = &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
)

// Constant stands for a constant value.
type Constant struct {
	Value   types.Datum
	RetType *types.FieldType
	// DeferredExpr holds deferred function in PlanCache cached plan.
	// it's only used to represent non-deterministic functions(see expression.DeferredFunctions)
	// in PlanCache cached plan, so let them can be evaluated until cached item be used.
	DeferredExpr Expression
	// ParamMarker holds param index inside sessionVars.PreparedParams.
	// It's only used to reference a user variable provided in the `EXECUTE` statement or `COM_EXECUTE` binary protocol.
	ParamMarker *ParamMarker
	hashcode    []byte
}

// ParamMarker indicates param provided by COM_STMT_EXECUTE.
type ParamMarker struct {
	ctx   sessionctx.Context
	order int
	tp    types.FieldType
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar() types.Datum {
	sessionVars := d.ctx.GetSessionVars()
	return sessionVars.PreparedParams[d.order]
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.RetType
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx sessionctx.Context, _ chunk.Row) (int64, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return 0, true, err
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToInt64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return 0, true, err
		}
		c.Value.SetInt64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx sessionctx.Context, _ chunk.Row) (float64, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return 0, true, err
		}
		if dt.IsNull() {
			return 0, true, nil
		}
		val, err := dt.ToFloat64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return 0, true, err
		}
		c.Value.SetFloat64(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return 0, true, nil
		}
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToFloat64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sessionctx.Context, _ chunk.Row) (string, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return "", true, err
		}
		if dt.IsNull() {
			return "", true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return "", true, err
		}
		c.Value.SetString(val)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return "", true, nil
		}
	}
	res, err := c.Value.ToString()
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx sessionctx.Context, _ chunk.Row) (*types.MyDecimal, bool, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return nil, true, err
		}
		if dt.IsNull() {
			return nil, true, nil
		}
		c.Value.SetValue(dt.GetValue())
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return nil, true, nil
		}
	}
	res, err := c.Value.ToDecimal(ctx.GetSessionVars().StmtCtx)
	return res, err != nil, err
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx sessionctx.Context, _ chunk.Row) (val types.Time, isNull bool, err error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return types.ZeroTime, true, err
		}
		if dt.IsNull() {
			return types.ZeroTime, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.ZeroTime, true, err
		}
		tim, err := types.ParseDatetime(ctx.GetSessionVars().StmtCtx, val)
		if err != nil {
			return types.ZeroTime, true, err
		}
		c.Value.SetMysqlTime(tim)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.ZeroTime, true, nil
		}
	}
	return c.Value.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx sessionctx.Context, _ chunk.Row) (val types.Duration, isNull bool, err error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return types.Duration{}, true, err
		}
		if dt.IsNull() {
			return types.Duration{}, true, nil
		}
		val, err := dt.ToString()
		if err != nil {
			return types.Duration{}, true, err
		}
		dur, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, val, types.MaxFsp)
		if err != nil {
			return types.Duration{}, true, err
		}
		c.Value.SetMysqlDuration(dur)
	} else {
		if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
			return types.Duration{}, true, nil
		}
	}
	return c.Value.GetMysqlDuration(), false, nil
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// ConstItem implements Expression interface.
func (c *Constant) ConstItem(sc *stmtctx.StatementContext) bool {
	return !sc.UseCache || (c.DeferredExpr == nil)
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	return c
}

// Eval implements Expression interface.
// func (c *Constant) Eval(_ chunk.Row) (types.Datum, error) {
// 	return c.Value, nil
// }

func (c *Constant) getLazyDatum() (dt types.Datum, isLazy bool, err error) {
	return
}

// HashCode implements Expression interface.
func (c *Constant) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(c.hashcode) > 0 {
		return c.hashcode
	}
	_, err := c.Eval(chunk.Row{})
	if err != nil {
		terror.Log(err)
	}
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode, err = codec.EncodeValue(sc, c.hashcode, c.Value)
	if err != nil {
		terror.Log(err)
	}
	return c.hashcode
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx sessionctx.Context, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(chunk.Row{})
	_, err2 := c.Eval(chunk.Row{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ chunk.Row) (types.Datum, error) {
	if dt, lazy, err := c.getLazyDatum(); lazy {
		if err != nil {
			return c.Value, err
		}
		if dt.IsNull() {
			c.Value.SetNull()
			return c.Value, nil
		}
		if c.DeferredExpr != nil {
			sf, sfOk := c.DeferredExpr.(*ScalarFunction)
			if sfOk {
				val, err := dt.ConvertTo(sf.GetCtx().GetSessionVars().StmtCtx, c.RetType)
				if err != nil {
					return dt, err
				}
				return val, nil
			}
		}
		return dt, nil
	}
	return c.Value, nil
}
