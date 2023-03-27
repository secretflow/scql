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
	"strings"

	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/codec"
)

// CorrelatedColumn stands for a column in a correlated sub query.
type CorrelatedColumn struct {
	Column

	Data *types.Datum
}

type Column struct {
	RetType *types.FieldType
	// ID is used to specify whether this column is ExtraHandleColumn or to access histogram.
	// We'll try to remove it in the future.
	ID int64
	// UniqueID is the unique id of this column.
	UniqueID int64

	// Index is used for execution, to tell the column's position in the given row.
	Index int

	hashcode []byte

	// VirtualExpr is used to save expression for virtual column
	VirtualExpr Expression

	OrigName string // OrigName usually in format DBName.TableName.ColumnName
	IsHidden bool

	// InOperand indicates whether this column is the inner operand of column equal condition converted
	// from `[not] in (subq)`.
	InOperand      bool
	UseAsThreshold bool
}

func (col *Column) Equal(_ sessionctx.Context, expr Expression) bool {
	if newCol, ok := expr.(*Column); ok {
		return newCol.UniqueID == col.UniqueID
	}
	return false
}

func (col *Column) Clone() Expression {
	newCol := *col
	return &newCol
}

// HashCode implements Expression interface.
func (col *Column) HashCode(_ *stmtctx.StatementContext) []byte {
	if len(col.hashcode) != 0 {
		return col.hashcode
	}
	col.hashcode = make([]byte, 0, 9)
	col.hashcode = append(col.hashcode, columnFlag)
	col.hashcode = codec.EncodeInt(col.hashcode, int64(col.UniqueID))
	return col.hashcode
}

const columnPrefix = "Column#"

func (col *Column) String() string {
	if col.OrigName != "" {
		return col.OrigName
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s%d", columnPrefix, col.UniqueID)
	return builder.String()
}

func (col *Column) GetType() *types.FieldType {
	return col.RetType
}

// Column2Exprs will transfer column slice to expression slice.
func Column2Exprs(cols []*Column) []Expression {
	result := make([]Expression, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}
	return result
}

// Decorrelate implements Expression interface.
func (col *Column) Decorrelate(_ *Schema) Expression {
	return col
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	if col.GetType().Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		if val.IsNull() {
			return 0, true, nil
		}
		res, err := val.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	if row.IsNull(col.Index) {
		return 0, true, nil
	}
	return row.GetInt64(col.Index), false, nil
}

// EvalReal returns real representation of Column.
func (col *Column) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	if row.IsNull(col.Index) {
		return 0, true, nil
	}
	if col.GetType().Tp == mysql.TypeFloat {
		return float64(row.GetFloat32(col.Index)), false, nil
	}
	return row.GetFloat64(col.Index), false, nil
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	if row.IsNull(col.Index) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if col.GetType().Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	val := row.GetString(col.Index)
	return val, false, nil
}

// EvalDecimal returns decimal representation of Column.
func (col *Column) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	if row.IsNull(col.Index) {
		return nil, true, nil
	}
	return row.GetMyDecimal(col.Index), false, nil
}

// Eval implements Expression interface.
func (col *Column) Eval(row chunk.Row) (types.Datum, error) {
	return row.GetDatum(col.Index, col.RetType), nil
}

// ConstItem implements Expression interface.
func (col *Column) ConstItem(_ *stmtctx.StatementContext) bool {
	return false
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Column.
func (col *Column) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	if row.IsNull(col.Index) {
		return types.ZeroTime, true, nil
	}
	return row.GetTime(col.Index), false, nil
}

// EvalDuration returns Duration representation of Column.
func (col *Column) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	if row.IsNull(col.Index) {
		return types.Duration{}, true, nil
	}
	duration := row.GetDuration(col.Index, col.RetType.Decimal)
	return duration, false, nil
}
