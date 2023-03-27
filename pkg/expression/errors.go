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
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/terror"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
)

var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount = terror.ClassExpression.New(mysql.ErrWrongParamcountToNativeFct, mysql.MySQLErrName[mysql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero          = terror.ClassExpression.New(mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	ErrOperandColumns          = terror.ClassExpression.New(mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrRegexp                  = terror.ClassExpression.NewStd(mysql.ErrRegexp)
	// All the un-exported errors are defined here:
	errNonUniq                     = terror.ClassExpression.New(mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
	errFunctionNotExists           = terror.ClassExpression.New(mysql.ErrSpDoesNotExist, mysql.MySQLErrName[mysql.ErrSpDoesNotExist])
	errWarnAllowedPacketOverflowed = terror.ClassExpression.New(mysql.ErrWarnAllowedPacketOverflowed, mysql.MySQLErrName[mysql.ErrWarnAllowedPacketOverflowed])
	errUnsupportedJSONComparison   = terror.ClassExpression.New(mysql.ErrNotSupportedYet, "comparison of JSON in the LEAST and GREATEST operators")
	ErrInvalidArgumentForLogarithm = terror.ClassExpression.NewStd(mysql.ErrInvalidArgumentForLogarithm)
	errIncorrectArgs               = terror.ClassExpression.NewStd(mysql.ErrWrongArguments)
)

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx sessionctx.Context) error {
	sc := ctx.GetSessionVars().StmtCtx
	// NOTE(yang.y): SCQL doesn't support the following statement
	// if sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt {
	// 	if !ctx.GetSessionVars().SQLMode.HasErrorForDivisionByZeroMode() {
	// 		return nil
	// 	}
	// 	if ctx.GetSessionVars().StrictSQLMode && !sc.DividedByZeroAsWarning {
	// 		return ErrDivisionByZero
	// 	}
	// }
	sc.AppendWarning(ErrDivisionByZero)
	return nil
}

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) ||
		types.ErrTruncatedWrongVal.Equal(err) || types.ErrInvalidWeekModeFormat.Equal(err) ||
		types.ErrDatetimeFunctionOverflow.Equal(err)) {
		return err
	}
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StrictSQLMode && (sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt) {
		return err
	}
	sc.AppendWarning(err)
	return nil
}
