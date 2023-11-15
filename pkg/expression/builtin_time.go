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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/chunk"
	"github.com/secretflow/scql/pkg/util/mathutil"
)

var (
	_ functionClass = &dateDiffFunctionClass{}
	_ functionClass = &currentDateFunctionClass{}
	_ functionClass = &currentTimeFunctionClass{}
	_ functionClass = &dateFormatFunctionClass{}
	_ functionClass = &addDateFunctionClass{}
	_ functionClass = &subDateFunctionClass{}
	_ functionClass = &addTimeFunctionClass{}
	_ functionClass = &subTimeFunctionClass{}
	_ functionClass = &timeDiffFunctionClass{}
	_ functionClass = &nowFunctionClass{}
	_ functionClass = &lastDayFunctionClass{}
	_ functionClass = &strToDateFunctionClass{}
	_ functionClass = &dateFormatFunctionClass{}
)

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime, types.ETDatetime)
	sig := &builtinDateDiffSig{bf}
	return sig, nil
}

type builtinDateDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDateDiffSig) Clone() builtinFunc {
	newSig := &builtinDateDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// baseDateArithmitical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmitical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArighmeticalUtil() baseDateArithmitical {
	return baseDateArithmitical{
		intervalRegexp: regexp.MustCompile(`-?[\d]+`),
	}
}

func (du *baseDateArithmitical) getDateFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
	return date, err != nil, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmitical) getDateFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTimeFromInt64(sc, dateInt)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	dateTp := mysql.TypeDate
	if date.Type() == mysql.TypeDatetime || date.Type() == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.SetType(dateTp)
	return date, false, nil
}

func (du *baseDateArithmitical) getDateFromDatetime(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if date.Type() == mysql.TypeDatetime || date.Type() == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.SetType(dateTp)
	return date, false, nil
}

func (du *baseDateArithmitical) getIntervalFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	// unit "DAY" and "HOUR" has to be specially handled.
	if toLower := strings.ToLower(unit); toLower == "day" || toLower == "hour" {
		if strings.ToLower(interval) == "true" {
			interval = "1"
		} else if strings.ToLower(interval) == "false" {
			interval = "0"
		} else {
			interval = du.intervalRegexp.FindString(interval)
		}
	}
	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromDecimal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	decimal, isNull, err := args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	interval := decimal.String()

	switch strings.ToUpper(unit) {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		neg := false
		if interval != "" && interval[0] == '-' {
			neg = true
			interval = interval[1:]
		}
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			interval = strings.Replace(interval, ".", ":", -1)
		case "YEAR_MONTH":
			interval = strings.Replace(interval, ".", "-", -1)
		case "DAY_HOUR":
			interval = strings.Replace(interval, ".", " ", -1)
		case "DAY_MINUTE":
			interval = "0 " + strings.Replace(interval, ".", ":", -1)
		case "DAY_SECOND":
			interval = "0 00:" + strings.Replace(interval, ".", ":", -1)
		case "DAY_MICROSECOND":
			interval = "0 00:00:" + interval
		case "HOUR_MICROSECOND":
			interval = "00:00:" + interval
		case "HOUR_SECOND":
			interval = "00:" + strings.Replace(interval, ".", ":", -1)
		case "MINUTE_MICROSECOND":
			interval = "00:" + interval
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
		if neg {
			interval = "-" + interval
		}
	case "SECOND":
		// interval is already like the %f format.
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		castExpr := WrapWithCastAsString(ctx, WrapWithCastAsInt(ctx, args[1]))
		interval, isNull, err = castExpr.EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmitical) getIntervalFromReal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatFloat(interval, 'f', args[1].GetType().Decimal, 64), false, nil
}

func (du *baseDateArithmitical) returnIntervalSeconds(ctx sessionctx.Context, interval string, unit string) (int64, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return 0, err
	}
	if year != 0 && month != 0 {
		return 0, fmt.Errorf("not supported year and month calc")
	}
	return day*constant.SecondsOneDay + nano/int64(time.Second), nil
}
func (du *baseDateArithmitical) add(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	goTime, err := date.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	goTime = goTime.Add(time.Duration(nano))
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.SetFsp(0)
	} else {
		date.SetFsp(6)
	}

	if goTime.Year() < 0 || goTime.Year() > (1<<16-1) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

func (du *baseDateArithmitical) addDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Add(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) subDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Sub(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) sub(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day, nano = -year, -month, -day, -nano

	goTime, err := date.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	duration := time.Duration(nano)
	goTime = goTime.Add(duration)
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.SetFsp(0)
	} else {
		date.SetFsp(6)
	}

	if goTime.Year() < 0 || goTime.Year() > (1<<16-1) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

type addDateFunctionClass struct {
	baseFunctionClass
}

func (c *addDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration && dateEvalTp != types.ETTimestamp {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else if dateEvalTp == types.ETTimestamp {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETTimestamp, argTps...)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	} else {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case (dateEvalTp == types.ETDatetime || dateEvalTp == types.ETTimestamp) && intervalEvalTp == types.ETInt:
		// when expression rewrite to sql, do not need support diffrent type args for adddate as tidb,
		// so here just support args type like: adddate(col_name, INTERVAL 30 day)
		sig = &builtinAddDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	default:
		return nil, errors.Errorf("Invalid args for adddate func")
	}
	return sig, nil
}

// getExpressionFsp calculates the fsp from given expression.
func getExpressionFsp(ctx sessionctx.Context, expression Expression) (int, error) {
	constExp, isConstant := expression.(*Constant)
	if isConstant && types.IsString(expression.GetType().Tp) && !isTemporalColumn(expression) {
		str, isNil, err := constExp.EvalString(ctx, chunk.Row{})
		if isNil || err != nil {
			return 0, err
		}
		return int(types.GetFsp(str)), nil
	}
	return mathutil.Min(expression.GetType().Decimal, int(types.MaxFsp)), nil
}

type builtinAddDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeIntSig) Clone() builtinFunc {
	newSig := &builtinAddDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type subDateFunctionClass struct {
	baseFunctionClass
}

func (c *subDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration && dateEvalTp != types.ETTimestamp {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else if dateEvalTp == types.ETTimestamp {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETTimestamp, argTps...)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	} else {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case (dateEvalTp == types.ETDatetime || dateEvalTp == types.ETTimestamp) && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	default:
		return nil, errors.Errorf("Invalid args for subdate func")
	}
	return sig, nil
}

type builtinSubDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeIntSig) Clone() builtinFunc {
	newSig := &builtinSubDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// getBf4TimeAddSub parses input types, generates baseBuiltinFunc and set related attributes for
// builtin function 'ADDTIME' and 'SUBTIME'
func getBf4TimeAddSub(ctx sessionctx.Context, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc, err error) {
	tp1, tp2 = args[0].GetType(), args[1].GetType()
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		argTp1, retTp = types.ETDuration, types.ETString
	default:
		argTp1, retTp = types.ETString, types.ETString
	}
	switch tp2.Tp {
	case mysql.TypeDatetime, mysql.TypeDuration:
		argTp2 = types.ETDuration
	default:
		argTp2 = types.ETString
	}
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return
	}

	bf = newBaseBuiltinFuncWithTp(ctx, args, retTp, argTp1, argTp2)
	bf.tp.Decimal = mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), int(types.MaxFsp))
	if retTp == types.ETString {
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeString, mysql.MaxDatetimeWidthWithFsp, types.UnspecifiedLength
	}
	return
}

type addTimeFunctionClass struct {
	baseFunctionClass
}

func (c *addTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, _, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		sig = &builtinAddDatetimeAndStringSig{bf}
	default:
		return nil, errors.Errorf("Invalid args for addtime func")
	}
	return sig, nil
}

type builtinAddDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDatetimeAndStringSig) Clone() builtinFunc {
	newSig := &builtinAddDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, _, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		sig = &builtinSubDatetimeAndStringSig{bf}
	default:
		return nil, errors.Errorf("Invalid args for subtime func")
	}
	return sig, nil
}

type builtinSubDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDatetimeAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type timeDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timeDiffFunctionClass) getArgEvalTp(fieldTp *types.FieldType) types.EvalType {
	argTp := types.ETString
	switch tp := fieldTp.EvalType(); tp {
	case types.ETDuration, types.ETDatetime, types.ETTimestamp:
		argTp = tp
	}
	return argTp
}

func (c *timeDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	arg0FieldTp, arg1FieldTp := args[0].GetType(), args[1].GetType()
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Tp, arg1Tp)

	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return nil, err
	}
	bf.tp.Decimal = mathutil.Max(arg0Dec, arg1Dec)

	var sig builtinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
	// TODO: we don't really need Duration type, actually in MySQL, it use Time class to represent
	// all the time type, and use filed type to distinguish datetime, date, timestamp or time(duration).
	// With the duration type, we are hard to port all the MySQL behavior.
	switch arg0Tp {
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinTimeTimeTimeDiffSig{bf}
	default:
		return nil, errors.Errorf("Invalid args for timediff func")
	}
	return sig, nil
}

type builtinTimeTimeTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeTimeTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinTimeTimeTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime, types.ETString)

	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{bf}
	return sig, nil
}

type builtinDateFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinDateFormatSig) Clone() builtinFunc {
	newSig := &builtinDateFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) evalString(row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, handleInvalidTimeError(b.ctx, err)
	}
	formatMask, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	// MySQL compatibility, #11203
	// If format mask is 0 then return 0 without warnings
	if formatMask == "0" {
		return "0", false, nil
	}

	if t.InvalidZero() {
		// MySQL compatibility, #11203
		// 0 | 0.0 should be converted to null without warnings
		n, err := t.ToNumber().ToInt()
		isOriginalIntOrDecimalZero := err == nil && n == 0
		// Args like "0000-00-00", "0000-00-00 00:00:00" set Fsp to 6
		isOriginalStringZero := t.Fsp() > 0
		if isOriginalIntOrDecimalZero && !isOriginalStringZero {
			return "", true, nil
		}
		return "", true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}

	res, err := t.DateFormat(formatMask)
	return res, isNull, err
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx sessionctx.Context, arg Expression) (tp byte, fsp int) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, int(types.MaxFsp)
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return
	}

	isDuration, isDate := types.GetFormatType(format)
	if isDuration && !isDate {
		tp = mysql.TypeDuration
	} else if !isDuration && isDate {
		tp = mysql.TypeDate
	}
	if strings.Contains(format, "%f") {
		fsp = int(types.MaxFsp)
	}
	return
}

// getFunction see https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp, fsp := c.getRetTp(ctx, args[1])
	switch retTp {
	case mysql.TypeDate:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		bf.setDecimalAndFlenForDate()
		sig = &builtinStrToDateDateSig{bf}
	case mysql.TypeDatetime:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		bf.setDecimalAndFlenForDatetime(fsp)
		sig = &builtinStrToDateDatetimeSig{bf}
	case mysql.TypeDuration:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString, types.ETString)
		bf.setDecimalAndFlenForTime(fsp)
		sig = &builtinStrToDateDurationSig{bf}
	default:
		return nil, fmt.Errorf("unsupported type %v", retTp)
	}
	return sig, nil
}

type builtinStrToDateDateSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDateSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValueForType.GenWithStackByArgs(types.DateTimeStr, date, ast.StrToDate))
	}
	t.SetType(mysql.TypeDate)
	t.SetFsp(types.MinFsp)
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetType(mysql.TypeDatetime)
	t.SetFsp(int8(b.tp.Decimal))
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDurationSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration
// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetFsp(int8(b.tp.Decimal))
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentDateSig) Clone() builtinFunc {
	newSig := &builtinCurrentDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func getFlenAndDecimal4UTCTimestampAndNow(ctx sessionctx.Context, arg Expression) (flen, decimal int) {
	if constant, ok := arg.(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = int(types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			decimal = int(types.MinFsp)
		} else {
			decimal = int(fsp)
		}
	}
	if decimal > 0 {
		flen = 19 + 1 + decimal
	} else {
		flen = 19
	}
	return flen, decimal
}

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	if len(args) != 0 {
		return nil, errors.Errorf("Invalid arg for cur_time func")
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, int(types.MinFsp)
	sig = &builtinCurrentTime0ArgSig{bf}
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) Clone() builtinFunc {
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{bf}
	} else {
		sig = &builtinNowWithoutArgSig{bf}
	}
	return sig, nil
}

type builtinNowWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinNowWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.DefaultFsp)
	sig := &builtinLastDaySig{bf}
	return sig, nil
}

type builtinLastDaySig struct {
	baseBuiltinFunc
}

func (b *builtinLastDaySig) Clone() builtinFunc {
	newSig := &builtinLastDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	tm := arg
	if arg.InvalidZero() {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	year, month := tm.Year(), tm.Month()
	lastDay := types.GetLastDay(year, month)
	ret := types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	return ret, false, nil
}
