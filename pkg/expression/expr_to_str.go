// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/charset"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"
)

// ExprConverter converts expression to string
type ExprConverter struct {
}

func (pc ExprConverter) ExprToStrWithColString(expr Expression, colToString map[int64]string, qualified bool) (string, error) {
	switch x := expr.(type) {
	case *Constant:
		return pc.constantToStr(x)
	case *CorrelatedColumn:
		return pc.corColToStr(x)
	case *Column:
		return pc.columnToStr(x, colToString, qualified)
	case *ScalarFunction:
		return pc.scalarFuncToStr(x, colToString, qualified)
	default:
		return "", errors.Errorf("Unknown expression type: %v", expr)
	}
}

func (pc ExprConverter) constantToStr(constant *Constant) (string, error) {
	if constant.Value.Kind() == types.KindString {
		return strconv.Quote(constant.String()), nil
	}
	return constant.String(), nil
}

func (pc ExprConverter) corColToStr(col *CorrelatedColumn) (string, error) {
	return pc.columnToStr(&col.Column, map[int64]string{}, true)
}

func (pc ExprConverter) columnToStr(column *Column, colToString map[int64]string, qualified bool) (string, error) {
	if s, ok := colToString[column.UniqueID]; ok {
		return s, nil
	}
	if qualified {
		return column.OrigName, nil
	}
	// origin name of window funcs may be nil, it's name stored in p.names
	if column.OrigName == "" {
		return "", nil
	}
	// column (not scalar function) can split by "."
	subStrs := strings.Split(column.OrigName, ".")
	if len(subStrs) < 1 {
		return "", fmt.Errorf("err column name %s for split(%s) < 1", column.OrigName, column.OrigName)
	}
	return subStrs[len(subStrs)-1], nil
}

func (pc ExprConverter) typeToStr(ft *types.FieldType) (string, error) {
	switch ft.EvalType() {
	case types.ETString:
		str := ""
		if ft.Charset == charset.CharsetBin && ft.Collate == charset.CollationBin {
			str += "binary"
		} else {
			str += "char"
		}
		if ft.Flen != types.UnspecifiedLength {
			str += fmt.Sprintf("(%d)", ft.Flen)
		}
		if ft.Flag&mysql.BinaryFlag != 0 {
			str += " binary"
		}
		if ft.Charset != charset.CharsetBin && ft.Charset != mysql.DefaultCharset {
			str += " charset " + ft.Charset
		}
		return str, nil
	case types.ETDecimal:
		str := "decimal"
		str += fmt.Sprintf("(%d, %d)", ft.Flen, ft.Decimal)
		return str, nil
	case types.ETReal:
		str := "decimal"
		maxIntPart := 64
		maxFractionalPart := 30
		str += fmt.Sprintf("(%d, %d)", maxIntPart, maxFractionalPart)
		return str, nil
	case types.ETInt:
		if ft.Flag&mysql.UnsignedFlag != 0 {
			return "unsigned", nil
		} else {
			return "signed", nil
		}
	case types.ETDatetime:
		return "datetime", nil
	default:
		return "", errors.Errorf("Unknown field type: %v", ft.Tp)
	}
}

func (pc ExprConverter) scalarFuncToStr(expr *ScalarFunction, colToString map[int64]string, qualified bool) (string, error) {
	children := make([]string, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		argStr, err := pc.ExprToStrWithColString(arg, colToString, qualified)
		if err != nil {
			return "", fmt.Errorf("scalarFuncToStr: %v", err)
		}
		children = append(children, argStr)
	}
	switch expr.FuncName.L {
	case ast.Ifnull:
		return fmt.Sprintf("ifnull(%s, %s)", children[0], children[1]), nil
	case ast.If:
		return fmt.Sprintf("if(%s, %s, %s)", children[0], children[1], children[2]), nil
	case ast.Cast:
		sub_str, err := pc.ExprToStrWithColString(expr.GetArgs()[0], colToString, qualified)
		if err != nil {
			return "", err
		}
		field_str, err := pc.typeToStr(expr.RetType)
		if err != nil {
			return "", fmt.Errorf("scalarFuncToStr/Cast(%s as %s): %v", sub_str, field_str, err)
		}
		return fmt.Sprintf("cast(%s as %v)", sub_str, field_str), err
	}

	switch expr.Function.(type) {
	case *builtinLogicAndSig:
		return fmt.Sprintf("(%s and %s)", children[0], children[1]), nil
	case *builtinLogicOrSig:
		return fmt.Sprintf("(%s or %s)", children[0], children[1]), nil
	case *builtinLogicXorSig:
		return fmt.Sprintf("(%s xor %s)", children[0], children[1]), nil
	case *builtinLTIntSig, *builtinLTStringSig, *builtinLTRealSig, *builtinLTDecimalSig:
		return fmt.Sprintf("(%s<%s)", children[0], children[1]), nil
	case *builtinEQIntSig, *builtinEQStringSig, *builtinEQRealSig, *builtinEQDecimalSig:
		return fmt.Sprintf("(%s=%s)", children[0], children[1]), nil
	case *builtinLEIntSig, *builtinLEStringSig, *builtinLERealSig, *builtinLEDecimalSig:
		return fmt.Sprintf("(%s<=%s)", children[0], children[1]), nil
	case *builtinGTIntSig, *builtinGTStringSig, *builtinGTRealSig, *builtinGTDecimalSig:
		return fmt.Sprintf("(%s>%s)", children[0], children[1]), nil
	case *builtinGEIntSig, *builtinGEStringSig, *builtinGERealSig, *builtinGEDecimalSig:
		return fmt.Sprintf("(%s>=%s)", children[0], children[1]), nil
	case *builtinNEIntSig, *builtinNEStringSig, *builtinNERealSig, *builtinNEDecimalSig:
		return fmt.Sprintf("(%s<>%s)", children[0], children[1]), nil
	case *builtinInIntSig, *builtinInStringSig:
		return fmt.Sprintf("(%s in (%s))", children[0], strings.Join(children[1:], ",")), nil
	case *builtinCaseWhenIntSig, *builtinCaseWhenDecimalSig, *builtinCaseWhenRealSig, *builtinCaseWhenStringSig:
		var clauses []string
		for i := 0; i < len(children)-1; i = i + 2 {
			clauses = append(clauses, fmt.Sprintf("WHEN %s THEN %s", children[i], children[i+1]))
		}
		if len(children)%2 == 1 {
			clauses = append(clauses, fmt.Sprintf("ELSE %s", children[len(children)-1]))
		}
		return fmt.Sprintf("CASE %s END", strings.Join(clauses, " ")), nil
	case *builtinDecimalIsNullSig, *builtinIntIsNullSig, *builtinRealIsNullSig, *builtinStringIsNullSig:
		return fmt.Sprintf("(%s is null)", children[0]), nil
	case *builtinUnaryNotRealSig, *builtinUnaryNotDecimalSig, *builtinUnaryNotIntSig:
		return fmt.Sprintf("(not(%s))", children[0]), nil
	case *builtinLikeSig:
		return fmt.Sprintf("(%s like %s)", children[0], children[1]), nil
	case *builtinRegexpSig, *builtinRegexpUTF8Sig:
		return fmt.Sprintf("(%s regexp %s)", children[0], children[1]), nil
	case *builtinConcatSig:
		return fmt.Sprintf("concat(%s)", strings.Join(children, ",")), nil
	case *builtinArithmeticPlusRealSig, *builtinArithmeticPlusDecimalSig, *builtinArithmeticPlusIntSig:
		return fmt.Sprintf("(%s + %s)", children[0], children[1]), nil
	case *builtinArithmeticMinusRealSig, *builtinArithmeticMinusDecimalSig, *builtinArithmeticMinusIntSig:
		return fmt.Sprintf("(%s - %s)", children[0], children[1]), nil
	case *builtinArithmeticMultiplyDecimalSig, *builtinArithmeticMultiplyIntSig, *builtinArithmeticMultiplyIntUnsignedSig, *builtinArithmeticMultiplyRealSig:
		return fmt.Sprintf("(%s * %s)", children[0], children[1]), nil
	case *builtinArithmeticDivideRealSig, *builtinArithmeticDivideDecimalSig:
		return fmt.Sprintf("(%s / %s)", children[0], children[1]), nil
	case *builtinArithmeticIntDivideIntSig:
		return fmt.Sprintf("(%s DIV %s)", children[0], children[1]), nil
	case *builtinSubstring2ArgsSig, *builtinSubstring2ArgsUTF8Sig, *builtinSubstring3ArgsSig, *builtinSubstring3ArgsUTF8Sig:
		return fmt.Sprintf("substring(%s)", strings.Join(children, ",")), nil
	case *builtinGreatestDecimalSig, *builtinGreatestIntSig, *builtinGreatestRealSig, *builtinGreatestStringSig:
		return fmt.Sprintf("greatest(%s)", strings.Join(children, ",")), nil
	case *builtinLeastDecimalSig, *builtinLeastIntSig, *builtinLeastRealSig, *builtinLeastStringSig:
		return fmt.Sprintf("least(%s)", strings.Join(children, ",")), nil
	case *builtinUpperSig, *builtinUpperUTF8Sig:
		return fmt.Sprintf("upper(%s)", children[0]), nil
	case *builtinLowerSig:
		return fmt.Sprintf("lower(%s)", children[0]), nil
	case *builtinCoalesceDecimalSig, *builtinCoalesceIntSig, *builtinCoalesceRealSig, *builtinCoalesceStringSig:
		return fmt.Sprintf("coalesce(%s)", strings.Join(children, ",")), nil
	case *builtinLengthSig:
		return fmt.Sprintf("length(%s)", children[0]), nil
	case *builtinReplaceSig:
		return fmt.Sprintf("replace(%s)", strings.Join(children, ",")), nil
	case *builtinCosSig:
		return fmt.Sprintf("cos(%s)", children[0]), nil
	case *builtinAbsDecSig, *builtinAbsIntSig, *builtinAbsRealSig, *builtinAbsUIntSig:
		return fmt.Sprintf("abs(%s)", children[0]), nil
	case *builtinCeilDecToDecSig, *builtinCeilDecToIntSig, *builtinCeilIntToDecSig, *builtinCeilIntToIntSig,
		*builtinCeilRealSig:
		return fmt.Sprintf("ceil(%s)", children[0]), nil
	case *builtinFloorDecToDecSig, *builtinFloorDecToIntSig, *builtinFloorIntToDecSig, *builtinFloorIntToIntSig,
		*builtinFloorRealSig:
		return fmt.Sprintf("floor(%s)", children[0]), nil
	case *builtinLog1ArgSig, *builtinLog2ArgsSig:
		return fmt.Sprintf("log(%s)", strings.Join(children, ",")), nil
	case *builtinLog2Sig:
		return fmt.Sprintf("log2(%s)", children[0]), nil
	case *builtinLog10Sig:
		return fmt.Sprintf("log10(%s)", children[0]), nil
	case *builtinRandSig, *builtinRandWithSeedFirstGenSig:
		if len(children) == 0 {
			return "rand()", nil
		} else {
			return fmt.Sprintf("rand(%s)", strings.Join(children, ",")), nil
		}
	case *builtinPowSig:
		return fmt.Sprintf("pow(%s)", strings.Join(children, ",")), nil
	case *builtinConvSig:
		return fmt.Sprintf("conv(%s)", strings.Join(children, ",")), nil
	case *builtinCRC32Sig:
		return fmt.Sprintf("crc32(%s)", children[0]), nil
	case *builtinSignSig:
		return fmt.Sprintf("sign(%s)", children[0]), nil
	case *builtinSqrtSig:
		return fmt.Sprintf("sqrt(%s)", children[0]), nil
	case *builtinAcosSig:
		return fmt.Sprintf("acos(%s)", children[0]), nil
	case *builtinAsinSig:
		return fmt.Sprintf("asin(%s)", children[0]), nil
	case *builtinAtan1ArgSig:
		return fmt.Sprintf("atan(%s)", children[0]), nil
	case *builtinAtan2ArgsSig:
		return fmt.Sprintf("atan(%s)", strings.Join(children, ",")), nil
	case *builtinCotSig:
		return fmt.Sprintf("cot(%s)", children[0]), nil
	case *builtinDegreesSig:
		return fmt.Sprintf("degrees(%s)", children[0]), nil
	case *builtinExpSig:
		return fmt.Sprintf("exp(%s)", strings.Join(children, ",")), nil
	case *builtinPISig:
		return "pi()", nil
	case *builtinRadiansSig:
		return fmt.Sprintf("radians(%s)", children[0]), nil
	case *builtinSinSig:
		return fmt.Sprintf("sin(%s)", children[0]), nil
	case *builtinTanSig:
		return fmt.Sprintf("tan(%s)", children[0]), nil
	case *builtinTruncateDecimalSig, *builtinTruncateIntSig, *builtinTruncateRealSig,
		*builtinTruncateUintSig:
		return fmt.Sprintf("truncate(%s)", strings.Join(children, ",")), nil
	case *builtinRoundDecSig, *builtinRoundIntSig, *builtinRoundRealSig, *builtinRoundWithFracDecSig,
		*builtinRoundWithFracIntSig, *builtinRoundWithFracRealSig:
		return fmt.Sprintf("round(%s)", strings.Join(children, ",")), nil
	case *builtinArithmeticModIntSig:
		return fmt.Sprintf("mod(%s, %s)", children[0], children[1]), nil
	case *builtinInstrSig, *builtinInstrUTF8Sig:
		return fmt.Sprintf("instr(%s, %s)", children[0], children[1]), nil
	case *builtinUnaryMinusDecimalSig, *builtinUnaryMinusIntSig, *builtinUnaryMinusRealSig:
		return fmt.Sprintf("-%s", children[0]), nil
	case *builtinDateDiffSig:
		return fmt.Sprintf("datediff(%s, %s)", children[0], children[1]), nil
	case *builtinAddDateDatetimeDecimalSig, *builtinAddDateDatetimeIntSig, *builtinAddDateDatetimeRealSig, *builtinAddDateDatetimeStringSig,
		*builtinAddDateDurationDecimalSig, *builtinAddDateDurationIntSig, *builtinAddDateDurationRealSig, *builtinAddDateDurationStringSig,
		*builtinAddDateIntDecimalSig, *builtinAddDateIntIntSig, *builtinAddDateIntRealSig, *builtinAddDateIntStringSig,
		*builtinAddDateStringDecimalSig, *builtinAddDateStringIntSig, *builtinAddDateStringRealSig, *builtinAddDateStringStringSig:
		return fmt.Sprintf("date_add(%s, interval %s %s)", children[0], children[1], strings.Trim(children[2], "\"")), nil
	case *builtinSubDateDatetimeDecimalSig, *builtinSubDateDatetimeIntSig, *builtinSubDateDatetimeRealSig, *builtinSubDateDatetimeStringSig,
		*builtinSubDateDurationDecimalSig, *builtinSubDateDurationIntSig, *builtinSubDateDurationRealSig, *builtinSubDateDurationStringSig,
		*builtinSubDateIntDecimalSig, *builtinSubDateIntIntSig, *builtinSubDateIntRealSig, *builtinSubDateIntStringSig,
		*builtinSubDateStringDecimalSig, *builtinSubDateStringIntSig, *builtinSubDateStringRealSig, *builtinSubDateStringStringSig:
		return fmt.Sprintf("date_sub(%s, interval %s %s)", children[0], children[1], strings.Trim(children[2], "\"")), nil
	case *builtinDateFormatSig:
		return fmt.Sprintf("date_format(%s, %s)", children[0], children[1]), nil
	case *builtinStrToDateDateSig, *builtinStrToDateDatetimeSig, *builtinStrToDateDurationSig:
		return fmt.Sprintf("str_to_date(%s, %s)", children[0], children[1]), nil
	case *builtinCurrentDateSig:
		return fmt.Sprint("curdate()"), nil
	case *builtinLastDaySig:
		return fmt.Sprintf("last_day(%s)", children[0]), nil
	default:
		return "", errors.Errorf("Unknown expr: %+v", expr.Function)
	}

}
