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
	"strings"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/format"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/opcode"
	"github.com/secretflow/scql/pkg/types"
	driver "github.com/secretflow/scql/pkg/types/parser_driver"
)

// ExprConverter converts expression to expr node
type ExprConverter struct {
}

const (
	precNonOperatorExpression = 100
)

func (c ExprConverter) ConvertExpressionToExprNode(dialect format.Dialect, expr Expression, prec int, idToExpr map[int64]ast.ExprNode) (ast.ExprNode, error) {
	switch x := expr.(type) {
	case *Constant:
		return c.convertConst(x)
	case *Column:
		if expr, ok := idToExpr[x.UniqueID]; ok {
			return expr, nil
		}
		return c.convertColumn(x)
	case *ScalarFunction:
		return c.convertScalarFunction(dialect, x, prec, idToExpr)
	default:
		return nil, errors.Errorf("Unknown expression type: %v", expr)
	}
}

func (c ExprConverter) isArgScalarFunc(expr Expression) bool {
	switch expr.(type) {
	case *ScalarFunction:
		return true
	}
	return false
}
func (c ExprConverter) convertConst(constant *Constant) (*driver.ValueExpr, error) {
	return &driver.ValueExpr{Datum: constant.Value}, nil
}

func (c ExprConverter) convertColumn(column *Column) (*ast.ColumnNameExpr, error) {
	name := ast.ColumnName{}
	// TODO(@xiaoyuan) origin name of window funcs may be nil, it's name stored in p.names
	// column (not scalar function) can split by "."
	subStrs := strings.Split(column.OrigName, ".")
	if len(subStrs) == 0 || len(subStrs) > 3 {
		return nil, fmt.Errorf("failed to check column name for len(split(%s)) is not in (1,2,3)", column.OrigName)
	}

	name.Name = model.NewCIStr(subStrs[len(subStrs)-1])
	if len(subStrs) >= 2 {
		name.Table = model.NewCIStr(subStrs[len(subStrs)-2])
	}
	if len(subStrs) == 3 {
		name.Schema = model.NewCIStr(subStrs[len(subStrs)-3])
	}
	return &ast.ColumnNameExpr{Name: &name}, nil
}

// for now binary function need Parentheses
// ref to https://dev.mysql.com/doc/refman/8.0/en/operator-precedence.html
// TODO: @xiaoyuan we don't support MEMBER OF/IS/BETWEEN......
func (c ExprConverter) getOpPrecedence(expr *ScalarFunction) int {
	switch expr.Function.(type) {
	case *builtinLogicXorSig:
		return 10
	case *builtinLogicOrSig:
		return 15
	case *builtinLogicAndSig:
		return 20
	case *builtinCaseWhenIntSig, *builtinCaseWhenDecimalSig, *builtinCaseWhenRealSig, *builtinCaseWhenStringSig:
		return 30
	case *builtinLEIntSig, *builtinLEStringSig, *builtinLERealSig, *builtinLEDecimalSig,
		*builtinGTIntSig, *builtinGTStringSig, *builtinGTRealSig, *builtinGTDecimalSig,
		*builtinGEIntSig, *builtinGEStringSig, *builtinGERealSig, *builtinGEDecimalSig,
		*builtinLTIntSig, *builtinLTStringSig, *builtinLTRealSig, *builtinLTDecimalSig,
		*builtinEQIntSig, *builtinEQStringSig, *builtinEQRealSig, *builtinEQDecimalSig,
		*builtinNEIntSig, *builtinNEStringSig, *builtinNERealSig, *builtinNEDecimalSig,
		*builtinLikeSig, *builtinInIntSig, *builtinInStringSig:
		return 40
	case *builtinArithmeticPlusRealSig, *builtinArithmeticPlusDecimalSig,
		*builtinArithmeticPlusIntSig, *builtinArithmeticMinusRealSig,
		*builtinArithmeticMinusDecimalSig, *builtinArithmeticMinusIntSig:
		return 50
	case *builtinArithmeticMultiplyIntUnsignedSig, *builtinArithmeticMultiplyRealSig,
		*builtinArithmeticMultiplyDecimalSig, *builtinArithmeticMultiplyIntSig,
		*builtinArithmeticDivideRealSig, *builtinArithmeticDivideDecimalSig,
		*builtinArithmeticModIntSig, *builtinArithmeticIntDivideIntSig:
		return 60
	case *builtinUnaryMinusDecimalSig, *builtinUnaryMinusIntSig, *builtinUnaryMinusRealSig:
		return 70
	}
	return precNonOperatorExpression
}

func (c ExprConverter) convertScalarFunction(dialect format.Dialect, expr *ScalarFunction, fatherPrec int, idToExpr map[int64]ast.ExprNode) (result ast.ExprNode, err error) {
	children := make([]ast.ExprNode, 0, len(expr.GetArgs()))
	prec := c.getOpPrecedence(expr)
	for _, arg := range expr.GetArgs() {
		argExpr, err := c.ConvertExpressionToExprNode(dialect, arg, prec, idToExpr)
		if err != nil {
			return nil, fmt.Errorf("convertScalarFunction: %v", err)
		}
		children = append(children, argExpr)
	}
	defer func() {
		if fatherPrec != precNonOperatorExpression && (fatherPrec > prec ||
			(fatherPrec != 0 && IsCompareOp(expr) && dialect.NeedParenthesesForCmpOperand())) {
			result = &ast.ParenthesesExpr{Expr: result}
		}
	}()
	switch expr.FuncName.L {
	case ast.Cast:
		return &ast.FuncCastExpr{Expr: children[0], Tp: expr.RetType, FunctionType: ast.CastFunction}, nil
	case ast.Ifnull, ast.Coalesce:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case ast.If:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case ast.Abs, ast.Round, ast.Log10, ast.Ceil, ast.Floor, ast.Instr, ast.Length, ast.Sin, ast.Acos, ast.Cos:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	}
	switch expr.Function.(type) {
	case *builtinUnaryMinusDecimalSig, *builtinUnaryMinusIntSig, *builtinUnaryMinusRealSig:
		return &ast.UnaryOperationExpr{Op: opcode.Minus, V: children[0]}, nil
	case *builtinArithmeticPlusRealSig, *builtinArithmeticPlusDecimalSig, *builtinArithmeticPlusIntSig:
		return &ast.BinaryOperationExpr{Op: opcode.Plus, L: children[0], R: children[1]}, nil
	case *builtinArithmeticMinusRealSig, *builtinArithmeticMinusDecimalSig, *builtinArithmeticMinusIntSig:
		return &ast.BinaryOperationExpr{Op: opcode.Minus, L: children[0], R: children[1]}, nil
	case *builtinArithmeticMultiplyDecimalSig, *builtinArithmeticMultiplyIntSig, *builtinArithmeticMultiplyIntUnsignedSig, *builtinArithmeticMultiplyRealSig:
		return &ast.BinaryOperationExpr{Op: opcode.Mul, L: children[0], R: children[1]}, nil
	case *builtinArithmeticDivideRealSig, *builtinArithmeticDivideDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.Div, L: children[0], R: children[1]}, nil
	case *builtinArithmeticModIntSig:
		return &ast.BinaryOperationExpr{Op: opcode.Mod, L: children[0], R: children[1]}, nil
	case *builtinArithmeticIntDivideIntSig:
		// for postgres, intdiv is not a operator but a function call
		if _, ok := dialect.(*format.PostgresDialect); ok {
			return &ast.FuncCallExpr{FnName: model.NewCIStr(expr.FuncName.L), Args: children}, nil
		}
		return &ast.BinaryOperationExpr{Op: opcode.IntDiv, L: children[0], R: children[1]}, nil
	case *builtinLTIntSig, *builtinLTStringSig, *builtinLTRealSig, *builtinLTDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.LT, L: children[0], R: children[1]}, nil
	case *builtinEQIntSig, *builtinEQStringSig, *builtinEQRealSig, *builtinEQDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.EQ, L: children[0], R: children[1]}, nil
	case *builtinLEIntSig, *builtinLEStringSig, *builtinLERealSig, *builtinLEDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.LE, L: children[0], R: children[1]}, nil
	case *builtinGTIntSig, *builtinGTStringSig, *builtinGTRealSig, *builtinGTDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.GT, L: children[0], R: children[1]}, nil
	case *builtinGEIntSig, *builtinGEStringSig, *builtinGERealSig, *builtinGEDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.GE, L: children[0], R: children[1]}, nil
	case *builtinNEIntSig, *builtinNEStringSig, *builtinNERealSig, *builtinNEDecimalSig:
		return &ast.BinaryOperationExpr{Op: opcode.NE, L: children[0], R: children[1]}, nil
	case *builtinLogicAndSig:
		return &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: children[0], R: children[1]}, nil
	case *builtinLogicOrSig:
		return &ast.BinaryOperationExpr{Op: opcode.LogicOr, L: children[0], R: children[1]}, nil
	case *builtinLogicXorSig:
		return &ast.BinaryOperationExpr{Op: opcode.LogicXor, L: children[0], R: children[1]}, nil
	case *builtinLikeSig:
		return &ast.PatternLikeExpr{Expr: children[0], Pattern: children[1], Escape: '\\'}, nil
	// case *builtinRegexpSig, *builtinRegexpUTF8Sig:
	// 	return  &ast.ColumnNameExpr{Name: &ast.ColumnName{Table: children[0].}}, nil
	case *builtinDecimalIsNullSig, *builtinIntIsNullSig, *builtinRealIsNullSig, *builtinStringIsNullSig:
		return &ast.IsNullExpr{Expr: children[0], Not: false}, nil
	case *builtinInIntSig, *builtinInStringSig:
		return &ast.PatternInExpr{Expr: children[0], List: children[1:]}, nil
	case *builtinSubstring2ArgsSig, *builtinSubstring2ArgsUTF8Sig, *builtinSubstring3ArgsSig, *builtinSubstring3ArgsUTF8Sig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinTrimSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinUnaryNotRealSig, *builtinUnaryNotDecimalSig, *builtinUnaryNotIntSig:
		// not always need ()
		return &ast.UnaryOperationExpr{Op: opcode.Not, V: &ast.ParenthesesExpr{Expr: children[0]}}, nil
	case *builtinCaseWhenIntSig, *builtinCaseWhenDecimalSig, *builtinCaseWhenRealSig, *builtinCaseWhenStringSig:
		var whenClauses []*ast.WhenClause
		for i := 0; i < len(children)-1; i = i + 2 {
			whenClauses = append(whenClauses, &ast.WhenClause{Expr: children[i], Result: children[i+1]})
		}
		result := &ast.CaseExpr{WhenClauses: whenClauses}
		if len(children)%2 == 1 {
			result.ElseClause = children[len(children)-1]
		}
		return result, nil
	case *builtinConcatSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinTruncateDecimalSig, *builtinTruncateIntSig, *builtinTruncateRealSig, *builtinTruncateUintSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinNowWithoutArgSig, *builtinCurrentDateSig, *builtinCurrentTime0ArgSig, *builtinLastDaySig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinAddDateDatetimeIntSig, *builtinSubDateDatetimeIntSig, *builtinAddDatetimeAndStringSig, *builtinSubDatetimeAndStringSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinDateDiffSig, *builtinTimeTimeTimeDiffSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinStrToDateDateSig, *builtinStrToDateDatetimeSig, *builtinStrToDateDurationSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinDateFormatSig:
		return &ast.FuncCallExpr{FnName: expr.FuncName, Args: children}, nil
	case *builtinGeoDist:
		return c.buildGeoDistExpr(children, dialect, prec, idToExpr)
	}
	return nil, errors.Errorf("Unknown expr: %+v", expr.Function)
}

func radians(node ast.ExprNode) *ast.FuncCallExpr {
	return &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Radians), Args: []ast.ExprNode{node}}
}

func (c ExprConverter) buildGeoDistExpr(children []ast.ExprNode, dialect format.Dialect, prec int, idToExpr map[int64]ast.ExprNode) (ast.ExprNode, error) {
	lo1 := children[0] //longtitude1
	la1 := children[1] //latitude1
	lo2 := children[2] //longtitude2
	la2 := children[3] //latitude2

	lo1Radians := radians(lo1)
	la1Radians := radians(la1)
	lo2Radians := radians(lo2)
	la2Radians := radians(la2)

	sinLa1 := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Sin), Args: []ast.ExprNode{la1Radians}}
	sinLa2 := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Sin), Args: []ast.ExprNode{la2Radians}}
	cosLa1 := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Cos), Args: []ast.ExprNode{la1Radians}}
	cosLa2 := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Cos), Args: []ast.ExprNode{la2Radians}}

	// lo1 - lo2
	lo1MinusLo2 := &ast.BinaryOperationExpr{Op: opcode.Minus, L: lo1Radians, R: lo2Radians}
	// cos(lo1 - lo2)
	cosLo1MinusLo2 := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Cos), Args: []ast.ExprNode{lo1MinusLo2}}

	// sin(la1) * sin(la2)
	sinLa1MulSinLa2 := &ast.BinaryOperationExpr{Op: opcode.Mul, L: sinLa1, R: sinLa2}

	// cos(la1) * cos(la2)
	cosLa1MulCosLa2 := &ast.BinaryOperationExpr{Op: opcode.Mul, L: cosLa1, R: cosLa2}
	// cos(la1) * cos(la2) * cos(lo1 - lo2)
	cosMulCosMulCos := &ast.BinaryOperationExpr{Op: opcode.Mul, L: cosLa1MulCosLa2, R: cosLo1MinusLo2}
	// sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(lo1 - lo2)
	sinPlusCos := &ast.BinaryOperationExpr{Op: opcode.Plus, L: sinLa1MulSinLa2, R: cosMulCosMulCos}

	// arccos(sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(lo1 - lo2))
	arcCos := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Acos), Args: []ast.ExprNode{sinPlusCos}}

	if len(children) == 4 {
		radius := &Constant{
			Value:   types.NewDatum(6371),
			RetType: types.NewFieldType(mysql.TypeInt24),
		}
		radiusNode, err := c.ConvertExpressionToExprNode(dialect, radius, prec, idToExpr)
		if err != nil {
			return nil, err
		}
		return &ast.BinaryOperationExpr{Op: opcode.Mul, L: radiusNode, R: arcCos}, nil
	} else {
		radiusNode := children[4]
		return &ast.BinaryOperationExpr{Op: opcode.Mul, L: radiusNode, R: arcCos}, nil
	}
}

func IsCompareOp(expr *ScalarFunction) bool {
	switch expr.Function.(type) {
	case *builtinLEIntSig, *builtinLEStringSig, *builtinLERealSig, *builtinLEDecimalSig,
		*builtinGTIntSig, *builtinGTStringSig, *builtinGTRealSig, *builtinGTDecimalSig,
		*builtinGEIntSig, *builtinGEStringSig, *builtinGERealSig, *builtinGEDecimalSig,
		*builtinLTIntSig, *builtinLTStringSig, *builtinLTRealSig, *builtinLTDecimalSig,
		*builtinEQIntSig, *builtinEQStringSig, *builtinEQRealSig, *builtinEQDecimalSig,
		*builtinNEIntSig, *builtinNEStringSig, *builtinNERealSig, *builtinNEDecimalSig:
		return true
	}
	return false

}
