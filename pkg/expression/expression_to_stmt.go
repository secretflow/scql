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
	"github.com/secretflow/scql/pkg/parser/opcode"
	driver "github.com/secretflow/scql/pkg/types/parser_driver"
)

// ExprConverter converts expression to expr node
type ExprConverter struct {
}

func (c ExprConverter) ConvertExpressionToExprNode(dialect format.Dialect, expr Expression, idToExpr map[int64]ast.ExprNode) (ast.ExprNode, error) {
	switch x := expr.(type) {
	case *Constant:
		return c.convertConst(x)
	case *Column:
		if expr, ok := idToExpr[x.UniqueID]; ok {
			return expr, nil
		}
		return c.convertColumn(x)
	case *ScalarFunction:
		return c.convertScalarFunction(dialect, x, idToExpr)
	default:
		return nil, errors.Errorf("Unknown expression type: %v", expr)
	}
}

func (c ExprConverter) isArgScalarFunc(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		return c.needArgParenthesesExpr(x)
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

	name.Name = model.CIStr{O: subStrs[len(subStrs)-1]}
	if len(subStrs) >= 2 {
		name.Table = model.CIStr{O: subStrs[len(subStrs)-2]}
	}
	if len(subStrs) == 3 {
		name.Schema = model.CIStr{O: subStrs[len(subStrs)-3]}
	}
	return &ast.ColumnNameExpr{Name: &name}, nil
}

// for now binary function need Parentheses
func (c ExprConverter) needArgParenthesesExpr(expr *ScalarFunction) bool {
	switch expr.Function.(type) {
	case *builtinUnaryMinusDecimalSig, *builtinUnaryMinusIntSig, *builtinUnaryMinusRealSig, *builtinArithmeticPlusRealSig,
		*builtinArithmeticPlusDecimalSig, *builtinArithmeticPlusIntSig, *builtinArithmeticMinusRealSig, *builtinArithmeticMinusDecimalSig,
		*builtinArithmeticMinusIntSig, *builtinArithmeticMultiplyDecimalSig, *builtinArithmeticMultiplyIntSig,
		*builtinArithmeticMultiplyIntUnsignedSig, *builtinArithmeticMultiplyRealSig, *builtinArithmeticDivideRealSig, *builtinArithmeticDivideDecimalSig,
		*builtinArithmeticModIntSig, *builtinArithmeticIntDivideIntSig,
		*builtinLTIntSig, *builtinLTStringSig, *builtinLTRealSig, *builtinLTDecimalSig,
		*builtinEQIntSig, *builtinEQStringSig, *builtinEQRealSig, *builtinEQDecimalSig,
		*builtinLEIntSig, *builtinLEStringSig, *builtinLERealSig, *builtinLEDecimalSig,
		*builtinGTIntSig, *builtinGTStringSig, *builtinGTRealSig, *builtinGTDecimalSig,
		*builtinGEIntSig, *builtinGEStringSig, *builtinGERealSig, *builtinGEDecimalSig,
		*builtinNEIntSig, *builtinNEStringSig, *builtinNERealSig, *builtinNEDecimalSig,
		*builtinLogicAndSig, *builtinLogicOrSig, *builtinLogicXorSig:
		return true
	}
	return false
}

func (c ExprConverter) convertScalarFunction(dialect format.Dialect, expr *ScalarFunction, idToExpr map[int64]ast.ExprNode) (ast.ExprNode, error) {
	children := make([]ast.ExprNode, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		argExpr, err := c.ConvertExpressionToExprNode(dialect, arg, idToExpr)
		if err != nil {
			return nil, fmt.Errorf("convertScalarFunction: %v", err)
		}
		children = append(children, argExpr)
	}
	if c.needArgParenthesesExpr(expr) {
		for i, arg := range expr.GetArgs() {
			if c.isArgScalarFunc(arg) {
				children[i] = &ast.ParenthesesExpr{Expr: children[i]}
			}
		}
	}
	switch expr.FuncName.L {
	case ast.Cast:
		return &ast.FuncCastExpr{Expr: children[0], Tp: expr.RetType, FunctionType: ast.CastFunction}, nil
	case ast.Ifnull:
		return &ast.FuncCallExpr{FnName: model.NewCIStr(dialect.GetSpecialFuncName(ast.Ifnull)), Args: children}, nil
	case ast.If:
		return &ast.FuncCallExpr{FnName: model.NewCIStr(ast.If), Args: children}, nil
	case ast.Cos, ast.Abs,ast.Log10, ast.Round:
		return &ast.FuncCallExpr{FnName: model.NewCIStr(expr.FuncName.L), Args: children}, nil
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
	// 	return &ast.ColumnNameExpr{Name: &ast.ColumnName{Table: children[0].}}, nil
	case *builtinDecimalIsNullSig, *builtinIntIsNullSig, *builtinRealIsNullSig, *builtinStringIsNullSig:
		return &ast.IsNullExpr{Expr: children[0], Not: false}, nil
	case *builtinInIntSig, *builtinInStringSig:
		return &ast.PatternInExpr{Expr: children[0], List: children[1:]}, nil
	case *builtinSubstring2ArgsSig, *builtinSubstring2ArgsUTF8Sig, *builtinSubstring3ArgsSig, *builtinSubstring3ArgsUTF8Sig:
		return &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Substring), Args: children}, nil
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
		return &ast.FuncCallExpr{FnName: model.NewCIStr(ast.Concat), Args: children}, nil
	}
	return nil, errors.Errorf("Unknown expr: %+v", expr.Function)
}
