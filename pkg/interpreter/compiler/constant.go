// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
)

const (
	RevealKeyAfterJoin = "reveal_key_after_join"
	RevealFilterMask   = "reveal_filter_mask"
)

var isCompareAstFuncMap = map[string]struct{}{
	ast.GT: {},
	ast.GE: {},
	ast.LT: {},
	ast.LE: {},
	ast.EQ: {},
	ast.NE: {},
}

var isLogicalAstFuncMap = map[string]struct{}{
	ast.LogicOr:  {},
	ast.LogicAnd: {},
	ast.UnaryNot: {},
}

var SupportedJoinType = []core.JoinType{
	core.InnerJoin,
	core.LeftOuterJoin,
	core.RightOuterJoin,
}

var constTensorNeedCastFuncs = map[string]struct{}{
	ast.GT:    {},
	ast.GE:    {},
	ast.LT:    {},
	ast.LE:    {},
	ast.EQ:    {},
	ast.NE:    {},
	ast.Plus:  {},
	ast.Minus: {},
}

var functionArgNum = map[string]int{
	/// unary
	// the translation of Cast/Cot/Geodist does not need this map
	// ast.Cast:      1,
	ast.UnaryNot: 1, // support variadic inputs in SCQL engine, but limited to be unary in SCQL interpreter
	ast.Cos:      1,
	ast.Sin:      1,
	ast.Acos:     1,
	ast.Asin:     1,
	ast.Tan:      1,
	//ast.Cot:       1,
	ast.Atan:      1,
	ast.Abs:       1,
	ast.Ceil:      1,
	ast.Floor:     1,
	ast.Round:     1,
	ast.Radians:   1,
	ast.Degrees:   1,
	ast.Ln:        1,
	ast.Log10:     1,
	ast.Log2:      1,
	ast.Sqrt:      1,
	ast.Exp:       1,
	ast.IsNull:    1,
	ast.Lower:     1,
	ast.Upper:     1,
	ast.Trim:      1,
	ast.Substr:    1, // const params do not count as arg here
	ast.Substring: 1,
	ast.StrToDate: 1,

	/// binary
	ast.LT:       2,
	ast.GT:       2,
	ast.LE:       2,
	ast.GE:       2,
	ast.EQ:       2,
	ast.NE:       2,
	ast.LogicOr:  2,
	ast.LogicAnd: 2,
	ast.Plus:     2,
	ast.Minus:    2,
	ast.Mul:      2,
	ast.Div:      2,
	ast.IntDiv:   2,
	ast.Mod:      2,
	ast.AddDate:  2,
	ast.SubDate:  2,
	ast.DateDiff: 2,
	ast.Pow:      2,
	ast.Ifnull:   2,
	ast.Atan2:    2,

	/// tenary
	ast.If: 3,

	/// Variadic
	ast.Greatest: -1,
	ast.Least:    -1,
	ast.Case:     -1,
	ast.Coalesce: -1,
	ast.GeoDist:  -1,
	ast.Concat:   -1,
}

var astName2NodeName = map[string]string{
	ast.If:         operator.OpNameIf,
	ast.Greatest:   operator.OpNameGreatest,
	ast.Least:      operator.OpNameLeast,
	ast.LT:         operator.OpNameLess,
	ast.LE:         operator.OpNameLessEqual,
	ast.GT:         operator.OpNameGreater,
	ast.GE:         operator.OpNameGreaterEqual,
	ast.EQ:         operator.OpNameEqual,
	ast.NE:         operator.OpNameNotEqual,
	ast.LogicOr:    operator.OpNameLogicalOr,
	ast.LogicAnd:   operator.OpNameLogicalAnd,
	ast.Plus:       operator.OpNameAdd,
	ast.UnaryMinus: operator.OpNameMinus,
	ast.Minus:      operator.OpNameMinus,
	ast.Mul:        operator.OpNameMul,
	ast.Div:        operator.OpNameDiv,
	ast.IntDiv:     operator.OpNameIntDiv,
	ast.Mod:        operator.OpNameMod,
	ast.DateDiff:   operator.OpNameMinus,
	ast.AddDate:    operator.OpNameAdd,
	ast.SubDate:    operator.OpNameMinus,
	ast.Sin:        operator.OpNameSin,
	ast.Cos:        operator.OpNameCos,
	ast.Acos:       operator.OpNameACos,
	ast.Asin:       operator.OpNameASin,
	ast.Tan:        operator.OpNameTan,
	ast.Cot:        operator.OpNameCot,
	ast.Atan:       operator.OpNameATan,
	ast.Atan2:      operator.OpNameATan2,
	// tidb parser does not suport acot function
	// ast.Acot:       operator.OpNameACot,
	ast.Abs:      operator.OpNameAbs,
	ast.Ceil:     operator.OpNameCeil,
	ast.Floor:    operator.OpNameFloor,
	ast.Round:    operator.OpNameRound,
	ast.Degrees:  operator.OpNameDegrees,
	ast.Radians:  operator.OpNameRadians,
	ast.Ln:       operator.OpNameLn,
	ast.Log2:     operator.OpNameLog2,
	ast.Log10:    operator.OpNameLog10,
	ast.Sqrt:     operator.OpNameSqrt,
	ast.Exp:      operator.OpNameExp,
	ast.Pow:      operator.OpNamePow,
	ast.IsNull:   operator.OpNameIsNull,
	ast.Cast:     operator.OpNameCast,
	ast.UnaryNot: operator.OpNameNot,
}

var JoinTypeLpToEp = map[core.JoinType]int{
	core.InnerJoin:      graph.InnerJoin,
	core.LeftOuterJoin:  graph.LeftOuterJoin,
	core.RightOuterJoin: graph.RightOuterJoin,
}
