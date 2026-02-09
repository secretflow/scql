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

package core

import (
	"context"
	"regexp"
	"time"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"
)

// timeZonePatcher is an optimization rule that adds timezone information
// to timestamp literals when they are being compared to timestamp columns
type timeZonePatcher struct{}

func (t *timeZonePatcher) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	// Get the session context from the logical plan
	sctx := lp.SCtx()
	if sctx == nil {
		return lp, nil
	}

	// Get the createdAt time from the session context
	createdAt := sctx.GetSessionVars().CreatedAt
	if createdAt.IsZero() {
		return lp, nil
	}

	// Create and apply the logical plan timestamp visitor
	visitor := newLogicalPlanTimestampVisitor(createdAt)
	visitor.visit(lp)

	return lp, nil
}

func (t *timeZonePatcher) name() string {
	return "time_zone_patch"
}

// logicalPlanTimestampVisitor is a visitor that adds timezone information to timestamp literals
// in the logical plan directly, using the type information available in the logical plan
type logicalPlanTimestampVisitor struct {
	createdAt time.Time
}

// newLogicalPlanTimestampVisitor creates a new logicalPlanTimestampVisitor
func newLogicalPlanTimestampVisitor(createdAt time.Time) *logicalPlanTimestampVisitor {
	return &logicalPlanTimestampVisitor{
		createdAt: createdAt,
	}
}

// visit implements a recursive traversal of the logical plan to modify timestamp literals
func (lptv *logicalPlanTimestampVisitor) visit(lp LogicalPlan) {
	for _, child := range lp.Children() {
		lptv.visit(child)
	}

	lptv.processNode(lp)
}

// processNode processes a single logical plan node to modify timestamp literals
func (lptv *logicalPlanTimestampVisitor) processNode(lp LogicalPlan) {
	switch node := lp.(type) {
	case *LogicalSelection:
		for _, expr := range node.Conditions {
			lptv.processExpression(expr, logicalPlanContextInfo{})
		}

	case *LogicalProjection:
		for _, expr := range node.Exprs {
			lptv.processExpression(expr, logicalPlanContextInfo{})
		}

	case *LogicalAggregation:
		// Process aggregation function arguments and group by items
		for _, aggFunc := range node.AggFuncs {
			for _, arg := range aggFunc.Args {
				lptv.processExpression(arg, logicalPlanContextInfo{})
			}
		}
		for _, item := range node.GroupByItems {
			lptv.processExpression(item, logicalPlanContextInfo{})
		}
	case *LogicalWindow:
		// Process window function arguments, partition by, order by, and frame bounds
		for _, windowFunc := range node.WindowFuncDescs {
			for _, arg := range windowFunc.Args {
				lptv.processExpression(arg, logicalPlanContextInfo{})
			}
		}
		for _, item := range node.PartitionBy {
			lptv.processExpression(item.Col, logicalPlanContextInfo{})
		}
		for _, item := range node.OrderBy {
			lptv.processExpression(item.Col, logicalPlanContextInfo{})
		}
		// Process frame bounds if present
		if node.Frame != nil {
			if node.Frame.Start != nil {
				for _, calcFunc := range node.Frame.Start.CalcFuncs {
					lptv.processExpression(calcFunc, logicalPlanContextInfo{})
				}
			}
			if node.Frame.End != nil {
				for _, calcFunc := range node.Frame.End.CalcFuncs {
					lptv.processExpression(calcFunc, logicalPlanContextInfo{})
				}
			}
		}

	case *LogicalSort:
		// Process sort expressions
		for _, item := range node.ByItems {
			lptv.processExpression(item.Expr, logicalPlanContextInfo{})
		}

	case *LogicalApply:
		// Process correlated columns
		for _, col := range node.CorCols {
			lptv.processExpression(&col.Column, logicalPlanContextInfo{})
		}

		if join, ok := lp.(*LogicalJoin); ok {
			for _, eqCond := range join.EqualConditions {
				lptv.processExpression(eqCond, logicalPlanContextInfo{})
			}
			for _, leftCond := range join.LeftConditions {
				lptv.processExpression(leftCond, logicalPlanContextInfo{})
			}
			for _, rightCond := range join.RightConditions {
				lptv.processExpression(rightCond, logicalPlanContextInfo{})
			}
			for _, otherCond := range join.OtherConditions {
				lptv.processExpression(otherCond, logicalPlanContextInfo{})
			}
		}

		// In SCQL, join operations typically only involve column-to-column equality comparisons
		// case *LogicalJoin:
		// 	// Process all join conditions
		// 	for _, eqCond := range node.EqualConditions {
		// 		lptv.processExpression(eqCond, logicalPlanContextInfo{})
		// 	}
		// 	for _, leftCond := range node.LeftConditions {
		// 		lptv.processExpression(leftCond, logicalPlanContextInfo{})
		// 	}
		// 	for _, rightCond := range node.RightConditions {
		// 		lptv.processExpression(rightCond, logicalPlanContextInfo{})
		// 	}
		// 	for _, otherCond := range node.OtherConditions {
		// 		lptv.processExpression(otherCond, logicalPlanContextInfo{})
		// 	}
	}
}

// processExpression processes a single expression to modify timestamp literals
func (lptv *logicalPlanTimestampVisitor) processExpression(expr expression.Expression, ctx logicalPlanContextInfo) {
	// If this is a scalar function, we need to check if it's a timestamp comparison
	// and process its arguments with the appropriate context
	if scalarFunc, ok := expr.(*expression.ScalarFunction); ok {
		isTimestampComparison := lptv.isTimestampComparison(expr)

		newCtx := logicalPlanContextInfo{
			inTimestampComparison: isTimestampComparison,
		}

		lptv.processExpressions(scalarFunc.GetArgs(), newCtx)
		return
	}

	// If this is a constant value expression, check if it's a timestamp literal we need to modify
	if constant, ok := expr.(*expression.Constant); ok {
		lptv.processConstant(constant, ctx)
		return
	}
}

// processExpressions processes a slice of expressions to modify timestamp literals
func (lptv *logicalPlanTimestampVisitor) processExpressions(exprs []expression.Expression, ctx logicalPlanContextInfo) {
	for _, expr := range exprs {
		lptv.processExpression(expr, ctx)
	}
}

// logicalPlanContextInfo tracks information about the current context during logical plan traversal
type logicalPlanContextInfo struct {
	// Whether we're in a comparison operation with a timestamp column
	inTimestampComparison bool
}

// isTimestampComparison checks if an expression is a comparison with a timestamp column
func (lptv *logicalPlanTimestampVisitor) isTimestampComparison(expr expression.Expression) bool {
	if scalarFunc, ok := expr.(*expression.ScalarFunction); ok {
		switch scalarFunc.FuncName.L {
		case ast.EQ, ast.NE, ast.LT, ast.LE, ast.GT, ast.GE:
			for _, arg := range scalarFunc.GetArgs() {
				if lptv.isTimestampColumn(arg) {
					return true
				}
			}
		case ast.In:
			args := scalarFunc.GetArgs()
			if len(args) > 0 && lptv.isTimestampColumn(args[0]) {
				return true
			}
		}
	}
	return false
}

// isTimestampColumn checks if an expression represents a timestamp column
func (lptv *logicalPlanTimestampVisitor) isTimestampColumn(expr expression.Expression) bool {
	if col, ok := expr.(*expression.Column); ok {
		return col.RetType.Tp == mysql.TypeTimestamp
	}
	return false
}

// isTimestampWithoutTimezone checks if a string is a timestamp format without timezone
func (lptv *logicalPlanTimestampVisitor) isTimestampWithoutTimezone(s string) bool {
	// Check if it matches timestamp pattern like "2023-10-01 08:30:00" or "2023-10-01T08:30:00"
	timestampPattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$`)

	// Check if it already has timezone information
	hasTimezonePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2}|Z)$`)

	return timestampPattern.MatchString(s) && !hasTimezonePattern.MatchString(s)
}

// processConstant processes a constant value to modify timestamp literals
func (lptv *logicalPlanTimestampVisitor) processConstant(constant *expression.Constant, ctx logicalPlanContextInfo) {
	if constant.Value.Kind() == types.KindString {
		strValue := constant.Value.GetString()

		if ctx.inTimestampComparison {
			if lptv.isTimestampWithoutTimezone(strValue) {
				// Add timezone from createdAt
				timezoneStr := lptv.createdAt.Format("-07:00")
				if timezoneStr[0] != '-' && timezoneStr[0] != '+' {
					timezoneStr = "+" + timezoneStr
				}
				newValue := strValue + timezoneStr

				// Update the constant with the new value
				constant.Value.SetString(newValue)
			}
		}
	}
}
