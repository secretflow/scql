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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
)

// TestLogicalPlanTimestampVisitor tests the logical plan timestamp visitor
func TestLogicalPlanTimestampVisitor(t *testing.T) {
	createdAt := time.Date(2023, 10, 1, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*3600))
	visitor := newLogicalPlanTimestampVisitor(createdAt)

	// Test case 1: isTimestampWithoutTimezone should correctly identify timestamp strings
	t.Run("IsTimestampWithoutTimezone", func(t *testing.T) {
		// Should match timestamps without timezone
		assert.True(t, visitor.isTimestampWithoutTimezone("2023-10-01 08:30:00"))
		assert.True(t, visitor.isTimestampWithoutTimezone("2023-10-01T08:30:00"))

		// Should not match timestamps with timezone
		assert.False(t, visitor.isTimestampWithoutTimezone("2023-10-01 08:30:00+08:00"))
		assert.False(t, visitor.isTimestampWithoutTimezone("2023-10-01T08:30:00Z"))

		// Should not match other strings
		assert.False(t, visitor.isTimestampWithoutTimezone("not a timestamp"))
		assert.False(t, visitor.isTimestampWithoutTimezone("2023-10-01"))
	})

	// Test case 2: processConstant should add timezone when in timestamp comparison context
	t.Run("ProcessConstantInTimestampComparison", func(t *testing.T) {
		// Create a constant with timestamp value
		timestampConst := &expression.Constant{
			Value: types.NewStringDatum("2023-10-01 08:30:00"),
		}

		// Process with timestamp comparison context
		ctx := logicalPlanContextInfo{inTimestampComparison: true}
		visitor.processConstant(timestampConst, ctx)

		// Check that the timezone was appended
		expected := "2023-10-01 08:30:00+08:00"
		assert.Equal(t, expected, timestampConst.Value.GetString())
	})

	// Test case 3: processConstant should not add timezone when not in timestamp comparison context
	t.Run("ProcessConstantNotInTimestampComparison", func(t *testing.T) {
		// Create a constant with timestamp value
		timestampConst := &expression.Constant{
			Value: types.NewStringDatum("2023-10-01 08:30:00"),
		}

		// Process with non-timestamp comparison context
		ctx := logicalPlanContextInfo{inTimestampComparison: false}
		visitor.processConstant(timestampConst, ctx)

		// Check that the timezone was NOT appended
		expected := "2023-10-01 08:30:00"
		assert.Equal(t, expected, timestampConst.Value.GetString())
	})

	// Test case 4: processConstant should not modify non-string constants
	t.Run("ProcessNonStringConstant", func(t *testing.T) {
		// Create a constant with integer value
		intConst := &expression.Constant{
			Value: types.NewIntDatum(123),
		}

		// Process with timestamp comparison context
		ctx := logicalPlanContextInfo{inTimestampComparison: true}
		visitor.processConstant(intConst, ctx)

		// Check that the value was not modified
		expected := int64(123)
		assert.Equal(t, expected, intConst.Value.GetInt64())
	})

	// Test case 5: isTimestampColumn should correctly identify timestamp columns
	t.Run("IsTimestampColumn", func(t *testing.T) {
		// Create a column with timestamp type
		timestampCol := &expression.Column{
			RetType: types.NewFieldType(mysql.TypeTimestamp),
		}

		// Create a column with varchar type
		varcharCol := &expression.Column{
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}

		// Check that timestamp column is identified correctly
		assert.True(t, visitor.isTimestampColumn(timestampCol))

		// Check that non-timestamp column is identified correctly
		assert.False(t, visitor.isTimestampColumn(varcharCol))
	})
}

// TestTimestampLiteralOptimizer tests the complete timestamp literal optimization functionality
func TestTimestampLiteralOptimizer(t *testing.T) {
	// Create a session context with createdAt time
	sctx := sessionctx.NewContext()
	createdAt := time.Date(2023, 10, 1, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*3600))
	sctx.GetSessionVars().CreatedAt = createdAt
	sctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0

	// Create a simple infoschema for testing
	tableInfos := map[string][]*model.TableInfo{
		"test": {
			{
				Name: model.NewCIStr("orders"),
				Columns: []*model.ColumnInfo{
					{
						Name:      model.NewCIStr("created_at"),
						FieldType: *types.NewFieldType(mysql.TypeTimestamp),
					},
					{
						Name:      model.NewCIStr("status"),
						FieldType: *types.NewFieldType(mysql.TypeVarchar),
					},
				},
			},
		},
	}
	is := infoschema.MockInfoSchema(tableInfos)

	// Test case 1: Simple equality comparison with timestamp column
	t.Run("SimpleEqualityComparison", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT * FROM test.orders WHERE created_at = '2023-10-01 08:30:00'", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 2: IN clause with timestamp column
	t.Run("InClauseComparison", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT * FROM test.orders WHERE created_at IN ('2023-10-01 08:30:00', '2023-10-02 09:00:00')", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 2)
		if len(constants1) >= 2 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
			assert.Equal(t, "2023-10-02 09:00:00", constants1[1].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 2)
		if len(constants2) >= 2 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
			assert.Equal(t, "2023-10-02 09:00:00+08:00", constants2[1].Value.GetString())
		}
	})

	// Test case 3: Timestamp with existing timezone should not be modified
	t.Run("TimestampWithExistingTimezone", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT * FROM test.orders WHERE created_at = '2023-10-01 08:30:00+08:00'", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 4: Non-timestamp column should not be modified
	t.Run("NonTimestampColumn", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT * FROM test.orders WHERE status = '2023-10-01 08:30:00'", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants2[0].Value.GetString())
		}
	})

	// Test case 5: Timestamp comparison in projection
	t.Run("TimestampComparisonInProjection", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT created_at > '2023-10-01 08:30:00' FROM test.orders", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 6: Timestamp literals in aggregation expressions
	t.Run("TimestampLiteralsInAggregation", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT SUM(IF(created_at > '2023-10-01 08:30:00', 1, 0.5)) FROM test.orders", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 7: Timestamp literals in GROUP BY with time comparisons
	t.Run("TimestampLiteralsInGroupByWithComparisons", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT COUNT(*) FROM test.orders GROUP BY created_at > '2023-10-01 08:30:00'", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 8: Timestamp literals in window functions
	t.Run("TimestampLiteralsInWindowFunctions", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT created_at, ROW_NUMBER() OVER (PARTITION BY created_at > '2023-10-01 08:30:00' ORDER BY created_at) as rn FROM test.orders", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 9: Timestamp literals in ORDER BY clauses
	t.Run("TimestampLiteralsInOrderBy", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT created_at FROM test.orders ORDER BY (created_at > '2023-10-01 08:30:00') DESC", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})

	// Test case 10: Timestamp literals in subquery with APPLY
	t.Run("TimestampLiteralsInApply", func(t *testing.T) {
		p := parser.New()
		stmts, _, err := p.Parse("SELECT (SELECT COUNT(*) FROM test.orders o2 WHERE o2.created_at > '2023-10-01 08:30:00' AND o2.status = o1.status) FROM test.orders o1", "", "")
		assert.NoError(t, err)
		assert.Len(t, stmts, 1)

		// Build logical plan without optimization
		plan1, _, err := BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan1)

		lp1, ok := plan1.(LogicalPlan)
		assert.True(t, ok)

		// Find constants in the non-optimized plan
		constants1 := findConstantsInPlan(lp1)
		assert.GreaterOrEqual(t, len(constants1), 1)
		if len(constants1) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00", constants1[0].Value.GetString())
		}

		// Build logical plan with optimization
		plan2, _, err := BuildLogicalPlanWithOptimization(context.Background(), sctx, stmts[0], is)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		// Find constants in the optimized plan
		constants2 := findConstantsInPlan(plan2)
		assert.GreaterOrEqual(t, len(constants2), 1)
		if len(constants2) > 0 {
			assert.Equal(t, "2023-10-01 08:30:00+08:00", constants2[0].Value.GetString())
		}
	})
}

// findConstantsInPlan recursively traverses a logical plan and collects all constant expressions
func findConstantsInPlan(lp LogicalPlan) []*expression.Constant {
	var constants []*expression.Constant

	switch node := lp.(type) {
	case *LogicalSelection:
		for _, expr := range node.Conditions {
			constants = append(constants, findConstantsInExpression(expr)...)
		}

	case *LogicalProjection:
		for _, expr := range node.Exprs {
			constants = append(constants, findConstantsInExpression(expr)...)
		}

	case *LogicalAggregation:
		// Process aggregation function arguments and group by items
		for _, aggFunc := range node.AggFuncs {
			for _, arg := range aggFunc.Args {
				constants = append(constants, findConstantsInExpression(arg)...)
			}
		}
		for _, item := range node.GroupByItems {
			constants = append(constants, findConstantsInExpression(item)...)
		}

	case *LogicalWindow:
		// Process window function arguments, partition by, order by, and frame bounds
		for _, windowFunc := range node.WindowFuncDescs {
			for _, arg := range windowFunc.Args {
				constants = append(constants, findConstantsInExpression(arg)...)
			}
		}
		for _, item := range node.PartitionBy {
			constants = append(constants, findConstantsInExpression(item.Col)...)
		}
		for _, item := range node.OrderBy {
			constants = append(constants, findConstantsInExpression(item.Col)...)
		}
		// Process frame bound expressions if they exist
		if node.Frame != nil {
			if node.Frame.Start != nil {
				for _, calcFunc := range node.Frame.Start.CalcFuncs {
					constants = append(constants, findConstantsInExpression(calcFunc)...)
				}
			}
			if node.Frame.End != nil {
				for _, calcFunc := range node.Frame.End.CalcFuncs {
					constants = append(constants, findConstantsInExpression(calcFunc)...)
				}
			}
		}

	case *LogicalSort:
		// Process sort expressions
		for _, item := range node.ByItems {
			constants = append(constants, findConstantsInExpression(item.Expr)...)
		}

	case *LogicalApply:
		// Process correlated columns
		for _, col := range node.CorCols {
			constants = append(constants, findConstantsInExpression(&col.Column)...)
		}
		// Process all join conditions
		for _, eqCond := range node.EqualConditions {
			constants = append(constants, findConstantsInExpression(eqCond)...)
		}
		for _, leftCond := range node.LeftConditions {
			constants = append(constants, findConstantsInExpression(leftCond)...)
		}
		for _, rightCond := range node.RightConditions {
			constants = append(constants, findConstantsInExpression(rightCond)...)
		}
		for _, otherCond := range node.OtherConditions {
			constants = append(constants, findConstantsInExpression(otherCond)...)
		}
	}

	// Recursively process children
	for _, child := range lp.Children() {
		constants = append(constants, findConstantsInPlan(child)...)
	}

	return constants
}

// findConstantsInExpression recursively traverses an expression and collects all constant expressions
func findConstantsInExpression(expr expression.Expression) []*expression.Constant {
	var constants []*expression.Constant

	if constant, ok := expr.(*expression.Constant); ok {
		// Only include constants that match timestamp formats
		if constant.Value.Kind() == types.KindString {
			strValue := constant.Value.GetString()
			// Check if it matches timestamp pattern like "2023-10-01 08:30:00" or "2023-10-01T08:30:00"
			timestampPattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`)
			if timestampPattern.MatchString(strValue) {
				constants = append(constants, constant)
			}
		}
	}

	if scalarFunc, ok := expr.(*expression.ScalarFunction); ok {
		for _, arg := range scalarFunc.GetArgs() {
			constants = append(constants, findConstantsInExpression(arg)...)
		}
	}

	return constants
}
