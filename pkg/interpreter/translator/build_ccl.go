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

package translator

import (
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

const (
	GroupByThreshold = 4
)

func (n *baseNode) buildChildCCL(columnTracer *ccl.ColumnTracer) error {
	n.dataSourceParties = []string{}
	for _, c := range n.Children() {
		if err := c.buildCCL(columnTracer); err != nil {
			return err
		}
		n.dataSourceParties = append(n.dataSourceParties, c.DataSourceParty()...)
	}
	n.dataSourceParties = sliceutil.SliceDeDup(n.dataSourceParties)
	return nil
}

func (n *DataSourceNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	ds, ok := n.lp.(*core.DataSource)
	if !ok {
		return fmt.Errorf("assert failed while dataSourceNode buildCCL, expected: core.DataSource, actual: %T", n.lp)
	}
	// 1. fill ccl
	resultCCL := make(map[int64]*ccl.CCL, 0)
	for i := range ds.Columns {
		sc := ds.Schema().Columns[i]
		originCCL, ok := n.originCCL[sc.OrigName]
		if !ok {
			return fmt.Errorf("%s doesn't set ccl", sc.OrigName)
		}
		resultCCL[sc.UniqueID] = originCCL.Clone()
	}
	n.ccl = resultCCL
	// 2. fill data source
	for _, c := range ds.Schema().Columns {
		colTracer.AddSourceParties(c.UniqueID, n.DataSourceParty())
	}
	return nil
}

func (n *ProjectionNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)
	proj, ok := n.lp.(*core.LogicalProjection)
	if !ok {
		return fmt.Errorf("assert failed while projectionNode buildCCL, expected: core.LogicalProjection, actual: %T", n.lp)
	}
	resultCCL := make(map[int64]*ccl.CCL)
	for i, expr := range proj.Exprs {
		cc, err := ccl.InferExpressionCCL(expr, childCCL)
		if err != nil {
			return fmt.Errorf("projectionNode.buildCCL: %v", err)
		}
		resultCCL[proj.Schema().Columns[i].UniqueID] = cc
	}

	for i, expr := range proj.Exprs {
		colUniqueID := n.Schema().Columns[i].UniqueID
		colTracer.SetSourcePartiesFromExpr(colUniqueID, expr)
	}
	n.ccl = resultCCL
	return nil
}

func (n *JoinNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)

	join, ok := n.lp.(*core.LogicalJoin)
	if !ok {
		return fmt.Errorf("assert failed while joinNode buildCCL, expected: core.LogicalJoin, actual: %T", n.lp)
	}
	joinKeyCCLs := make(map[int64]*ccl.CCL)
	allJoinColumnUniqueIds := make(map[int64]bool)
	// extract data source party of join tables
	for _, col := range join.Schema().Columns {
		allJoinColumnUniqueIds[col.UniqueID] = true
	}
	// fill child data source parties
	for i, child := range join.Children() {
		var childDataSourceParty []string
		for _, column := range child.Schema().Columns {
			if allJoinColumnUniqueIds[column.UniqueID] {
				childDataSourceParty = append(childDataSourceParty, colTracer.FindSourceParties(column.UniqueID)...)
			}
		}
		n.childDataSourceParties[i] = sliceutil.SliceDeDup(childDataSourceParty)
	}
	type joinKeyPair struct {
		leftId  int64
		rightId int64
	}
	var joinKeyPairs []joinKeyPair
	for _, equalCondition := range join.EqualConditions {
		cols, err := extractEQColumns(equalCondition)
		if err != nil {
			return fmt.Errorf("joinNode.buildCCL: %v", err)
		}
		var conditionCCLs []*ccl.CCL
		for _, col := range cols {
			cc, ok := childCCL[col.UniqueID]
			if !ok {
				return fmt.Errorf("joinNode.buildCCL: can not find ccl for col(%d)", col.UniqueID)
			}
			conditionCCLs = append(conditionCCLs, cc.Clone())
		}
		leftCc := conditionCCLs[0]
		rightCc := conditionCCLs[1]
		leftId, rightId := cols[0].UniqueID, cols[1].UniqueID
		for _, p := range colTracer.FindSourceParties(leftId) {
			if rightCc.LevelFor(p) == ccl.Join {
				rightCc.SetLevelForParty(p, ccl.Plain)
			}
		}
		joinKeyPairs = append(joinKeyPairs, joinKeyPair{
			leftId:  leftId,
			rightId: rightId,
		})
		for _, p := range colTracer.FindSourceParties(rightId) {
			if leftCc.LevelFor(p) == ccl.Join {
				leftCc.SetLevelForParty(p, ccl.Plain)
			}
		}
		leftCc.UpdateMoreRestrictedCCLFrom(rightCc)
		// left/right column must have same ccl level
		for _, p := range leftCc.Parties() {
			level := ccl.LookUpVis(leftCc.LevelFor(p), rightCc.LevelFor(p))
			leftCc.SetLevelForParty(p, level)
			rightCc.SetLevelForParty(p, level)
		}
		// check ccl
		for _, p := range colTracer.FindSourceParties(rightId) {
			if !leftCc.IsVisibleFor(p) {
				return fmt.Errorf("joinNode.buildCCL: failed to check condition (%s) left party(%s) ccl(%v)", equalCondition.String(), p, conditionCCLs[0].LevelFor(p).String())
			}
		}
		for _, p := range colTracer.FindSourceParties(leftId) {
			if !rightCc.IsVisibleFor(p) {
				return fmt.Errorf("joinNode.buildCCL: failed to check condition (%s) right party(%s) ccl(%v)", equalCondition.String(), p, conditionCCLs[1].LevelFor(p).String())
			}
		}
		joinKeyCCLs[leftId] = leftCc
		joinKeyCCLs[rightId] = leftCc
	}
	// update source parties
	// select * from ta join (select tb.bob_id, tc.carol_id from tb join tc on tb.index = tc.index) as tt on ta.alice_id = tt.bob_id and ta.alice_id = tt.carol_id
	// alice_id.SourceParties = [alice bob carol]
	// bob_id.SourceParties = [alice bob]
	// carol_id.SourceParties = [alice carol]
	// find source parties
	IdToSourceParties := make(map[int64][]string)
	for _, pair := range joinKeyPairs {
		IdToSourceParties[pair.leftId] = append(IdToSourceParties[pair.leftId], colTracer.FindSourceParties(pair.rightId)...)
		IdToSourceParties[pair.rightId] = append(IdToSourceParties[pair.rightId], colTracer.FindSourceParties(pair.leftId)...)
	}
	for id, parties := range IdToSourceParties {
		colTracer.AddSourceParties(id, parties)
	}
	result := make(map[int64]*ccl.CCL)
	for _, c := range n.Schema().Columns {
		cc, ok := childCCL[c.UniqueID]
		if !ok {
			return fmt.Errorf("joinNode.buildCCL: can not find ccl for col(ID: %d, name: %s)", c.UniqueID, c.OrigName)
		}
		if joinCc, ok := joinKeyCCLs[c.UniqueID]; ok {
			cc = joinCc.Clone()
		}
		result[c.UniqueID] = cc
	}

	n.ccl = result
	return nil
}

func (n *SelectionNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)

	sel, ok := n.lp.(*core.LogicalSelection)
	if !ok {
		return fmt.Errorf("assert failed while selectionNode buildCCL, expected: core.LogicalSelection, actual: %T", n.lp)
	}
	for i, expr := range sel.Conditions {
		if col, ok := expr.(*expression.Column); ok && col.UseAsThreshold {
			args := []expression.Expression{col, &expression.Constant{
				Value:   types.NewDatum(GroupByThreshold),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}}
			newExpr, err := expression.NewFunction(sessionctx.NewContext(), ast.GE, types.NewFieldType(mysql.TypeTiny), args...)
			if err != nil {
				return fmt.Errorf("SelectionNode buildCCL: %s", err)
			}
			sel.Conditions[i] = newExpr
		}
	}
	var conditionCCL *ccl.CCL
	for i, expr := range sel.Conditions {
		cc, err := ccl.InferExpressionCCL(expr, childCCL)
		if err != nil {
			return fmt.Errorf("selectionNode buildCCL: %s", err)
		}
		if i == 0 {
			conditionCCL = cc.Clone()
			continue
		}
		conditionCCL, err = ccl.InferBinaryOpOutputVisibility(operator.OpNameLogicalAnd, conditionCCL, cc)
		if err != nil {
			return fmt.Errorf("selectionNode buildCCL: %s", err)
		}
	}

	// condition ccls for all parties are plaintext
	result := make(map[int64]*ccl.CCL)
	for _, c := range n.Schema().Columns {
		cc, ok := childCCL[c.UniqueID]
		if !ok {
			return fmt.Errorf("selectionNode.buildCCL: ccl is nil for column %d", c.UniqueID)
		}
		newCC := cc.Clone()
		newCC.UpdateMoreRestrictedCCLFrom(conditionCCL)
		result[c.UniqueID] = newCC
	}
	n.condVis = conditionCCL
	n.ccl = result
	return nil
}

func extractChildCCL(n logicalNode) map[int64]*ccl.CCL {
	childCCL := make(map[int64]*ccl.CCL)
	for _, child := range n.Children() {
		for id, level := range child.CCL() {
			childCCL[id] = level
		}
	}
	return childCCL
}

func (n *ApplyNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)
	apply, ok := n.lp.(*core.LogicalApply)
	if !ok {
		return fmt.Errorf("assert failed while applyNode buildCCL, expected: core.LogicalApply, actual: %T", n.lp)
	}
	if len(apply.OtherConditions)+len(apply.EqualConditions) != 1 {
		return fmt.Errorf("fail to check conditions: Apply.buildCCL doesn't support condition other:%s, equal:%s", apply.OtherConditions, apply.EqualConditions)
	}
	var sFunc *expression.ScalarFunction
	if len(apply.OtherConditions) > 0 {
		ok := false
		sFunc, ok = apply.OtherConditions[0].(*expression.ScalarFunction)
		if !ok {
			return fmt.Errorf("fail to check conditions: Apply.buildCCL doesn't support condition %s", apply.OtherConditions[0])
		}
	}
	if len(apply.EqualConditions) > 0 {
		conditions := apply.EqualConditions
		sFunc = conditions[0]
	}
	if sFunc.FuncName.L != ast.EQ {
		return fmt.Errorf("fail to check conditions: Apply.buildCCL doesn't support condition %s", sFunc)
	}

	correlatedFunctionCCL, err := ccl.InferExpressionCCL(sFunc, childCCL)
	if err != nil {
		return err
	}
	result := map[int64]*ccl.CCL{}
	switch apply.JoinType {
	case core.AntiLeftOuterSemiJoin, core.LeftOuterSemiJoin: // SELECT ta.id [NOT] IN (select tb.id from tb) as f from ta
		for _, c := range n.Schema().Columns[:len(n.Schema().Columns)-1] {
			result[c.UniqueID] = childCCL[c.UniqueID]
		}
		correlatedId := n.Schema().Columns[len(n.Schema().Columns)-1].UniqueID
		result[correlatedId] = correlatedFunctionCCL
		colTracer.SetSourcePartiesFromExpr(correlatedId, sFunc)
	case core.AntiSemiJoin, core.SemiJoin: // select ta.id, ta.x1 from ta WHERE ta.id [NOT] IN (select tb.id from tb)
		for _, c := range n.Schema().Columns {
			result[c.UniqueID] = childCCL[c.UniqueID].Clone()
			result[c.UniqueID].UpdateMoreRestrictedCCLFrom(correlatedFunctionCCL)
		}
	default:
		return fmt.Errorf("fail to check join type: unsupported join type %s", apply.JoinType)
	}
	n.ccl = result
	return nil
}

func (n *AggregationNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)
	agg, ok := n.lp.(*core.LogicalAggregation)
	if !ok {
		return fmt.Errorf("assert failed while aggregationNode buildCCL, expected: core.LogicalAggregation, actual: %T", n.lp)
	}
	childCCLAfterGroupBy := make(map[int64]*ccl.CCL)
	for id, cc := range childCCL {
		childCCLAfterGroupBy[id] = cc.Clone()
	}
	// 1. infer group by key's ccl
	var groupKeyCC []*ccl.CCL
	for _, expr := range agg.GroupByItems {
		cols := expression.ExtractColumns(expr)
		for _, col := range cols {
			cc, exist := childCCLAfterGroupBy[col.UniqueID]
			if !exist {
				return fmt.Errorf("failed to get ccl for column %+v", col)
			}
			for _, p := range cc.Parties() {
				if cc.LevelFor(p) == ccl.GroupBy {
					cc.SetLevelForParty(p, ccl.Plain)
				}
			}
			groupKeyCC = append(groupKeyCC, cc)
		}
	}
	// 2. infer group func's ccl which influenced by group key and function's args
	result := make(map[int64]*ccl.CCL)
	for i, aggFunc := range agg.AggFuncs {
		argCCL, err := ccl.InferExpressionCCL(aggFunc.Args[0], childCCLAfterGroupBy)
		if err != nil {
			return fmt.Errorf("aggregationNode.buildCCL: %v", err)
		}
		outputCCL := argCCL.Clone()
		switch aggFunc.Name {
		case ast.AggFuncCount:
			// for agg count, if len(group by keys) = 0, then set plaintext for all parties
			// if len(group by keys) != 0, if ccl for party p is not encrypt, set it to plaintext
			for _, p := range outputCCL.Parties() {
				outputCCL.SetLevelForParty(p, ccl.Plain)
			}
			for _, cc := range groupKeyCC {
				for _, p := range cc.Parties() {
					if cc.LevelFor(p) == ccl.Unknown {
						outputCCL.SetLevelForParty(p, cc.LevelFor(p))
						break
					}
					if cc.LevelFor(p) == ccl.Encrypt {
						outputCCL.SetLevelForParty(p, cc.LevelFor(p))
					}
				}
			}
		case ast.AggFuncFirstRow:
			// if column is not one of group keys, refuse the query. for example:
			// select t.a, count(t.b) from t group by t.b ->
			// DataScan(t)->Aggr(count(test.t.b),firstrow(test.t.a))->Projection([test.t.a Column#16])
			for _, cc := range groupKeyCC {
				outputCCL.UpdateMoreRestrictedCCLFrom(cc)
			}

		// TODO(xiaoyuan) support AggFuncStddevPop, AggFuncMedian later
		// sum,avg,max,min are all aggregate operators
		case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncMax, ast.AggFuncMin:
			for _, cc := range groupKeyCC {
				outputCCL.UpdateMoreRestrictedCCLFrom(cc)
			}
			for _, p := range outputCCL.Parties() {
				if outputCCL.LevelFor(p) == ccl.Aggregate {
					outputCCL.SetLevelForParty(p, ccl.Plain)
				}
			}
		default:
			return fmt.Errorf("unimplemented op: %s", aggFunc.Name)
		}
		result[agg.Schema().Columns[i].UniqueID] = outputCCL
	}
	n.ccl = result
	return nil
}

func (n *UnionAllNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(colTracer); err != nil {
		return err
	}
	childCCL := extractChildCCL(n)
	union, ok := n.lp.(*core.LogicalUnionAll)
	if !ok {
		return fmt.Errorf("assert failed while buildCCL, expected: *core.LogicalUnionAll, actual: %T", n.lp)
	}
	result := map[int64]*ccl.CCL{}
	var ccls []*ccl.CCL
	for _, cc := range childCCL {
		ccls = append(ccls, cc)
	}
	parties := ccl.ExtractPartyCodes(ccls)
	for i, c := range union.Schema().Columns {
		newCC := ccl.CreateAllPlainCCL(parties)
		for _, child := range n.Children() {
			cc, exist := childCCL[child.Schema().Columns[i].UniqueID]
			if !exist {
				return fmt.Errorf("failed to find ccl for column %+v", c)
			}
			newCC.UpdateMoreRestrictedCCLFrom(cc)
		}
		for _, p := range parties {
			if newCC.LevelFor(p) == ccl.Unknown {
				childCCLStr := "failed to check union ccl: "
				for _, child := range n.Children() {
					childCCLStr += fmt.Sprintf(" ccl of child %d is (%v)", i, childCCL[child.Schema().Columns[i].UniqueID])
				}
				return fmt.Errorf(childCCLStr)
			}
		}
		result[c.UniqueID] = newCC
	}
	n.ccl = result
	return nil
}

func (n *LimitNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("limit function not supported yet")
}

func (n *SortNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("sort function not supported yet")
}

func (n *WindowNode) buildCCL(colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("window function not supported yet")
}
