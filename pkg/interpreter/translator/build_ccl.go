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

	"github.com/sirupsen/logrus"

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

func (n *baseNode) buildChildCCL(ctx *ccl.Context, columnTracer *ccl.ColumnTracer) error {
	n.dataSourceParties = []string{}
	for _, c := range n.Children() {
		if err := c.buildCCL(ctx, columnTracer); err != nil {
			return err
		}
		n.dataSourceParties = append(n.dataSourceParties, c.DataSourceParty()...)
	}
	n.dataSourceParties = sliceutil.SliceDeDup(n.dataSourceParties)
	return nil
}

func (n *DataSourceNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
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

func (n *ProjectionNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for ProjectionNode: %v", err)
	}
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

func (n *JoinNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for JoinNode: %v", err)
	}

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
	// if table_alice joins table_bob, then alice and bob can see each other's join payload: payloadVisible[alice][bob] = payloadVisible[bob][alice] = true
	payloadVisible := make(map[string]map[string]bool)
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

		joinKeyPairs = append(joinKeyPairs, joinKeyPair{
			leftId:  leftId,
			rightId: rightId,
		})

		// set and check ccl
		leftSourceParties := colTracer.FindSourceParties(leftId)
		rigthSourceParties := colTracer.FindSourceParties(rightId)
		// temporaryly record payloadVisible for inner join only
		if join.JoinType == core.InnerJoin {
			for _, lp := range leftSourceParties {
				for _, rp := range rigthSourceParties {
					if _, ok := payloadVisible[lp]; !ok {
						payloadVisible[lp] = make(map[string]bool)
					}
					payloadVisible[lp][rp] = true

					if _, ok := payloadVisible[rp]; !ok {
						payloadVisible[rp] = make(map[string]bool)
					}
					payloadVisible[rp][lp] = true
				}
			}
		}
		for _, p := range leftSourceParties {
			if rightCc.LevelFor(p) == ccl.Join {
				if join.JoinType == core.InnerJoin || join.JoinType == core.LeftOuterJoin {
					rightCc.SetLevelForParty(p, ccl.Plain)
				}
			}
			if !rightCc.IsVisibleFor(p) && rightCc.LevelFor(p) != ccl.Join {
				return fmt.Errorf("joinNode.buildCCL: join on condition (%s) failed: column(%s) ccl(%v) for party(%s) does not belong to (PLAINTEXT_AFTER_JOIN, PLAINTEXT)",
					equalCondition.String(), equalCondition.GetArgs()[1].String(), rightCc.LevelFor(p).String(), p)
			}
		}

		for _, p := range rigthSourceParties {
			if leftCc.LevelFor(p) == ccl.Join {
				if join.JoinType == core.InnerJoin || join.JoinType == core.RightOuterJoin {
					leftCc.SetLevelForParty(p, ccl.Plain)
				}
			}
			if !leftCc.IsVisibleFor(p) && leftCc.LevelFor(p) != ccl.Join {
				return fmt.Errorf("joinNode.buildCCL: join on condition (%s) failed: column(%s) ccl(%v) for party(%s) does not belong to (PLAINTEXT_AFTER_JOIN, PLAINTEXT)",
					equalCondition.String(), equalCondition.GetArgs()[0].String(), leftCc.LevelFor(p).String(), p)
			}
		}

		joinKeyCCLs[leftId] = leftCc
		joinKeyCCLs[rightId] = rightCc
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
		} else {
			// set for PLAINTEXT_AS_JOIN_PAYLOAD
			for _, p := range cc.Parties() {
				if cc.LevelFor(p) == ccl.AsJoinPayload {
					for _, sp := range colTracer.FindSourceParties(c.UniqueID) {
						if payloadVisible[p][sp] {
							cc.SetLevelForParty(p, ccl.Plain)
							logrus.Infof("set '%+v' from PLAINTEXT_AS_JOIN_PAYLOAD to PLAINTEXT for %s", c, p)
							break
						}
					}
				}
			}
		}

		result[c.UniqueID] = cc
	}

	n.ccl = result
	return nil
}

func (n *SelectionNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for SelectionNode: %v", err)
	}

	sel, ok := n.lp.(*core.LogicalSelection)
	if !ok {
		return fmt.Errorf("assert failed while selectionNode buildCCL, expected: core.LogicalSelection, actual: %T", n.lp)
	}
	for i, expr := range sel.Conditions {
		col, ok := expr.(*expression.Column)
		if (!ok) || (!col.UseAsThreshold) {
			continue
		}
		if ctx.GroupByThreshold <= 0 {
			return fmt.Errorf("SelectionNode buildCCL: group by threshold %d must be greater than zero", ctx.GroupByThreshold)
		}
		if ctx.GroupByThreshold > 1 {
			args := []expression.Expression{col, &expression.Constant{
				Value:   types.NewDatum(int(ctx.GroupByThreshold)),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}}
			newExpr, err := expression.NewFunction(sessionctx.NewContext(), ast.GE, types.NewFieldType(mysql.TypeTiny), args...)
			if err != nil {
				return fmt.Errorf("SelectionNode buildCCL: %s", err)
			}
			sel.Conditions[i] = newExpr
			sel.AffectedByGroupThreshold = true
		} else {
			// remove the threshold condition, may cause empty selection node
			sel.Conditions = append(sel.Conditions[:i], sel.Conditions[i+1:]...)
		}
	}
	if len(sel.Conditions) == 0 {
		n.ccl = childCCL
		return nil
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

func extractChildCCL(n logicalNode) (map[int64]*ccl.CCL, error) {
	childCCL := make(map[int64]*ccl.CCL)
	for _, child := range n.Children() {
		for id, level := range child.CCL() {
			if pre, ok := childCCL[id]; ok {
				return nil, fmt.Errorf("extractChildCCL: child column id %v duplicated set ccl, pre %v, cur %v", id, pre, level)
			}
			childCCL[id] = level
		}
	}
	return childCCL, nil
}

func (n *ApplyNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for ApplyNode: %v", err)
	}
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

func SetAllPlainWhenLevelGroupBy(cc *ccl.CCL) {
	for _, p := range cc.Parties() {
		if cc.LevelFor(p) == ccl.GroupBy {
			cc.SetLevelForParty(p, ccl.Plain)
		}
	}
}

func (n *AggregationNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for AggregationNode: %v", err)
	}
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
			SetAllPlainWhenLevelGroupBy(cc)
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
		// for distinct, if ccl is group by, set to plaintext, otherwise return
		if agg.ProducedByDistinct {
			SetAllPlainWhenLevelGroupBy(outputCCL)
			result[agg.Schema().Columns[i].UniqueID] = outputCCL
			continue
		}
		switch aggFunc.Name {
		case ast.AggFuncCount:
			if len(groupKeyCC) == 0 {
				// check ccls of all cols in this schema
				// if one of the ccls is not unknown, then the result is plain
				// TODO(xiaoyuan): check here if null is supported
				for _, p := range outputCCL.Parties() {
					outputCCL.SetLevelForParty(p, ccl.Unknown)
					for _, cc := range childCCL {
						if cc.LevelFor(p) != ccl.Unknown {
							outputCCL.SetLevelForParty(p, ccl.Plain)
							break
						}
					}
				}
				break
			}
			// for agg count if len(group by keys) != 0, ccl of the result is determined by group by keys
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

func (n *UnionAllNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}
	union, ok := n.lp.(*core.LogicalUnionAll)
	if !ok {
		return fmt.Errorf("assert failed while buildCCL, expected: *core.LogicalUnionAll, actual: %T", n.lp)
	}
	result := map[int64]*ccl.CCL{}
	var ccls []*ccl.CCL
	for _, child := range n.Children() {
		for _, cc := range child.CCL() {
			ccls = append(ccls, cc)
		}
	}
	parties := ccl.ExtractPartyCodes(ccls)
	for i, c := range union.Schema().Columns {
		newCC := ccl.CreateAllPlainCCL(parties)
		for _, child := range n.Children() {
			cc, exist := child.CCL()[child.Schema().Columns[i].UniqueID]
			if !exist {
				return fmt.Errorf("failed to find ccl for column %+v", c)
			}
			newCC.UpdateMoreRestrictedCCLFrom(cc)
		}
		for _, p := range parties {
			if newCC.LevelFor(p) == ccl.Unknown {
				childCCLStr := "failed to check union ccl: "
				for _, child := range n.Children() {
					childCCLStr += fmt.Sprintf(" ccl of child %d is (%v)", i, child.CCL()[child.Schema().Columns[i].UniqueID])
				}
				return fmt.Errorf(childCCLStr)
			}
		}
		result[c.UniqueID] = newCC
	}
	n.ccl = result
	return nil
}

func (n *LimitNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}

	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildCCL for LimitNode: %v", err)
	}

	n.ccl = childCCL
	return nil
}

func (n *SortNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("sort function not supported yet")
}

func (n *WindowNode) buildCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	window, ok := n.lp.(*core.LogicalWindow)
	if !ok {
		return fmt.Errorf("assert failed while windowNode buildCCL, expected: core.LogicalWindow, actual: %T", n.lp)
	}

	if len(window.WindowFuncDescs) != 1 {
		return fmt.Errorf("assert failed for window functions expect(1) but actual(%v)", len(window.WindowFuncDescs))
	}

	desc := window.WindowFuncDescs[0]
	if ccl.IsRankWindowFunc(desc.Name) {
		return n.buildRankWindowCCL(ctx, colTracer)
	} else {
		return n.buildAggWindowCCL(ctx, colTracer)
	}

}

func (n *WindowNode) buildRankWindowCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	if err := n.buildChildCCL(ctx, colTracer); err != nil {
		return err
	}

	childCCL, err := extractChildCCL(n)
	if err != nil {
		return fmt.Errorf("buildRankWindowCCL: failed to build child ccl for child node of WindowNode: %v", err)
	}
	window, ok := n.lp.(*core.LogicalWindow)
	if !ok {
		return fmt.Errorf("buildRankWindowCCL: assert failed while windowNode buildCCL, expected: core.LogicalWindow, actual: %T", n.lp)
	}

	if len(window.WindowFuncDescs) != 1 {
		return fmt.Errorf("buildRankWindowCCL: assert failed for window functions expect(1) but actual(%v)", len(window.WindowFuncDescs))
	}

	if len(window.OrderBy) == 0 {
		return fmt.Errorf("buildRankWindowCCL: order field is required in %v function", window.WindowFuncDescs[0].Name)
	}

	if len(window.PartitionBy) == 0 {
		return fmt.Errorf("buildRankWindowCCL: partition field is required in %v function", window.WindowFuncDescs[0].Name)
	}

	result := make(map[int64]*ccl.CCL)
	for id, cc := range childCCL {
		result[id] = cc.Clone()
	}

	// the window func result is the last column
	lastCol := window.Schema().Columns[len(window.Schema().Columns)-1]

	var newCC *ccl.CCL
	init, ok := childCCL[window.OrderBy[0].Col.UniqueID]
	if !ok {
		return fmt.Errorf("buildRankWindowCCL: failed to find ccl for col(%v: %v)", window.OrderBy[0].Col.OrigName, window.OrderBy[0].Col.UniqueID)
	}

	newCC = init.Clone()
	for _, item := range window.OrderBy[1:] {
		orderItemCCL, ok := childCCL[item.Col.UniqueID]
		if !ok {
			return fmt.Errorf("buildRankWindowCCL: failed to find ccl for col(%v: %v) in order list", item.Col.OrigName, item.Col.UniqueID)
		}

		newCC.UpdateMoreRestrictedCCLFrom(orderItemCCL)
	}

	for _, p := range ccl.ExtractPartyCodes([]*ccl.CCL{newCC}) {
		if newCC.LevelFor(p) == ccl.Rank {
			newCC.SetLevelForParty(p, ccl.Plain)
		}
	}

	result[lastCol.UniqueID] = newCC
	colTracer.AddSourceParties(lastCol.UniqueID, ccl.ExtractPartyCodes([]*ccl.CCL{newCC}))

	n.ccl = result
	return nil
}

func (n *WindowNode) buildAggWindowCCL(ctx *ccl.Context, colTracer *ccl.ColumnTracer) error {
	return fmt.Errorf("buildAggWindowCCL: agg window does not support yet")
}
