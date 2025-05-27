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
	"strconv"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/types"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

var JoinTypeLpToEp = map[core.JoinType]int{
	core.InnerJoin:      graph.InnerJoin,
	core.LeftOuterJoin:  graph.LeftOuterJoin,
	core.RightOuterJoin: graph.RightOuterJoin,
}

func (t *translator) buildApply(ln *ApplyNode) (err error) {
	apply, ok := ln.lp.(*core.LogicalApply)
	if !ok {
		return fmt.Errorf("buildApply: ApplyNode contains invalid LogicalPlan type %T", ln.lp)
	}
	if len(ln.Children()) != 2 {
		return fmt.Errorf("buildApply: unexpected number of children %v != 2", len(ln.children))
	}

	// no conditions on both sides, build as cross join
	if len(apply.OtherConditions) == 0 && len(apply.EqualConditions) == 0 && len(apply.RightConditions) == 0 && len(apply.LeftConditions) == 0 {
		resultIdToTensor, err := t.buildCrossJoin(ln.Children()[0], ln.Children()[1])
		if err != nil {
			return err
		}
		err = setResultTable(ln, resultIdToTensor)
		return err
	}

	if len(apply.OtherConditions)+len(apply.EqualConditions) != 1 || len(apply.RightConditions) != 0 || len(apply.LeftConditions) != 0 {
		return fmt.Errorf("buildApply: doesn't support condition other:%s, equal:%s,  right condition (%s), left condition (%s)",
			apply.OtherConditions, apply.EqualConditions, apply.RightConditions, apply.LeftConditions)
	}

	childIdToTensor := map[int64]*graph.Tensor{}
	for _, child := range ln.Children() {
		for i, c := range child.Schema().Columns {
			childIdToTensor[c.UniqueID] = child.ResultTable()[i]
		}
	}

	colIdToTensor := map[int64]*graph.Tensor{}
	for _, c := range apply.Schema().Columns {
		colIdToTensor[c.UniqueID] = childIdToTensor[c.UniqueID]
	}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()
	var sFunc *expression.ScalarFunction
	if len(apply.OtherConditions) > 0 {
		conditions := apply.OtherConditions
		sFunc, ok = conditions[0].(*expression.ScalarFunction)
		if !ok {
			return fmt.Errorf("buildApply: type assertion failed")
		}
	}
	if len(apply.EqualConditions) > 0 {
		conditions := apply.EqualConditions
		sFunc = conditions[0]
	}
	if sFunc.FuncName.L != ast.EQ || len(sFunc.GetArgs()) != 2 {
		return fmt.Errorf("buildApply: type assertion failed")
	}

	leftKeyT, err := t.buildExpression(sFunc.GetArgs()[0], childIdToTensor, true, ln)
	if err != nil {
		return err
	}
	rightKeyT, err := t.buildExpression(sFunc.GetArgs()[1], childIdToTensor, true, ln)
	if err != nil {
		return err
	}
	var filterT *graph.Tensor
	if t.CompileOpts.Batched {
		leftTs := ln.Children()[0].ResultTable()
		rightTs := ln.Children()[1].ResultTable()
		var parties []string
		var leftKeyTs, rightKeyTs []*graph.Tensor
		parties = append(parties, leftKeyT.OwnerPartyCode)
		parties = append(parties, rightKeyT.OwnerPartyCode)
		leftKeyTs, leftTs, err = t.addBucketNode([]*graph.Tensor{leftKeyT}, leftTs, parties)
		if err != nil {
			return err
		}
		rightKeyTs, _, err = t.addBucketNode([]*graph.Tensor{rightKeyT}, rightTs, parties)
		if err != nil {
			return err
		}
		leftKeyT = leftKeyTs[0]
		rightKeyT = rightKeyTs[0]
		for i, c := range ln.Children()[0].Schema().Columns {
			// replace not add new
			if _, ok := colIdToTensor[c.UniqueID]; !ok {
				continue
			}
			colIdToTensor[c.UniqueID] = leftTs[i]
		}
	}
	filterT, err = t.addInNode(sFunc, leftKeyT, rightKeyT)
	if err != nil {
		return err
	}
	// get result party list of IN-op result tensor
	reverseFilter := func(filter *graph.Tensor) (*graph.Tensor, error) {
		partyList := t.enginesInfo.GetParties()
		if filter.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyList = []string{filter.OwnerPartyCode}
		}
		return t.ep.AddNotNode("not", filterT, partyList)
	}

	selectIn := func(reversed bool) (err error) {
		var filter *graph.Tensor
		if reversed {
			filter, err = reverseFilter(filterT)
			if err != nil {
				return
			}
		} else {
			filter = filterT
		}
		cs := ln.Schema().Columns
		colIdToTensor[cs[len(cs)-1].UniqueID] = filter
		return nil
	}

	whereIn := func(reversed bool) (err error) {
		var filter *graph.Tensor
		if reversed {
			filter, err = reverseFilter(filterT)
			if err != nil {
				return
			}
		} else {
			filter = filterT
		}
		outs, err := t.addFilterNode(filter, colIdToTensor)
		if err != nil {
			return err
		}
		for _, c := range ln.Children()[0].Schema().Columns {
			colIdToTensor[c.UniqueID] = outs[c.UniqueID]
		}
		return nil
	}

	switch apply.JoinType {
	case core.AntiLeftOuterSemiJoin: // SELECT ta.id NOT IN (select tb.id from tb) as f from ta
		return selectIn(true)
	case core.LeftOuterSemiJoin: // SELECT ta.id IN (select tb.id from tb) as f from ta
		return selectIn(false)
	case core.AntiSemiJoin: // select ta.id, ta.x1 from ta WHERE ta.id NOT IN (select tb.id from tb)
		return whereIn(true)
	case core.SemiJoin: // select ta.id, ta.x1 from ta WHERE ta.id IN (select tb.id from tb)
		return whereIn(false)
	default:
		return fmt.Errorf("buildApply: invalid join type %s", apply.JoinType)
	}
}

func (t *translator) buildJoin(ln *JoinNode) (err error) {
	join, ok := ln.lp.(*core.LogicalJoin)
	if !ok {
		return fmt.Errorf("assert failed while translator buildJoin, expected: core.LogicalJoin, actual: %T", ln.lp)
	}
	if len(ln.Children()) != 2 {
		return fmt.Errorf("buildJoin: unexpected number of children %v != 2", len(ln.children))
	}
	if _, ok := JoinTypeLpToEp[join.JoinType]; !ok {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("buildJoin doesn't support join type %s", join.JoinType))
	}

	if len(join.OtherConditions) != 0 || len(join.RightConditions) != 0 || len(join.LeftConditions) != 0 {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("buildJoin doesn't support other condition (%+v), right condition (%+v), left condition (%+v)", join.OtherConditions, join.RightConditions, join.LeftConditions))
	}

	if len(join.EqualConditions) > 0 {
		return t.buildEQJoin(ln)
	}

	resultIdToTensor, err := t.buildCrossJoin(ln.Children()[0], ln.Children()[1])
	if err != nil {
		return err
	}
	err = setResultTable(ln, resultIdToTensor)
	return err
}

func (t *translator) buildEQJoin(ln *JoinNode) (err error) {
	join, ok := ln.lp.(*core.LogicalJoin)
	if !ok {
		return fmt.Errorf("assert failed while translator buildEQJoin, expected: core.LogicalJoin, actual: %T", ln.lp)
	}
	left, right := ln.Children()[0], ln.Children()[1]

	// step 1: collect join keys and payloads
	var leftKeyTs = []*graph.Tensor{}
	var rightKeyTs = []*graph.Tensor{}
	hasSharedKey := false
	for _, equalCondition := range join.EqualConditions {
		cols, err := extractEQColumns(equalCondition)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftIndexCol, rightIndexCol := cols[0], cols[1]
		leftT, err := left.FindTensorByColumnId(leftIndexCol.UniqueID)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftKeyTs = append(leftKeyTs, leftT)
		rightT, err := right.FindTensorByColumnId(rightIndexCol.UniqueID)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		rightKeyTs = append(rightKeyTs, rightT)

		if leftT.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE || rightT.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
			hasSharedKey = true
		}
	}
	leftUsed := core.GetUsedList(join.Schema().Columns, left.Schema())
	rightUsed := core.GetUsedList(join.Schema().Columns, right.Schema())
	leftTs := sliceutil.Take(left.ResultTable(), leftUsed)
	rightTs := sliceutil.Take(right.ResultTable(), rightUsed)
	if hasSharedKey {
		return t.buildSecretEQJoin(ln, leftKeyTs, rightKeyTs, leftTs, rightTs, sliceutil.Take(left.Schema().Columns, leftUsed), sliceutil.Take(right.Schema().Columns, rightUsed))
	}

	// step 2: build join index with psi join
	var parties []string
	for i, leftKey := range leftKeyTs {
		leftParty := leftKey.OwnerPartyCode
		rightParty := rightKeyTs[i].OwnerPartyCode
		// TODO(xiaoyuan) support local join
		if leftParty == rightParty {
			return fmt.Errorf("buildEQJoin: invalid [leftParty, rightParty] = [%v, %v]", leftParty, rightParty)
		}
		if i == 0 {
			parties = append(parties, leftParty, rightParty)
		} else {
			if leftParty != parties[0] || rightParty != parties[1] {
				return fmt.Errorf("buildEQJoin: Error current [leftParty, rightParty] not equal pre [leftParty, rightParty] =  [%v, %v] not equal [%v, %v]", leftParty, rightParty, parties[0], parties[1])
			}
		}
	}

	if t.CompileOpts.Batched {
		leftKeyTs, leftTs, err = t.addBucketNode(leftKeyTs, leftTs, parties)
		if err != nil {
			return err
		}
		rightKeyTs, rightTs, err = t.addBucketNode(rightKeyTs, rightTs, parties)
		if err != nil {
			return err
		}
	}
	joinArgs := graph.JoinNodeArgs{
		HasLeftResult:  len(leftTs) > 0,
		HasRightResult: len(rightTs) > 0,
		PartyCodes:     parties,
		JoinType:       JoinTypeLpToEp[join.JoinType],
		PsiAlg:         t.CompileOpts.GetOptimizerHints().GetPsiAlgorithmType(),
	}
	// for ub psi, we think both parties get result by default
	joinArgs.HasLeftResult = joinArgs.HasLeftResult || joinArgs.PsiAlg == proto.PsiAlgorithmType_OPRF || join.JoinType == core.LeftOuterJoin
	joinArgs.HasRightResult = joinArgs.HasRightResult || joinArgs.PsiAlg == proto.PsiAlgorithmType_OPRF || join.JoinType == core.RightOuterJoin
	leftIndexT, rightIndexT, err := t.ep.AddJoinNode("join", leftKeyTs, rightKeyTs, &joinArgs)
	if err != nil {
		return fmt.Errorf("buildEQJoin: %v", err)
	}

	leftIndexCC, rightIndexCC := createCCLForIndexT(ln.childDataSourceParties)
	// step 3: apply join index
	// record tensor id and tensor pointer in result table
	resultIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, resultIdToTensor)
	}()

	if len(leftTs) > 0 {
		leftParty := leftIndexT.OwnerPartyCode
		leftIndexT.CC = leftIndexCC
		leftFiltered, err := t.addFilterByIndexNode(leftIndexT, leftTs, leftParty)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		for i, c := range sliceutil.Take(left.Schema().Columns, leftUsed) {
			resultIdToTensor[c.UniqueID] = leftFiltered[i]
		}
	}
	if len(rightTs) > 0 {
		rightParty := rightIndexT.OwnerPartyCode
		rightIndexT.CC = rightIndexCC
		rightFiltered, err := t.addFilterByIndexNode(rightIndexT, rightTs, rightParty)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		for i, c := range sliceutil.Take(right.Schema().Columns, rightUsed) {
			resultIdToTensor[c.UniqueID] = rightFiltered[i]
		}
	}

	return
}

func (t *translator) buildSecretEQJoin(ln *JoinNode, leftKeyTs, rightKeyTs, leftTs, rightTs []*graph.Tensor, leftColumns, rightColumns []*expression.Column) (err error) {
	join, ok := ln.lp.(*core.LogicalJoin)
	if !ok {
		return fmt.Errorf("assert failed while translator buildSecretEQJoin, expected: core.LogicalJoin, actual: %T", ln.lp)
	}
	if join.JoinType != core.InnerJoin {
		return fmt.Errorf("buildSecretEQJoin: only support inner join, null not support in secret yet")
	}
	// convert private tensors to share
	{
		leftKeyTs, err = t.toShare(leftKeyTs)
		if err != nil {
			return err
		}
		rightKeyTs, err = t.toShare(rightKeyTs)
		if err != nil {
			return err
		}
		leftTs, err = t.toShare(leftTs)
		if err != nil {
			return err
		}
		rightTs, err = t.toShare(rightTs)
		if err != nil {
			return err
		}
	}

	leftTs, rightTs, err = t.ep.AddSecretJoinNode("secret_join", leftKeyTs, rightKeyTs, leftTs, rightTs, t.enginesInfo.GetParties())
	if err != nil {
		return fmt.Errorf("buildSecretEQJoin: %v", err)
	}

	resultIdToTensor := make(map[int64]*graph.Tensor)

	for i, c := range leftColumns {
		resultIdToTensor[c.UniqueID] = leftTs[i]
	}
	for i, c := range rightColumns {
		resultIdToTensor[c.UniqueID] = rightTs[i]
	}

	return setResultTable(ln, resultIdToTensor)
}

func (t *translator) toShare(inputs []*graph.Tensor) ([]*graph.Tensor, error) {
	var outputs []*graph.Tensor
	for _, input := range inputs {
		newT, err := t.converter.convertTo(input, &sharePlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, newT)
	}
	return outputs, nil
}

func createCCLForIndexT(childDataSourceParties [][]string) (left *ccl.CCL, right *ccl.CCL) {
	left = ccl.NewCCL()
	for _, party := range childDataSourceParties[0] {
		left.SetLevelForParty(party, ccl.Plain)
	}
	right = ccl.NewCCL()
	for _, party := range childDataSourceParties[1] {
		right.SetLevelForParty(party, ccl.Plain)
	}
	return
}

func extractResultTable(ln logicalNode, resultIdToTensor map[int64]*graph.Tensor) ([]*graph.Tensor, error) {
	rt := []*graph.Tensor{}
	for _, c := range ln.Schema().Columns {
		tensor, ok := resultIdToTensor[c.UniqueID]
		if !ok {
			return nil, fmt.Errorf("extractResultTable: unable to find columnID %v", c.UniqueID)
		}
		if ln.CCL() != nil {
			tensor.CC = ln.CCL()[c.UniqueID]
		}
		rt = append(rt, tensor)
	}
	return rt, nil
}

func setResultTable(ln logicalNode, resultIdToTensor map[int64]*graph.Tensor) error {
	rt, err := extractResultTable(ln, resultIdToTensor)
	if err != nil {
		return fmt.Errorf("setResultTable: %v", err)
	}
	return ln.SetResultTableWithDTypeCheck(rt)
}

func (t *translator) buildSelection(ln *SelectionNode) (err error) {
	selection, ok := ln.lp.(*core.LogicalSelection)
	if !ok {
		return fmt.Errorf("assert failed while translator buildSelection, expected: core.LogicalSelection, actual: %T", ln.lp)
	}
	t.AffectedByGroupThreshold = selection.AffectedByGroupThreshold

	if len(ln.Children()) != 1 {
		return fmt.Errorf("buildSelection: unexpected number of children %v != 1", len(ln.children))
	}
	child := ln.children[0]
	// record tensor id and tensor pointer
	colIdToTensor := map[int64]*graph.Tensor{}
	childIdToTensor := map[int64]*graph.Tensor{}
	// record tensor id and tensor pointer in result table
	resultIdToTensor := map[int64]*graph.Tensor{}
	for i, c := range child.Schema().Columns {
		childIdToTensor[c.UniqueID] = child.ResultTable()[i]
	}
	if len(selection.Conditions) == 0 {
		err = setResultTable(ln, childIdToTensor)
		return
	}
	for _, c := range selection.Schema().Columns {
		colIdToTensor[c.UniqueID] = childIdToTensor[c.UniqueID]
	}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, resultIdToTensor)
	}()

	// build filters
	filters := []*graph.Tensor{}
	for _, cond := range selection.Conditions {
		filter, err := t.buildExpression(cond, childIdToTensor, false, ln)
		if err != nil {
			return err
		}
		filters = append(filters, filter)
	}
	// ccl checked in build ccl
	// logical AND all filters
	filter := filters[0]
	for i := 1; i < len(filters); i++ {
		left, right := filter, filters[i]
		output, err := t.addBinaryNode("logical_and", operator.OpNameLogicalAnd, left, right)
		if err != nil {
			return err
		}
		filter = output
	}
	// private and share tensors filter have different tensor status
	shareTensors := []*graph.Tensor{}
	var shareIds []int64
	// private tensors need record it's owner party
	privateTensorsMap := make(map[string][]*graph.Tensor)
	privateIdsMap := make(map[string][]int64)
	for columnId, it := range sliceutil.SortedMap(colIdToTensor) {
		if len(filter.CC.GetVisibleParties()) == len(t.enginesInfo.GetParties()) {
			switch it.Status() {
			case proto.TensorStatus_TENSORSTATUS_SECRET:
				shareTensors = append(shareTensors, it)
				shareIds = append(shareIds, columnId)
			case proto.TensorStatus_TENSORSTATUS_PRIVATE:
				privateTensorsMap[it.OwnerPartyCode] = append(privateTensorsMap[it.OwnerPartyCode], it)
				privateIdsMap[it.OwnerPartyCode] = append(privateIdsMap[it.OwnerPartyCode], columnId)
			default:
				return fmt.Errorf("unsupported tensor status for selection node: %+v", it)
			}
		} else {
			if it.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE && filter.CC.IsVisibleFor(it.OwnerPartyCode) {
				privateTensorsMap[it.OwnerPartyCode] = append(privateTensorsMap[it.OwnerPartyCode], it)
				privateIdsMap[it.OwnerPartyCode] = append(privateIdsMap[it.OwnerPartyCode], columnId)
				continue
			}
			foundParty := false
			for _, party := range it.CC.GetVisibleParties() {
				if filter.CC.IsVisibleFor(party) {
					foundParty = true
					newT, err := t.converter.convertTo(it, &privatePlacement{partyCode: party})
					if err != nil {
						return err
					}
					privateTensorsMap[party] = append(privateTensorsMap[party], newT)
					privateIdsMap[party] = append(privateIdsMap[party], columnId)
					break
				}
			}
			if !foundParty {
				return fmt.Errorf("failed to find a party can see filter(%+v) and tensor(%+v)", filter, it)
			}
		}
	}

	if len(shareTensors) > 0 {
		// convert filter to public here, so filter must be visible to all parties
		publicFilter, err := t.converter.convertTo(filter, &publicPlacement{partyCodes: t.enginesInfo.GetParties()})
		if err != nil {
			return fmt.Errorf("buildSelection: %v", err)
		}
		// handling share tensors here
		output, err := t.ep.AddFilterNode("apply_filter", shareTensors, publicFilter, t.enginesInfo.GetParties())
		if err != nil {
			return fmt.Errorf("buildSelection: %v", err)
		}
		for i, id := range shareIds {
			resultIdToTensor[id] = output[i]
		}
	}
	// handling private tensors here
	if len(privateTensorsMap) > 0 {
		for p, ts := range sliceutil.SortedMap(privateTensorsMap) {
			newFilter, err := t.converter.convertTo(filter, &privatePlacement{partyCode: p})
			if err != nil {
				return fmt.Errorf("buildSelection: %v", err)
			}
			output, err := t.ep.AddFilterNode("apply_filter", ts, newFilter, []string{p})
			if err != nil {
				return fmt.Errorf("buildSelection: %v", err)
			}
			for i, id := range privateIdsMap[p] {
				resultIdToTensor[id] = output[i]
			}
		}
	}
	return nil
}

func (t *translator) buildProjection(ln *ProjectionNode) (err error) {
	proj, ok := ln.lp.(*core.LogicalProjection)
	if !ok {
		return fmt.Errorf("assert failed while translator buildProjection, expected: core.LogicalProjection, actual: %T", ln.lp)
	}
	if len(ln.Children()) != 1 {
		return fmt.Errorf("buildProjection: unexpected number of children %v != 1", len(ln.children))
	}

	child := ln.children[0]
	colIdToTensor := map[int64]*graph.Tensor{}
	for i, c := range child.Schema().Columns {
		colIdToTensor[c.UniqueID] = child.ResultTable()[i]
	}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	resultIdToTensor := map[int64]*graph.Tensor{}

	tensors := []*graph.Tensor{}
	for _, expr := range proj.Exprs {
		tensor, err := t.getTensorFromExpression(expr, colIdToTensor)
		if err != nil {
			return fmt.Errorf("buildProjection: %v", err)
		}
		tensors = append(tensors, tensor)
	}

	outputs, err := t.addBroadcastToNodeOndemand(tensors)
	if err != nil {
		return fmt.Errorf("buildProjection: %v", err)
	}

	for i := range proj.Exprs {
		cid := ln.Schema().Columns[i].UniqueID
		resultIdToTensor[cid] = outputs[i]
	}
	colIdToTensor = resultIdToTensor

	return nil
}

func (t *translator) buildAggregation(ln *AggregationNode) (err error) {
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return fmt.Errorf("buildAggregation: assert failed expected *core.LogicalAggregation, get %T", ln.LP())
	}
	if len(ln.Children()) != 1 {
		return fmt.Errorf("buildAggregation: failed to check number of children %v != 1", len(ln.children))
	}
	if len(agg.GroupByItems) > 0 {
		return t.buildGroupAggregation(ln)
	}
	child := ln.children[0]
	colIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	// Aggregation Function
	childColIdToTensor := t.getNodeResultTensor(child)
	for i, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
			return fmt.Errorf("buildAggregation: unsupported aggregation function %v", aggFunc)
		}
		switch aggFunc.Name {
		case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncAvg:
			// check arg type
			if aggFunc.Args[0].GetType().Tp == mysql.TypeString {
				return fmt.Errorf("buildAggregation: unsupported aggregation function %v with a string type argument", aggFunc.Name)
			}
			colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildAggregation: %v", err)
			}
			output, err := t.ep.AddReduceAggNode(aggFunc.Name, colT, map[string]*graph.Attribute{})
			if err != nil {
				return fmt.Errorf("buildAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
		case ast.AggFuncCount:
			// NOTE(yang.y): There are two mode for count function.
			// - The CompleteMode is the default mode in queries like `select count(*) from t`.
			//   In this mode, count function should be translated to ShapeOp.
			// - The FinalMode appears at `select count(*) from (t1 union all t2)`.
			//   The aggregation push down optimizer will rewrite the query plan to
			//   `select count_final(*) from (select count(*) from t1 union all select count(*) from t2)`.
			//   In this mode, count function be be translated to ReduceSumOp.
			// 	 If the pushed aggregation is grouped by unique key, it's no need to push it down.
			switch aggFunc.Mode {
			case aggregation.CompleteMode:
				var out *graph.Tensor
				if aggFunc.HasDistinct {
					colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
					if err != nil {
						return fmt.Errorf("buildAggregation: %v", err)
					}
					if colT.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
						partyCode := colT.OwnerPartyCode
						colT, err = t.addUniqueNode("unique", colT, partyCode)
						if err != nil {
							return fmt.Errorf("buildAggregation: add unique node: %v", err)
						}
						out, err = t.ep.AddReduceAggNode(ast.AggFuncCount, colT, map[string]*graph.Attribute{})
						if err != nil {
							return fmt.Errorf("buildAggregation: count: %v", err)
						}
					} else {
						// 1. sort
						keyTensor := []*graph.Tensor{colT}
						sortedDistinctCol, err := t.addSortNode("count.sort", keyTensor, keyTensor, false)
						if err != nil {
							return fmt.Errorf("buildAggregation: %v", err)
						}
						// 2. group mark
						groupMarkDistinct, err := t.addObliviousGroupMarkNode("group_mark", sortedDistinctCol)
						if err != nil {
							return fmt.Errorf("buildAggregation: %v", err)
						}
						// 3. sum(mark)
						out, err = t.ep.AddReduceAggNode(ast.AggFuncSum, groupMarkDistinct, map[string]*graph.Attribute{})
						if err != nil {
							return fmt.Errorf("buildAggregation: %v", err)
						}
					}
				} else if _, ok := aggFunc.Args[0].(*expression.Constant); ok {
					var partyCode string
					tensor := child.ResultTable()[0]
					switch tensor.Status() {
					case proto.TensorStatus_TENSORSTATUS_PRIVATE:
						partyCode = tensor.OwnerPartyCode
					case proto.TensorStatus_TENSORSTATUS_SECRET, proto.TensorStatus_TENSORSTATUS_PUBLIC:
						partyCode = t.enginesInfo.GetParties()[0]
					default:
						return fmt.Errorf("buildAggregation: count func doesn't support tensor status %v", tensor.Status())
					}
					out, err = t.ep.AddShapeNode("shape", tensor, 0, partyCode)
					if err != nil {
						return fmt.Errorf("buildAggregation: count: %v", err)
					}
				} else {
					colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
					if err != nil {
						return fmt.Errorf("buildAggregation: %v", err)
					}
					out, err = t.ep.AddReduceAggNode(ast.AggFuncCount, colT, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildAggregation: %v", err)
					}
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = out
			case aggregation.FinalMode:
				colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
				if err != nil {
					return fmt.Errorf("buildAggregation: %v", err)
				}
				output, err := t.ep.AddReduceAggNode(ast.AggFuncSum, colT, map[string]*graph.Attribute{})
				if err != nil {
					return fmt.Errorf("buildAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
			default:
				return fmt.Errorf("buildAggregation: unrecognized count func mode %v", aggFunc.Mode)
			}
		case ast.AggPercentileDisc:
			colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildAggregation: %v", err)
			}
			percent, err := strconv.ParseFloat(aggFunc.Args[1].String(), 64)
			if err != nil {
				return fmt.Errorf("buildAggregation: invalid percentile %s given, error; %v", aggFunc.Args[1].String(), err)
			}

			if percent < 0 || percent > 1 {
				return fmt.Errorf("buildAggregation: invalid percentile value %v, it should be in [0, 1]", percent)
			}
			attr := &graph.Attribute{}
			attr.SetDouble(percent)
			output, err := t.ep.AddReduceAggNode(ast.AggPercentileDisc, colT, map[string]*graph.Attribute{"percent": attr})
			if err != nil {
				return fmt.Errorf("buildAggregation: %v", err)
			}

			colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
		default:
			return fmt.Errorf("buildAggregation: unsupported aggregation function %v", aggFunc)
		}
	}

	return nil
}

func (t *translator) buildGroupAggregation(ln *AggregationNode) (err error) {
	party, err := t.findPartyHandleAll(ln)
	if err != nil {
		return fmt.Errorf("buildGroupAggregation: %v", err)
	}
	if party != "" {
		return t.buildPrivateGroupAggregation(ln, party)
	} else {
		return t.buildObliviousGroupAggregation(ln)
	}
}

// check whether there are any parties which can view all columns in partition and order by
func (t *translator) findVisibleWindowParty(ln *WindowNode) (string, error) {
	window, ok := ln.lp.(*core.LogicalWindow)
	if !ok {
		return "", fmt.Errorf("findVisibleWindowParty: failed to convert to logical window")
	}

	child := ln.Children()[0]

	partyCandidate := map[string]bool{}
	for _, pc := range t.enginesInfo.GetParties() {
		partyCandidate[pc] = true
	}

	for _, item := range window.PartitionBy {
		cc, err := ccl.InferExpressionCCL(item.Col, child.CCL())
		if err != nil {
			return "", fmt.Errorf("findVisibleWindowParty: %v", err)
		}

		for _, pc := range t.enginesInfo.GetParties() {
			if !cc.IsVisibleFor(pc) {
				delete(partyCandidate, pc)
			}
		}
	}

	for _, item := range window.OrderBy {
		cc, err := ccl.InferExpressionCCL(item.Col, child.CCL())
		if err != nil {
			return "", fmt.Errorf("findVisibleWindowParty: %v", err)
		}

		for _, pc := range t.enginesInfo.GetParties() {
			if !cc.IsVisibleFor(pc) {
				delete(partyCandidate, pc)
			}
		}
	}

	partyCandidateSlice := sliceutil.SortMapKeyForDeterminism(partyCandidate)
	if len(partyCandidateSlice) > 0 {
		return partyCandidateSlice[0], nil
	}

	return "", nil
}

func (t *translator) findPartyWithAccessToAllTensors(inputs []*graph.Tensor) (string, error) {
	partyCandidate := map[string]bool{}
	for _, pc := range t.enginesInfo.GetParties() {
		partyCandidate[pc] = true
	}

	for _, item := range inputs {
		for pc := range partyCandidate {
			if !item.CC.IsVisibleFor(pc) {
				delete(partyCandidate, pc)
			}
		}
	}

	parties := sliceutil.SortMapKeyForDeterminism(partyCandidate)
	for _, p := range parties {
		return p, nil
	}

	return "", nil
}

// find a party who:
//  1. owns plaintext ccl for all group-by keys so it can calculate the group id;
//  2. can handle all aggs: a) agg with plaintext ccl; b) simple count; c) HE suitable;
//
// return "" if not exist
func (t *translator) findPartyHandleAll(ln *AggregationNode) (string, error) {
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return "", fmt.Errorf("findPartyHandleAll: assert failed expected *core.LogicalAggregation, get %T", ln.LP())
	}
	child := ln.Children()[0]

	partyCandidate := map[string]bool{}
	for _, pc := range t.enginesInfo.GetParties() {
		partyCandidate[pc] = true
	}
	// filter partyCandidate with groupby keys
	for _, item := range agg.GroupByItems {
		cc, err := ccl.InferExpressionCCL(item, child.CCL())
		if err != nil {
			return "", fmt.Errorf("findPartyHandleAll: %v", err)
		}
		for _, pc := range t.enginesInfo.GetParties() {
			if !cc.IsVisibleFor(pc) {
				delete(partyCandidate, pc)
			}
		}
	}
	// filter with agg items
	partyCandidateSlice := sliceutil.SortMapKeyForDeterminism(partyCandidate)
	for _, party_candidate := range partyCandidateSlice {
		canHandleAggs := true
		for _, aggFunc := range agg.AggFuncs {
			if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
				return "", fmt.Errorf("findPartyHandleAll: args length > 1 for %v", aggFunc)
			}
			cc, err := ccl.InferExpressionCCL(aggFunc.Args[0], child.CCL())
			if err != nil {
				return "", fmt.Errorf("findPartyHandleAll: %v", err)
			}

			if !cc.IsVisibleFor(party_candidate) && !isSimpleCount(aggFunc) && !isHeSuitable(aggFunc, cc) {
				canHandleAggs = false
				break
			}
		}
		if canHandleAggs {
			return party_candidate, nil
		}
	}

	return "", nil
}

func (t *translator) getNodeResultTensor(ln logicalNode) map[int64]*graph.Tensor {
	colIdToTensor := map[int64]*graph.Tensor{}
	for i, tensor := range ln.ResultTable() {
		colIdToTensor[ln.Schema().Columns[i].UniqueID] = tensor
	}
	return colIdToTensor
}

func (t *translator) buildPrivateGroupAggregation(ln *AggregationNode, party string) (err error) {
	colIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	// 1. build group id
	groupId, groupNum, err := t.buildGroupId(ln, party)
	if err != nil {
		return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
	}

	// 2. build aggs
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return fmt.Errorf("buildPrivateGroupAggregation: cast to LogicalAggregation failed")
	}
	childColIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	for i, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
			return fmt.Errorf("buildPrivateGroupAggregation: unsupported aggregation function %v, expect len(aggFunc.Args)=1, but got %v", aggFunc, len(aggFunc.Args))
		}
		// deal with simple count, which no need to buildExpression
		if isSimpleCount(aggFunc) {
			outputs, err := t.addGroupAggNode(ast.AggFuncCount, operator.OpNameGroupCount, groupId, groupNum, []*graph.Tensor{groupId}, party, map[string]*graph.Attribute{})
			if err != nil {
				return fmt.Errorf("buildPrivateGroupAggregation: count(*): %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
			continue
		}

		colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
		if err != nil {
			return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
		}
		switch aggFunc.Name {
		case ast.AggFuncCount:
			switch aggFunc.Mode {
			case aggregation.CompleteMode:
				if aggFunc.HasDistinct {
					outputs, err := t.addGroupAggNode("count_distinct", operator.OpNameGroupCountDistinct, groupId, groupNum, []*graph.Tensor{colT}, party, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildPrivateGroupAggregation: count distinct complete mode: %v", err)
					}
					colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
				} else {
					return fmt.Errorf("buildPrivateGroupAggregation: count(*) should not reach here")
				}
			case aggregation.FinalMode:
				switch aggFunc.Args[0].(type) {
				case *expression.Column:
					outputs, err := t.addGroupAggNode(ast.AggFuncSum, operator.OpNameGroupSum, groupId, groupNum, []*graph.Tensor{colT}, party, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildPrivateGroupAggregation: count final mode: %v", err)
					}
					colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
				default:
					return fmt.Errorf("buildPrivateGroupAggregation: unsupported count final type %v", aggFunc)
				}
			default:
				return fmt.Errorf("buildPrivateGroupAggregation: aggFunc.Mode %v", aggFunc.Mode)
			}
		case ast.AggFuncFirstRow, ast.AggFuncMin, ast.AggFuncMax, ast.AggFuncAvg:
			outputs, err := t.addGroupAggNode(aggFunc.Name, operator.GroupAggOp[aggFunc.Name], groupId, groupNum, []*graph.Tensor{colT}, party, map[string]*graph.Attribute{})
			if err != nil {
				return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
		case ast.AggFuncSum: // deal with agg which may run in HE
			if colT.CC.IsVisibleFor(party) {
				outputs, err := t.addGroupAggNode(ast.AggFuncSum, operator.OpNameGroupSum, groupId, groupNum, []*graph.Tensor{colT}, party, map[string]*graph.Attribute{})
				if err != nil {
					return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
			} else {
				// run in HE
				var colTParty string
				if colT.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
					// If colT is Private Tensor, encrypt colT in colT.OwnerPartyCode to avoid colT's copying
					colTParty = colT.OwnerPartyCode
				} else {
					colTParty = colT.CC.GetVisibleParties()[0]
				}
				output, err := t.addGroupHeSumNode("he_sum", groupId, groupNum, colT, party, colTParty)
				if err != nil {
					return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
			}
		case ast.AggPercentileDisc:
			if len(aggFunc.Args) != 2 {
				return fmt.Errorf("buildPrivateGroupAggregation: percentile_disc requires 2 arguments, but received %d", len(aggFunc.Args))
			}
			attr := &graph.Attribute{}
			percent, err := strconv.ParseFloat(aggFunc.Args[1].String(), 64)
			if err != nil {
				return fmt.Errorf("buildPrivateGroupAggregation: %s is not a valid float value", aggFunc.Args[1].String())
			}

			if percent < 0 || percent > 1 {
				return fmt.Errorf("buildPrivateGroupAggregation: percent should be in [0, 1], but got %v", percent)
			}

			attr.SetDouble(percent)
			outputs, err := t.addGroupAggNode(ast.AggPercentileDisc, operator.OpNameGroupPercentileDisc, groupId, groupNum, []*graph.Tensor{colT}, party, map[string]*graph.Attribute{"percent": attr})
			if err != nil {
				return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
		default:
			return fmt.Errorf("buildPrivateGroupAggregation: unsupported aggregation function %v", aggFunc)
		}
	}

	return err
}

func (t *translator) buildGroupId(ln *AggregationNode, partyCode string) (*graph.Tensor, *graph.Tensor, error) {
	groupCol := []*graph.Tensor{}
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return nil, nil, fmt.Errorf("buildGroupId: cast to LogicalAggregation failed")
	}
	childColIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	for _, item := range agg.GroupByItems {
		tensor, err := t.getTensorFromExpression(item, childColIdToTensor)
		if err != nil {
			return nil, nil, fmt.Errorf("buildGroupId: %v", err)
		}
		groupCol = append(groupCol, tensor)
	}
	return t.addGroupNode("group", groupCol, partyCode)
}

// find a party who owns PLAINTEXT ccl or PLAINTEXT_AFTER_GROUP_BY ccl for all group-by keys
func (t *translator) findRevealPartyForShuffleSort(sortKeys []*graph.Tensor) string {
	if !t.CompileOpts.SecurityCompromise.RevealGroupCount {
		return ""
	}
	// skip if only one group-by key and can be seen by any one
	if len(sortKeys) == 1 {
		for _, p := range t.enginesInfo.GetParties() {
			if sortKeys[0].CC.IsVisibleFor(p) {
				return ""
			}
		}
	}
	// filter partyCandidate with groupby keys
	for _, p := range t.enginesInfo.GetParties() {
		found := true
		for _, keyT := range sortKeys {
			if !keyT.CC.IsVisibleFor(p) && keyT.CC.LevelFor(p) != ccl.GroupBy {
				found = false
			}
		}
		if found {
			return p
		}
	}
	return ""
}

func (t *translator) buildObliviousGroupAggregation(ln *AggregationNode) (err error) {
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return fmt.Errorf("buildAggregation: assert failed expected *core.LogicalAggregation, get %T", ln.LP())
	}
	child := ln.Children()[0]

	// sort group by keys
	sortKeys := []*graph.Tensor{}
	childColIdToTensor := t.getNodeResultTensor(child)
	for _, key := range agg.GroupByItems {
		sortKey, err := t.getTensorFromExpression(key, childColIdToTensor)
		if err != nil {
			return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
		}
		sortKeys = append(sortKeys, sortKey)
	}
	partyToReveal := t.findRevealPartyForShuffleSort(sortKeys)
	var sortPayload []*graph.Tensor
	if partyToReveal != "" {
		shuffledTensor, err := t.addShuffleNode("shuffle", append(sortKeys, child.ResultTable()...))
		if err != nil {
			return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
		}

		shuffledSortKeys := shuffledTensor[:len(sortKeys)]
		var convertedKeyTs []*graph.Tensor
		for _, key := range shuffledSortKeys {
			// partyToReveal is visible for key after groupby, key can reveal to partyToReveal in groupby.
			// which the safety if same with revealing to partyToReveal after groupby
			key.CC.SetLevelForParty(partyToReveal, ccl.Plain)
			out, err := t.converter.convertTo(
				key, &privatePlacement{partyCode: partyToReveal})
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			convertedKeyTs = append(convertedKeyTs, out)
		}
		sortKeys = convertedKeyTs
		sortPayload = append(sortPayload, sortKeys...)
		// TODO(jingshi): remove duplicated payload
		sortPayload = append(sortPayload, shuffledTensor[len(sortKeys):]...)
	} else {
		sortPayload = append(sortPayload, sortKeys...)
		// TODO(jingshi): remove duplicated payload
		sortPayload = append(sortPayload, child.ResultTable()...)
	}

	out, err := t.addSortNode("sort", sortKeys, sortPayload, false)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: sort with groupIds err: %v", err)
	}
	sortedKeys := out[0:len(sortKeys)]
	sortedTensors := out[len(sortKeys):]
	sortedChildColIdToTensor := map[int64]*graph.Tensor{}
	for i, tensor := range sortedTensors {
		sortedChildColIdToTensor[child.Schema().Columns[i].UniqueID] = tensor
	}

	// create group mark
	groupMark, err := t.addObliviousGroupMarkNode("group_mark", sortedKeys)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}

	// add agg funcs
	colIdToTensor := map[int64]*graph.Tensor{}
	for i, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
			return fmt.Errorf("buildObliviousGroupAggregation: unsupported aggregation function %v", aggFunc)
		}
		switch aggFunc.Name {
		case ast.AggFuncFirstRow:
			colT, err := t.buildExpression(aggFunc.Args[0], sortedChildColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = colT
		case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncAvg:
			// check arg type
			if aggFunc.Args[0].GetType().EvalType() == types.ETString {
				return fmt.Errorf("buildObliviousGroupAggregation: unsupported aggregation function %s with a string type argument", aggFunc.Name)
			}
			colT, err := t.buildExpression(aggFunc.Args[0], sortedChildColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			output, err := t.addObliviousGroupAggNode(aggFunc.Name, groupMark, colT, map[string]*graph.Attribute{})
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			output.SkipDTypeCheck = true
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
		case ast.AggFuncCount:
			// NOTE(yang.y): There are two mode for count function.
			// - The CompleteMode is the default mode in queries like `select count(*) from t`.
			//   In this mode, count function should be translated to ObliviousGroupCount.
			// - The FinalMode appears at `select count(*) from (t1 union all t2)`.
			//   The aggregation push down optimizer will rewrite the query plan to
			//   `select count_final(*) from (select count(*) from t1 union all select count(*) from t2)`.
			//   In this mode, count function will be translated to ObliviousGroupSum.
			// do complete count
			// sum up partial count
			var output *graph.Tensor
			switch aggFunc.Mode {
			case aggregation.CompleteMode:
				if aggFunc.HasDistinct {
					colT, err := t.buildExpression(aggFunc.Args[0], sortedChildColIdToTensor, false, ln)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
					// Sort with group by key(maybe keyTs or groupIds) and distinct column.
					// Please note that group by key is the major sort key,
					// so the groupMark is still valid.
					var keyAndDistinct []*graph.Tensor
					keyAndDistinct = append(keyAndDistinct, sortedKeys...)
					keyAndDistinct = append(keyAndDistinct, colT)

					// TODO(jingshi): using free column to avoid sort if possible
					sortedDistinctCol, err := t.addSortNode("sort", keyAndDistinct, []*graph.Tensor{colT}, false)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
					// Use group by keys and distinctCol to create groupMarkFull,
					// which is equivalent to the result of groupMark logic or groupMarkDistinct
					groupMarkDistinct, err := t.addObliviousGroupMarkNode("group_mark", sortedDistinctCol)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
					groupMarkFull, err := t.addBinaryNode(operator.OpNameLogicalOr, operator.OpNameLogicalOr, groupMark, groupMarkDistinct)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}

					output, err = t.addObliviousGroupAggNode(ast.AggFuncSum, groupMark, groupMarkFull, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
				} else {
					output, err = t.addObliviousGroupAggNode(ast.AggFuncCount, groupMark, groupMark, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
				}
			case aggregation.FinalMode:
				switch x := aggFunc.Args[0].(type) {
				case *expression.Column:
					colT := sortedChildColIdToTensor[x.UniqueID]
					output, err = t.addObliviousGroupAggNode(ast.AggFuncSum, groupMark, colT, map[string]*graph.Attribute{})
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
				default:
					return fmt.Errorf("buildObliviousGroupAggregation: unsupported aggregation function %v", aggFunc)
				}
			default:
				return fmt.Errorf("buildObliviousGroupAggregation: unrecognized count func mode %v", aggFunc.Mode)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
		case ast.AggPercentileDisc:
			colT, err := t.buildExpression(aggFunc.Args[0], sortedChildColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			var keysForPercentileSort []*graph.Tensor
			keysForPercentileSort = append(keysForPercentileSort, sortedKeys...)
			keysForPercentileSort = append(keysForPercentileSort, colT)
			sortedCol, err := t.addSortNode("sort", keysForPercentileSort, []*graph.Tensor{colT}, false)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			attr := &graph.Attribute{}
			percent, err := strconv.ParseFloat(aggFunc.Args[1].String(), 64)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %s is not a valid float value", aggFunc.Args[1].String())
			}

			if percent < 0 || percent > 1 {
				return fmt.Errorf("buildPrivateGroupAggregation: percent should be in [0, 1], but got %v", percent)
			}

			attr.SetDouble(percent)
			output, err := t.addObliviousGroupAggNode(aggFunc.Name, groupMark, sortedCol[0], map[string]*graph.Attribute{"percent": attr})
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
		default:
			return fmt.Errorf("buildObliviousGroupAggregation: unsupported aggregation function %v", aggFunc)
		}
	}
	rt, err := extractResultTable(ln, colIdToTensor)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}

	// TODO(jingshi): temporary remove shuffle here for simplicity, make group_mark public and support aggregation with public group_mark later for efficiency
	if !t.CompileOpts.GetSecurityCompromise().GetRevealGroupMark() {
		// shuffle and replace 'rt' and 'groupMark'
		shuffled, err := t.addShuffleNode("shuffle", append(rt, groupMark))
		if err != nil {
			return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
		}
		rt, groupMark = shuffled[:len(rt)], shuffled[len(rt)]
	}

	// set ccl plain if enabled revealGroupMark or after groupMark shuffled
	for _, p := range t.enginesInfo.GetParties() {
		groupMark.CC.SetLevelForParty(p, ccl.Plain)
	}
	groupMarkPub, err := t.converter.convertTo(groupMark, &publicPlacement{partyCodes: t.enginesInfo.GetParties()})
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}
	// filter
	rtFiltered, err := t.ep.AddFilterNode("filter", rt, groupMarkPub, t.enginesInfo.GetParties())
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}
	return ln.SetResultTableWithDTypeCheck(rtFiltered)
}

func (t *translator) buildUnion(ln *UnionAllNode) (err error) {
	union, ok := ln.lp.(*core.LogicalUnionAll)
	if !ok {
		return fmt.Errorf("buildUnion: assert failed expected *core.LogicalUnionAll, get %T", ln.LP())
	}
	colIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()
	for i, c := range union.Schema().Columns {
		var ts []*graph.Tensor
		for _, child := range ln.Children() {
			ts = append(ts, child.ResultTable()[i])
		}
		ot, err := t.addConcatNode(ts)
		if err != nil {
			return fmt.Errorf("buildUnion: %v", err)
		}
		colIdToTensor[c.UniqueID] = ot
	}

	return nil
}

func (t *translator) buildLimit(ln *LimitNode) (err error) {
	limit, ok := ln.lp.(*core.LogicalLimit)
	if !ok {
		return fmt.Errorf("buildLimit: LimitNode contains invalid LogicalPlan type %T", ln.lp)
	}
	if len(ln.Children()) != 1 {
		return fmt.Errorf("buildLimit: unexpected number of children %v != 1", len(ln.children))
	}

	colIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	var shareTensors []*graph.Tensor
	var shareIds []int64
	privateTensors := make(map[string][]*graph.Tensor)
	privateIds := make(map[string][]int64)
	childColIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	for id, it := range sliceutil.SortedMap(childColIdToTensor) {
		if it.Status() == proto.TensorStatus_TENSORSTATUS_SECRET || it.Status() == proto.TensorStatus_TENSORSTATUS_PUBLIC {
			shareTensors = append(shareTensors, it)
			shareIds = append(shareIds, id)
		} else if it.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			privateTensors[it.OwnerPartyCode] = append(privateTensors[it.OwnerPartyCode], it)
			privateIds[it.OwnerPartyCode] = append(privateIds[it.OwnerPartyCode], id)
		} else {
			return fmt.Errorf("buildLimit: unsupported tensor status for %v", it)
		}
	}

	if len(shareTensors) != 0 {
		output, err := t.ep.AddLimitNode("limit", shareTensors, int(limit.Offset), int(limit.Count), t.enginesInfo.GetParties())
		if err != nil {
			return fmt.Errorf("buildLimit: %v", err)
		}
		for i, t := range output {
			colIdToTensor[shareIds[i]] = t
		}
	}

	for party, tensors := range sliceutil.SortedMap(privateTensors) {
		output, err := t.ep.AddLimitNode("limit", tensors, int(limit.Offset), int(limit.Count), []string{party})
		if err != nil {
			return fmt.Errorf("buildLimit: %v", err)
		}
		for i, t := range output {
			colIdToTensor[privateIds[party][i]] = t
		}
	}

	return nil
}

func (t *translator) buildSort(ln *SortNode) (err error) {
	sort, ok := ln.lp.(*core.LogicalSort)
	if !ok {
		return fmt.Errorf("buildSort: can not convert node from %T to LogicalSort", ln.lp)
	}

	colIdToTensor := map[int64]*graph.Tensor{}
	defer func() {
		if err != nil {
			return
		}

		err = setResultTable(ln, colIdToTensor)
	}()

	childIdToTensor := t.getNodeResultTensor(ln.children[0])
	var sortKeys []*graph.Tensor

	if len(sort.ByItems) == 0 {
		return fmt.Errorf("buildSort: order by items are required in sort node")
	}

	desc := sort.ByItems[0].Desc
	for _, col := range sort.ByItems {
		if desc != col.Desc {
			return fmt.Errorf("buildSort: sorting multiple columns in different order directions is not supported")
		}

		tensor, err := t.getTensorFromExpression(col.Expr, childIdToTensor)
		if err != nil {
			return fmt.Errorf("buildSort: failed to get tensor of col(%s): %v", col.Expr, err)
		}
		sortKeys = append(sortKeys, tensor)
		cc := tensor.CC
		for _, p := range t.enginesInfo.GetParties() {
			if !cc.IsVisibleFor(p) && cc.LevelFor(p) != ccl.Rank {
				return fmt.Errorf("buildSort: col(%v)'s order is not visible for %s", col.String(), p)
			}
		}
	}

	var payload []*graph.Tensor
	for _, col := range sort.Schema().Columns {
		tensor, err := t.getTensorFromColumn(col, childIdToTensor)
		if err != nil {
			return fmt.Errorf("buildSort: failed to get tensor of col(%s): %v", col.OrigName, err)
		}

		payload = append(payload, tensor)
	}

	output, err := t.addSortNode(operator.OpNameSort, sortKeys, payload, desc)

	if len(output) != len(payload) {
		return fmt.Errorf("buildSort: input tensors' size(%v) not equal to output tensors' size(%v)", len(payload), len(output))
	}

	for i, col := range sort.Schema().Columns {
		colIdToTensor[col.UniqueID] = output[i]
	}

	return nil
}

// simpleCount means Complete Count, except count(distinct), e.g: count(colA), count(*), count(1)...
func isSimpleCount(aggFunc *aggregation.AggFuncDesc) bool {
	return aggFunc.Name == ast.AggFuncCount && aggFunc.Mode == aggregation.CompleteMode && !aggFunc.HasDistinct
}

// currently HE only support agg: SUM
func isHeSuitable(aggFunc *aggregation.AggFuncDesc, cc *ccl.CCL) bool {
	switch aggFunc.Name {
	case ast.AggFuncSum:
		parties := cc.GetVisibleParties()
		return len(parties) > 0
	default:
		return false
	}
}

func (t *translator) buildWindow(ln *WindowNode) (err error) {
	window, ok := ln.lp.(*core.LogicalWindow)
	if !ok {
		return fmt.Errorf("buildWindow: failed to convert to logical window")
	}
	if len(window.WindowFuncDescs) != 1 {
		return fmt.Errorf("buildWindow: unsupported windowFuncDescs length 1(expected) != %v(actural)",
			len(window.WindowFuncDescs))
	}
	desc := window.WindowFuncDescs[0]
	isRankWindow := ccl.IsRankWindowFunc(desc.Name)

	if isRankWindow {
		return t.buildRankWindow(ln)
	} else {
		return t.buildAggWindow(ln)
	}
}

func (t *translator) buildPartitionId(window *core.LogicalWindow, partyCode string, childColIdToTensor map[int64]*graph.Tensor) (*graph.Tensor, *graph.Tensor, error) {
	partitionCol := []*graph.Tensor{}

	for _, item := range window.PartitionBy {
		tensor, err := t.getTensorFromColumn(item.Col, childColIdToTensor)
		if err != nil {
			return nil, nil, fmt.Errorf("buildPartitionId: %v", err)
		}

		partitionCol = append(partitionCol, tensor)
	}

	return t.addGroupNode("group", partitionCol, partyCode)
}

func (t *translator) extractOrderByBlock(window *core.LogicalWindow, colIdToTensor map[int64]*graph.Tensor, partyCode string) ([]*graph.Tensor, []string, error) {
	sortReverse := make([]string, len(window.OrderBy))
	sortKey := []*graph.Tensor{}
	for i, item := range window.OrderBy {
		if item.Desc {
			sortReverse[i] = "1"
		} else {
			sortReverse[i] = "0"
		}

		tensor, err := t.getTensorFromColumn(item.Col, colIdToTensor)
		if err != nil {
			return nil, nil, fmt.Errorf("buildPartitionId: %v", err)
		}

		output, err := t.converter.convertTo(tensor, &privatePlacement{partyCode: partyCode})
		if err != nil {
			return nil, nil, fmt.Errorf("addPartitionNode: %v", err)
		}

		sortKey = append(sortKey, output)
	}

	return sortKey, sortReverse, nil
}

func (t *translator) buildObliviousRankWindow(ln *WindowNode) error {
	window, ok := ln.lp.(*core.LogicalWindow)
	if !ok {
		return fmt.Errorf("buildObliviousRankWindow: assert failed expected *core.LogicalWindow, get %T", ln.LP())
	}
	child := ln.Children()[0]
	childColIdToTensor := t.getNodeResultTensor(child)
	sortKeys := []*graph.Tensor{}
	partitionKeys := []*graph.Tensor{}
	for _, col := range window.PartitionBy {
		partitionKey, err := t.getTensorFromColumn(col.Col, childColIdToTensor)
		if err != nil {
			return fmt.Errorf("buildObliviousRankWindow: %v", err)
		}
		partitionKeys = append(partitionKeys, partitionKey)
	}
	orderKeys := []*graph.Tensor{}
	for _, col := range window.OrderBy {
		orderKey, err := t.getTensorFromColumn(col.Col, childColIdToTensor)
		if err != nil {
			return fmt.Errorf("builObliviousRankWindow: %v", err)
		}
		orderKeys = append(orderKeys, orderKey)
	}
	var sortPayloads []*graph.Tensor
	sortPayloads = append(sortPayloads, partitionKeys...)
	for _, col := range window.Schema().Columns[0 : len(window.Schema().Columns)-1] {
		payload, err := t.getTensorFromColumn(col, childColIdToTensor)
		if err != nil {
			return fmt.Errorf("buildObliviousRankWindow: %v", err)
		}
		sortPayloads = append(sortPayloads, payload)
	}
	sortKeys = append(sortKeys, partitionKeys...)
	sortKeys = append(sortKeys, orderKeys...)

	out, err := t.addSortNode("sort", sortKeys, sortPayloads, false)
	if err != nil {
		return fmt.Errorf("builObliviousRankWindow: sort with partition ids err: %v", err)
	}

	partitionedKeys := out[0:len(partitionKeys)]
	sortedPayloadTensors := out[len(partitionKeys):]
	groupMark, err := t.addObliviousGroupMarkNode("partition_mark", partitionedKeys)
	if err != nil {
		return fmt.Errorf("builObliviousRankWindow: %v", err)
	}
	windowDesc := window.WindowFuncDescs[0]
	lastCol := window.Schema().Columns[len(window.Schema().Columns)-1]
	var output *graph.Tensor
	switch windowDesc.Name {
	case ast.WindowFuncRowNumber:
		output, err = t.addObliviousGroupAggNode(ast.AggFuncCount, groupMark, groupMark, map[string]*graph.Attribute{})
		if err != nil {
			return fmt.Errorf("builObliviousRankWindow: %v", err)
		}
		childColIdToTensor[lastCol.UniqueID] = output
	case ast.WindowFuncPercentRank:
		output, err = t.addObliviousGroupAggNode(ast.WindowFuncPercentRank, groupMark, groupMark, map[string]*graph.Attribute{})
		if err != nil {
			return fmt.Errorf("builObliviousRankWindow: %v", err)
		}
	default:
		return fmt.Errorf("buildObliviousRankWindow: unsupported window function %v", windowDesc.Name)
	}

	result := append(sortedPayloadTensors, output)
	return ln.SetResultTableWithDTypeCheck(result)
}

func (t *translator) buildPrivateRankWindow(ln *WindowNode, party string, colIdToTensor map[int64]*graph.Tensor) (err error) {
	defer func() {
		if err != nil {
			return
		}

		err = setResultTable(ln, colIdToTensor)
	}()

	window, ok := ln.lp.(*core.LogicalWindow)
	if !ok {
		return fmt.Errorf("buildPrivateRankWindow: failed to convert to logical window node")
	}

	partitionId, partitionNum, err := t.buildPartitionId(window, party, colIdToTensor)
	if err != nil {
		return fmt.Errorf("buildPrivateRankWindow: %v", err)
	}

	output := t.ep.AddTensor("Out")

	output.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	lastCol := window.Schema().Columns[len(window.Schema().Columns)-1]
	tensor, ok := colIdToTensor[lastCol.UniqueID]
	if !ok {
		cc := ccl.NewCCL()
		cc.SetLevelForParty(party, ccl.Plain)
		output.CC = cc
		output.OwnerPartyCode = party
	} else {
		output.CC = tensor.CC.Clone()
		output.OwnerPartyCode = party
	}

	// reverse is a string array, '1' for true, '0' for false, here convert to string array and concatenate into string.
	sortKey, reverse, err := t.extractOrderByBlock(window, colIdToTensor, party)

	reverseAttr := &graph.Attribute{}
	reverseAttr.SetStrings(reverse)
	colIdToTensor[lastCol.UniqueID] = output

	for _, desc := range window.WindowFuncDescs {
		switch desc.Name {
		case ast.WindowFuncRowNumber:
			output.DType = proto.PrimitiveDataType_INT64
			return t.addWindowNode(operator.OpNameRowNumber, sortKey, partitionId, partitionNum, output, reverseAttr, party)
		case ast.WindowFuncPercentRank:
			output.DType = proto.PrimitiveDataType_FLOAT64
			return t.addWindowNode(operator.OpNamePercentRank, sortKey, partitionId, partitionNum, output, reverseAttr, party)
		default:
			return fmt.Errorf("buildPrivateRankWindow: unsupported window function %v", desc.Name)
		}
	}

	return nil
}

func (t *translator) buildRankWindow(ln *WindowNode) (err error) {
	colIdToTensor := t.getNodeResultTensor(ln.Children()[0])

	// if there are parties can view columns in partition and order, run this query in private mode
	visibleParty, err := t.findVisibleWindowParty(ln)
	if err != nil {
		return fmt.Errorf("buildRankWindow: %v", err)
	}

	if visibleParty != "" {
		return t.buildPrivateRankWindow(ln, visibleParty, colIdToTensor)
	} else {
		return t.buildObliviousRankWindow(ln)
	}
}

func (t *translator) buildAggWindow(ln *WindowNode) (err error) {
	return fmt.Errorf("aggregation window function is not supported")
}

func (t *translator) buildMaxOneRow(ln *MaxOneRowNode) (err error) {
	colIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	return setResultTable(ln, colIdToTensor)
}
