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
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
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
	core.InnerJoin:      InnerJoin,
	core.LeftOuterJoin:  LeftOuterJoin,
	core.RightOuterJoin: RightOuterJoin,
}

func (t *translator) buildApply(ln *ApplyNode) (err error) {
	apply, ok := ln.lp.(*core.LogicalApply)
	if !ok {
		return fmt.Errorf("buildApply: ApplyNode contains invalid LogicalPlan type %T", ln.lp)
	}
	if len(ln.Children()) != 2 {
		return fmt.Errorf("buildApply: unexpected number of children %v != 2", len(ln.children))
	}
	if len(apply.OtherConditions)+len(apply.EqualConditions) != 1 || len(apply.RightConditions) != 0 || len(apply.LeftConditions) != 0 {
		return fmt.Errorf("buildApply: doesn't support condition other:%s, equal:%s,  right condition (%s), left condition (%s)",
			apply.OtherConditions, apply.EqualConditions, apply.RightConditions, apply.LeftConditions)
	}

	childIdToTensor := map[int64]*Tensor{}
	for _, child := range ln.Children() {
		for i, c := range child.Schema().Columns {
			childIdToTensor[c.UniqueID] = child.ResultTable()[i]
		}
	}

	colIdToTensor := map[int64]*Tensor{}
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

	filterT, err := t.buildExpression(sFunc, childIdToTensor, true, ln)
	if err != nil {
		return err
	}

	// get result party list of IN-op result tensor
	reverseFilter := func(filter *Tensor) (*Tensor, error) {
		partyList := t.enginesInfo.partyInfo.GetParties()
		if filter.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyList = []string{filter.OwnerPartyCode}
		}
		return t.ep.AddNotNode("not", filterT, partyList)
	}

	selectIn := func(reversed bool) (err error) {
		var filter *Tensor
		if reversed {
			filter, err = reverseFilter(filterT)
			if err != nil {
				return
			}
		} else {
			filter = filterT
		}
		cs := ln.Schema().Columns
		for _, c := range cs[:len(cs)-1] {
			colIdToTensor[c.UniqueID] = childIdToTensor[c.UniqueID]
		}
		colIdToTensor[cs[len(cs)-1].UniqueID] = filter
		return nil
	}

	whereIn := func(reversed bool) (err error) {
		var filter *Tensor
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

	// TODO(xiaoyuan) support cross join
	// resultIdToTensor, err := t.buildCrossJoin(ln.Children()[0], ln.Children()[1])
	// if err != nil {
	// 	return err
	// }
	// err = setResultTable(ln, resultIdToTensor)
	// return err
	return fmt.Errorf("cross join is unimplemented")
}

func (t *translator) buildEQJoin(ln *JoinNode) (err error) {
	join, ok := ln.lp.(*core.LogicalJoin)
	if !ok {
		return fmt.Errorf("assert failed while translator buildEQJoin, expected: core.LogicalJoin, actual: %T", ln.lp)
	}
	left, right := ln.Children()[0], ln.Children()[1]

	// step 1: create join index
	var leftIndexT, rightIndexT *Tensor
	var leftTs = []*Tensor{}
	var rightTs = []*Tensor{}
	var parties []string
	for i, equalCondition := range join.EqualConditions {
		cols, err := extractEQColumns(equalCondition)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftIndexCol, rightIndexCol := cols[0], cols[1]
		leftT, err := left.FindTensorByColumnId(leftIndexCol.UniqueID)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftTs = append(leftTs, leftT)
		rightT, err := right.FindTensorByColumnId(rightIndexCol.UniqueID)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		rightTs = append(rightTs, rightT)
		if leftT.Status != proto.TensorStatus_TENSORSTATUS_PRIVATE || rightT.Status != proto.TensorStatus_TENSORSTATUS_PRIVATE {
			return fmt.Errorf("buildEQJoin: failed to check tensor status = [%v, %v]", leftT.Status, rightT.Status)
		}
		rightParty := rightT.OwnerPartyCode
		leftParty := leftT.OwnerPartyCode
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

	leftIndexT, rightIndexT, err = t.ep.AddJoinNode("join", leftTs, rightTs, parties, JoinTypeLpToEp[join.JoinType])
	if err != nil {
		return fmt.Errorf("buildEQJoin: %v", err)
	}

	leftIndexT.cc, rightIndexT.cc = createCCLForIndexT(ln.childDataSourceParties)
	// step 2: apply join index
	// record tensor id and tensor pointer in result table
	resultIdToTensor := map[int64]*Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, resultIdToTensor)
	}()

	{
		leftTs := left.ResultTable()
		leftParty := leftIndexT.OwnerPartyCode
		leftFiltered, err := t.addFilterByIndexNode(leftIndexT, leftTs, leftParty)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		for i, c := range left.Schema().Columns {
			resultIdToTensor[c.UniqueID] = leftFiltered[i]
		}
	}
	{
		rightTs := right.ResultTable()
		rightParty := rightIndexT.OwnerPartyCode
		rightFiltered, err := t.addFilterByIndexNode(rightIndexT, rightTs, rightParty)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		for i, c := range right.Schema().Columns {
			resultIdToTensor[c.UniqueID] = rightFiltered[i]
		}
	}

	return
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

func extractResultTable(ln logicalNode, resultIdToTensor map[int64]*Tensor) ([]*Tensor, error) {
	rt := []*Tensor{}
	for _, c := range ln.Schema().Columns {
		tensor, ok := resultIdToTensor[c.UniqueID]
		if !ok {
			return nil, fmt.Errorf("extractResultTable: unable to find columnID %v", c.UniqueID)
		}
		if ln.CCL() != nil {
			tensor.cc = ln.CCL()[c.UniqueID]
		}
		rt = append(rt, tensor)
	}
	return rt, nil
}

func setResultTable(ln logicalNode, resultIdToTensor map[int64]*Tensor) error {
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
	colIdToTensor := map[int64]*Tensor{}
	childIdToTensor := map[int64]*Tensor{}
	// record tensor id and tensor pointer in result table
	resultIdToTensor := map[int64]*Tensor{}
	for i, c := range child.Schema().Columns {
		childIdToTensor[c.UniqueID] = child.ResultTable()[i]
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
	filters := []*Tensor{}
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
	shareTensors := []*Tensor{}
	var shareIds []int64
	// private tensors need record it's owner party
	privateTensorsMap := make(map[string][]*Tensor)
	privateIdsMap := make(map[string][]int64)
	for _, columnId := range sliceutil.SortMapKeyForDeterminism(colIdToTensor) {
		it := colIdToTensor[columnId]
		if len(filter.cc.GetVisibleParties()) == len(t.ep.partyInfo.GetParties()) {
			switch it.Status {
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
			if it.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE && filter.cc.IsVisibleFor(it.OwnerPartyCode) {
				privateTensorsMap[it.OwnerPartyCode] = append(privateTensorsMap[it.OwnerPartyCode], it)
				privateIdsMap[it.OwnerPartyCode] = append(privateIdsMap[it.OwnerPartyCode], columnId)
				continue
			}
			foundParty := false
			for _, party := range it.cc.GetVisibleParties() {
				if filter.cc.IsVisibleFor(party) {
					foundParty = true
					newT, err := t.ep.converter.convertTo(it, &privatePlacement{partyCode: party})
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
		publicFilter, err := t.ep.converter.convertTo(filter, &publicPlacement{partyCodes: t.enginesInfo.partyInfo.GetParties()})
		if err != nil {
			return fmt.Errorf("buildSelection: %v", err)
		}
		// handling share tensors here
		output, err := t.ep.AddFilterNode("apply_filter", shareTensors, publicFilter, t.enginesInfo.partyInfo.GetParties())
		if err != nil {
			return fmt.Errorf("buildSelection: %v", err)
		}
		for i, id := range shareIds {
			resultIdToTensor[id] = output[i]
		}
	}
	// handling private tensors here
	if len(privateTensorsMap) > 0 {
		for _, p := range sliceutil.SortMapKeyForDeterminism(privateTensorsMap) {
			ts := privateTensorsMap[p]
			newFilter, err := t.ep.converter.convertTo(filter, &privatePlacement{partyCode: p})
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
	colIdToTensor := map[int64]*Tensor{}
	for i, c := range child.Schema().Columns {
		colIdToTensor[c.UniqueID] = child.ResultTable()[i]
	}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	resultIdToTensor := map[int64]*Tensor{}
	for i, expr := range proj.Exprs {
		cid := ln.Schema().Columns[i].UniqueID
		tensor, err := t.getTensorFromExpression(expr, colIdToTensor)
		if err != nil {
			return fmt.Errorf("buildProjection: %v", err)
		}
		resultIdToTensor[cid] = tensor
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
	colIdToTensor := map[int64]*Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	// Aggregation Function
	childColIdToTensor := t.getNodeResultTensor(child)
	for i, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 {
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
			output, err := t.ep.AddReduceAggNode(aggFunc.Name, colT)
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
				var out *Tensor
				if aggFunc.HasDistinct {
					colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
					if err != nil {
						return fmt.Errorf("buildAggregation: %v", err)
					}
					if colT.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
						partyCode := colT.OwnerPartyCode
						colT, err = t.ep.AddUniqueNode("unique", colT, partyCode)
						if err != nil {
							return fmt.Errorf("buildAggregation: add unique node: %v", err)
						}
						out, err = t.ep.AddShapeNode("shape", colT, 0, partyCode)
						if err != nil {
							return fmt.Errorf("buildAggregation: count: %v", err)
						}
					} else {
						// 1. sort
						keyTensor := []*Tensor{colT}
						sortedDistinctCol, err := t.ep.AddSortNode("count.sort", keyTensor, keyTensor)
						if err != nil {
							return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
						}
						// 2. group mark
						groupMarkDistinct, err := t.ep.AddObliviousGroupMarkNode("group_mark", sortedDistinctCol)
						if err != nil {
							return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
						}
						// 3. sum(mark)
						out, err = t.ep.AddReduceAggNode(ast.AggFuncSum, groupMarkDistinct)
						if err != nil {
							return fmt.Errorf("buildAggregation: %v", err)
						}
					}
				} else {
					var partyCode string
					tensor := child.ResultTable()[0]
					switch tensor.Status {
					case proto.TensorStatus_TENSORSTATUS_PRIVATE:
						partyCode = tensor.OwnerPartyCode
					case proto.TensorStatus_TENSORSTATUS_SECRET, proto.TensorStatus_TENSORSTATUS_PUBLIC:
						partyCode = t.enginesInfo.partyInfo.GetParties()[0]
					default:
						return fmt.Errorf("buildAggregation: count func doesn't support tensor status %v", tensor.Status)
					}
					out, err = t.ep.AddShapeNode("shape", tensor, 0, partyCode)
					if err != nil {
						return fmt.Errorf("buildAggregation: count: %v", err)
					}
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = out
			case aggregation.FinalMode:
				colT, err := t.buildExpression(aggFunc.Args[0], childColIdToTensor, false, ln)
				if err != nil {
					return fmt.Errorf("buildAggregation: %v", err)
				}
				output, err := t.ep.AddReduceAggNode(ast.AggFuncSum, colT)
				if err != nil {
					return fmt.Errorf("buildAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
			default:
				return fmt.Errorf("buildAggregation: unrecognized count func mode %v", aggFunc.Mode)
			}
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
	for _, pc := range t.ep.partyInfo.GetParties() {
		partyCandidate[pc] = true
	}
	// filter partyCandidate with groupby keys
	for _, item := range agg.GroupByItems {
		cc, err := ccl.InferExpressionCCL(item, child.CCL())
		if err != nil {
			return "", fmt.Errorf("findPartyHandleAll: %v", err)
		}
		for _, pc := range t.ep.partyInfo.GetParties() {
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
			if len(aggFunc.Args) != 1 {
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

func (t *translator) getNodeResultTensor(ln logicalNode) map[int64]*Tensor {
	colIdToTensor := map[int64]*Tensor{}
	for i, tensor := range ln.ResultTable() {
		colIdToTensor[ln.Schema().Columns[i].UniqueID] = tensor
	}
	return colIdToTensor
}

func (t *translator) buildPrivateGroupAggregation(ln *AggregationNode, party string) (err error) {
	colIdToTensor := map[int64]*Tensor{}
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
		if len(aggFunc.Args) != 1 {
			return fmt.Errorf("buildPrivateGroupAggregation: unsupported aggregation function %v, expect len(aggFunc.Args)=1, but got %v", aggFunc, len(aggFunc.Args))
		}
		// deal with simple count, which no need to buildExpression
		if isSimpleCount(aggFunc) {
			outputs, err := t.ep.AddGroupAggNode(ast.AggFuncCount, operator.OpNameGroupCount, groupId, groupNum, []*Tensor{groupId}, party)
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
					outputs, err := t.ep.AddGroupAggNode("count_distinct", operator.OpNameGroupCountDistinct, groupId, groupNum, []*Tensor{colT}, party)
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
					outputs, err := t.ep.AddGroupAggNode(ast.AggFuncSum, operator.OpNameGroupSum, groupId, groupNum, []*Tensor{colT}, party)
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
			outputs, err := t.ep.AddGroupAggNode(aggFunc.Name, operator.GroupAggOp[aggFunc.Name], groupId, groupNum, []*Tensor{colT}, party)
			if err != nil {
				return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
			}
			colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
		case ast.AggFuncSum: // deal with agg which may run in HE
			if colT.cc.IsVisibleFor(party) {
				outputs, err := t.ep.AddGroupAggNode(ast.AggFuncSum, operator.OpNameGroupSum, groupId, groupNum, []*Tensor{colT}, party)
				if err != nil {
					return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = outputs[0]
			} else {
				// run in HE
				var colTParty string
				if colT.status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
					// If colT is Private Tensor, encrypt colT in colT.OwnerPartyCode to avoid colT's copying
					colTParty = colT.OwnerPartyCode
				} else {
					colTParty = colT.cc.GetVisibleParties()[0]
				}
				output, err := t.ep.AddGroupHeSumNode("he_sum", groupId, groupNum, colT, party, colTParty)
				if err != nil {
					return fmt.Errorf("buildPrivateGroupAggregation: %v", err)
				}
				colIdToTensor[ln.Schema().Columns[i].UniqueID] = output
			}

		default:
			return fmt.Errorf("buildPrivateGroupAggregation: unsupported aggregation function %v", aggFunc)
		}
	}

	return err
}

func (t *translator) buildGroupId(ln *AggregationNode, partyCode string) (*Tensor, *Tensor, error) {
	groupCol := []*Tensor{}
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
	return t.ep.AddGroupNode("group", groupCol, partyCode)
}

func (t *translator) MergeKeys(ln *AggregationNode, keyTs []*Tensor) (mergedKeys []*Tensor, err error) {
	// separate private keyTs to different parties while appending the shared keyTs directly to the result
	partyToKeys := make(map[string][]*Tensor)
	var partiesInOrder []string // added to avoid DAG uncertainty caused by map hash
	for _, key := range keyTs {
		if key.Status != proto.TensorStatus_TENSORSTATUS_PRIVATE ||
			key.OwnerPartyCode == "" {
			mergedKeys = append(mergedKeys, key)
			continue
		}
		_, ok := partyToKeys[key.OwnerPartyCode]
		if !ok {
			partyToKeys[key.OwnerPartyCode] = []*Tensor{key}
			partiesInOrder = append(partiesInOrder, key.OwnerPartyCode)
		} else {
			partyToKeys[key.OwnerPartyCode] = append(partyToKeys[key.OwnerPartyCode], key)
		}
	}

	// NOTE: we do not build groupId by sort, so the resulting groups maybe not in order
	for _, party := range partiesInOrder {
		if len(partyToKeys[party]) > 1 {
			groupId, _, err := t.ep.AddGroupNode("group", partyToKeys[party], party)
			if err != nil {
				return nil, fmt.Errorf("MergeKeys: %v", err)
			}
			mergedKeys = append(mergedKeys, groupId)
		} else {
			mergedKeys = append(mergedKeys, partyToKeys[party][0])
		}
	}

	return mergedKeys, nil
}

func (t *translator) buildObliviousGroupAggregation(ln *AggregationNode) (err error) {
	agg, ok := ln.lp.(*core.LogicalAggregation)
	if !ok {
		return fmt.Errorf("buildAggregation: assert failed expected *core.LogicalAggregation, get %T", ln.LP())
	}
	child := ln.Children()[0]

	// sort group by keys
	keyTs := []*Tensor{}
	childColIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	for _, key := range agg.GroupByItems {
		keyT, err := t.getTensorFromExpression(key, childColIdToTensor)
		if err != nil {
			return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
		}
		keyTs = append(keyTs, keyT)
	}

	sortKey, err := t.MergeKeys(ln, keyTs)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}

	var sortPayload []*Tensor
	sortPayload = append(sortPayload, sortKey...)
	// TODO(jingshi): remove duplicated payload
	sortPayload = append(sortPayload, child.ResultTable()...)
	out, err := t.ep.AddSortNode("sort", sortKey, sortPayload)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: sort with groupIds err: %v", err)
	}
	sortedKeys := out[0:len(sortKey)]
	sortedTensors := out[len(sortKey):]
	sortedChildColIdToTensor := map[int64]*Tensor{}
	for i, tensor := range sortedTensors {
		sortedChildColIdToTensor[child.Schema().Columns[i].UniqueID] = tensor
	}
	// create group mark
	groupMark, err := t.ep.AddObliviousGroupMarkNode("group_mark", sortedKeys)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}

	// add agg funcs
	colIdToTensor := map[int64]*Tensor{}
	for i, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 {
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
				return fmt.Errorf("buildAggregation: unsupported aggregation function %s with a string type argument", aggFunc.Name)
			}
			colT, err := t.buildExpression(aggFunc.Args[0], sortedChildColIdToTensor, false, ln)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			output, err := t.ep.AddObliviousGroupAggNode(aggFunc.Name, groupMark, colT)
			if err != nil {
				return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
			}
			output.skipDTypeCheck = true
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
			var output *Tensor
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
					var keyAndDistinct []*Tensor
					keyAndDistinct = append(keyAndDistinct, sortedKeys...)
					keyAndDistinct = append(keyAndDistinct, colT)

					// TODO(jingshi): using free column to avoid sort if possible
					sortedDistinctCol, err := t.ep.AddSortNode("sort", keyAndDistinct, []*Tensor{colT})
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
					// Use group by keys and distinctCol to create groupMarkFull,
					// which is equivalent to the result of groupMark logic or groupMarkDistinct
					groupMarkDistinct, err := t.ep.AddObliviousGroupMarkNode("group_mark", sortedDistinctCol)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
					groupMarkFull, err := t.addBinaryNode(operator.OpNameLogicalOr, operator.OpNameLogicalOr, groupMark, groupMarkDistinct)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}

					output, err = t.ep.AddObliviousGroupAggNode(ast.AggFuncSum, groupMark, groupMarkFull)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
				} else {
					output, err = t.ep.AddObliviousGroupAggNode(ast.AggFuncCount, groupMark, groupMark)
					if err != nil {
						return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
					}
				}
			case aggregation.FinalMode:
				switch x := aggFunc.Args[0].(type) {
				case *expression.Column:
					colT := sortedChildColIdToTensor[x.UniqueID]
					output, err = t.ep.AddObliviousGroupAggNode(ast.AggFuncSum, groupMark, colT)
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
		default:
			return fmt.Errorf("buildObliviousGroupAggregation: unsupported aggregation function %v", aggFunc)
		}
	}
	rt, err := extractResultTable(ln, colIdToTensor)
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}

	// TODO(jingshi): temporary remove shuffle here for simplicity, make group_mark public and support aggregation with public group_mark later for efficiency
	if !t.securityCompromise.RevealGroupMark {
		// shuffle and replace 'rt' and 'groupMark'
		shuffled, err := t.ep.AddShuffleNode("shuffle", append(rt, groupMark))
		if err != nil {
			return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
		}
		rt, groupMark = shuffled[:len(rt)], shuffled[len(rt)]
	}

	// set ccl plain if enabled revealGroupMark or after groupMark shuffled
	for _, p := range t.enginesInfo.partyInfo.GetParties() {
		groupMark.cc.SetLevelForParty(p, ccl.Plain)
	}
	groupMarkPub, err := t.ep.converter.convertTo(groupMark, &publicPlacement{partyCodes: t.enginesInfo.partyInfo.GetParties()})
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}
	// filter
	rtFiltered, err := t.ep.AddFilterNode("filter", rt, groupMarkPub, t.enginesInfo.partyInfo.GetParties())
	if err != nil {
		return fmt.Errorf("buildObliviousGroupAggregation: %v", err)
	}
	return ln.SetResultTableWithDTypeCheck(rtFiltered)
}

func (t *translator) buildUnion(ln *UnionAllNode) (err error) {
	union, ok := ln.lp.(*core.LogicalUnionAll)
	if !ok {
		return fmt.Errorf("buildAggregation: assert failed expected *core.LogicalUnionAll, get %T", ln.LP())
	}
	colIdToTensor := map[int64]*Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()
	for i, c := range union.Schema().Columns {
		var ts []*Tensor
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

	colIdToTensor := map[int64]*Tensor{}
	defer func() {
		if err != nil {
			return
		}
		err = setResultTable(ln, colIdToTensor)
	}()

	var shareTensors []*Tensor
	var shareIds []int64
	privateTensors := make(map[string][]*Tensor)
	privateIds := make(map[string][]int64)
	childColIdToTensor := t.getNodeResultTensor(ln.Children()[0])
	for _, id := range sliceutil.SortMapKeyForDeterminism(childColIdToTensor) {
		it := childColIdToTensor[id]
		if it.Status == proto.TensorStatus_TENSORSTATUS_SECRET || it.Status == proto.TensorStatus_TENSORSTATUS_PUBLIC {
			shareTensors = append(shareTensors, it)
			shareIds = append(shareIds, id)
		} else if it.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			privateTensors[it.OwnerPartyCode] = append(privateTensors[it.OwnerPartyCode], it)
			privateIds[it.OwnerPartyCode] = append(privateIds[it.OwnerPartyCode], id)
		} else {
			return fmt.Errorf("buildLimit: unsupported tensor status for %v", it)
		}
	}

	if len(shareTensors) != 0 {
		output, err := t.ep.AddLimitNode("limit", shareTensors, int(limit.Offset), int(limit.Count), t.ep.partyInfo.GetParties())
		if err != nil {
			return fmt.Errorf("buildLimit: %v", err)
		}
		for i, t := range output {
			colIdToTensor[shareIds[i]] = t
		}
	}

	for _, party := range sliceutil.SortMapKeyForDeterminism(privateTensors) {
		output, err := t.ep.AddLimitNode("limit", privateTensors[party], int(limit.Offset), int(limit.Count), []string{party})
		if err != nil {
			return fmt.Errorf("buildLimit: %v", err)
		}
		for i, t := range output {
			colIdToTensor[privateIds[party][i]] = t
		}
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
