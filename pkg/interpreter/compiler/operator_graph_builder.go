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
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

type SourcePartiesTracker struct {
	lpSourceParties map[core.LogicalPlan][]string
	enginesInfo     *graph.EnginesInfo
}

func NewSourcePartiesTracker(enginesInfo *graph.EnginesInfo) *SourcePartiesTracker {
	return &SourcePartiesTracker{
		lpSourceParties: make(map[core.LogicalPlan][]string),
		enginesInfo:     enginesInfo,
	}
}

func (spt *SourcePartiesTracker) GetSourceParties(lp core.LogicalPlan) ([]string, error) {
	if parties, ok := spt.lpSourceParties[lp]; ok {
		return parties, nil
	}

	if ds, ok := lp.(*core.DataSource); ok {
		dt, err := core.NewDbTableFromString(ds.DBName.O + "." + ds.TableInfo().Name.O)
		if err != nil {
			return nil, err
		}
		sourceParty := spt.enginesInfo.GetPartyByTable(dt)
		if sourceParty == "" {
			return nil, fmt.Errorf("fail to get party by table: %s", dt.String())
		}
		parties := []string{sourceParty}
		spt.lpSourceParties[lp] = parties
		return parties, nil
	}

	if len(lp.Children()) == 0 {
		return nil, fmt.Errorf("logical plan which is not Datasouce has no children")
	}
	parties := []string{}
	for _, child := range lp.Children() {
		childParties, err := spt.GetSourceParties(child)
		if err != nil {
			return nil, err
		}
		parties = append(parties, childParties...)
	}
	parties = sliceutil.SliceDeDup(parties)
	spt.lpSourceParties[lp] = parties
	return parties, nil
}

type OperatorGraphBuilder struct {
	operators            []Operator
	sourcePartiesTracker *SourcePartiesTracker
	enginesInfo          *graph.EnginesInfo
	tensorMetaManager    *TensorMetaManager
	issuerPartyCode      string
	createdAt            time.Time
	checkFuncs           bool
	producerTracker      *TensorProducerTracker
	consumerTracker      *TensorConsumerTracker
}

func NewOperatorGraphBuilder(tensorMetaManager *TensorMetaManager, enginesInfo *graph.EnginesInfo, issuerPartyCode string, createdAt time.Time) *OperatorGraphBuilder {
	return &OperatorGraphBuilder{
		operators:            []Operator{},
		sourcePartiesTracker: NewSourcePartiesTracker(enginesInfo),
		enginesInfo:          enginesInfo,
		tensorMetaManager:    tensorMetaManager,
		issuerPartyCode:      issuerPartyCode,
		createdAt:            createdAt,
		checkFuncs:           true,
		producerTracker:      &TensorProducerTracker{},
		consumerTracker:      &TensorConsumerTracker{},
	}
}

func (builder *OperatorGraphBuilder) GetInvolvedParties(lp core.LogicalPlan) ([]string, error) {
	involvedParties, err := builder.sourcePartiesTracker.GetSourceParties(lp)
	if err != nil {
		return nil, err
	}
	involvedParties = append(involvedParties, builder.issuerPartyCode)
	involvedParties = sliceutil.SliceDeDup(involvedParties)
	return involvedParties, nil
}

func (builder *OperatorGraphBuilder) Build(lp core.LogicalPlan) (*OperatorGraph, error) {
	if err := builder.buildInternal(lp, false); err != nil {
		return nil, err
	}
	if err := builder.buildResult(lp); err != nil {
		return nil, err
	}
	operatorGraph := &OperatorGraph{
		operators:       builder.operators,
		tensors:         builder.tensorMetaManager.Tensors(),
		producerTracker: builder.producerTracker,
		consumerTracker: builder.consumerTracker,
	}
	return operatorGraph, nil
}

func (builder *OperatorGraphBuilder) addOperator(operator Operator) {
	builder.operators = append(builder.operators, operator)
	// id start from 1
	operator.SetID(len(builder.operators))

	// set source node for all output tensors
	for _, output := range GetNodeOutputs(operator) {
		builder.producerTracker.SetProducer(output, operator)
	}

	// set user node for all input tensors
	for _, input := range GetNodeInputs(operator) {
		builder.consumerTracker.AddConsumer(input, operator)
	}
}

func (builder *OperatorGraphBuilder) buildInternal(lp core.LogicalPlan, alreadyInSingleParty bool) error {
	if !alreadyInSingleParty {
		sourceParties, err := builder.sourcePartiesTracker.GetSourceParties(lp)
		if err != nil {
			return err
		}
		if len(sourceParties) == 1 {
			return builder.buildSinglePartySQL(lp, sourceParties[0])
		}
	}

	for _, childLP := range lp.Children() {
		if err := builder.buildInternal(childLP, alreadyInSingleParty); err != nil {
			return err
		}
	}

	switch x := lp.(type) {
	case *core.DataSource:
		sourceParties, err := builder.sourcePartiesTracker.GetSourceParties(lp)
		if err != nil {
			return err
		}
		return builder.buildDataSource(x, sourceParties[0])
	case *core.LogicalAggregation:
		return builder.buildLogicalAggregation(x)
	case *core.LogicalApply:
		return builder.buildLogicalJoin(&x.LogicalJoin)
	case *core.LogicalJoin:
		return builder.buildLogicalJoin(x)
	case *core.LogicalLimit:
		return builder.buildLogicalLimit(x)
	case *core.LogicalProjection:
		return builder.buildLogicalProjection(x)
	case *core.LogicalSelection:
		return builder.buildLogicalSelection(x)
	case *core.LogicalSort:
		return builder.buildLogicalSort(x)
	case *core.LogicalUnionAll:
		return builder.buildLogicalUnionAll(x)
	case *core.LogicalWindow:
		return builder.buildLogicalWindow(x)
	case *core.LogicalMaxOneRow:
		return builder.buildMaxOneRow(x)
	default:
		return fmt.Errorf("build OperatorGraph: unsupported logical plan type: %T", lp)
	}
}

// ============================================================================
// Handle Logical Plan Nodes
// ============================================================================

func (builder *OperatorGraphBuilder) buildDataSource(lp core.LogicalPlan, sourceParty string) error {
	resultTable := ResultTable{}

	tensors := make([]*TensorMeta, 0, len(lp.Schema().Columns))
	originNames := make([]string, 0, len(lp.Schema().Columns))
	for _, column := range lp.Schema().Columns {
		tp, err := graph.ConvertDataType(column.RetType)
		if err != nil {
			return fmt.Errorf("buildDataSource: %v", err)
		}
		name := column.String()
		tensor := builder.tensorMetaManager.CreateTensorMeta(name, tp)
		resultTable[column.UniqueID] = tensor
		tensors = append(tensors, tensor)
		originNames = append(originNames, column.OrigName)
	}

	if err := builder.tensorMetaManager.setLPResultTable(lp, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildDataSource: %v", err)
	}

	operator := &OperatorDataSource{
		outputs:     tensors,
		sourceParty: sourceParty,
		originNames: originNames,
	}
	builder.addOperator(operator)
	return nil
}

func (builder *OperatorGraphBuilder) buildSemiJoin(join *core.LogicalJoin) error {
	if len(join.Children()) != 2 {
		return fmt.Errorf("buildSemiJoin: unsupported children number: %d", len(join.Children()))
	}

	if len(join.OtherConditions)+len(join.EqualConditions) != 1 || len(join.LeftConditions) != 0 || len(join.RightConditions) != 0 {
		return fmt.Errorf("buildSemiJoin: doesn't support condition other:%s, equal:%s,  right condition (%s), left condition (%s)",
			join.OtherConditions, join.EqualConditions, join.RightConditions, join.LeftConditions)
	}

	leftResultTable, err := builder.tensorMetaManager.getLPResultTable(join.Children()[0])
	if err != nil {
		return fmt.Errorf("buildSemiJoin: %v", err)
	}
	rightResultTable, err := builder.tensorMetaManager.getLPResultTable(join.Children()[1])
	if err != nil {
		return fmt.Errorf("buildSemiJoin: %v", err)
	}
	childrenReusltTable := sliceutil.MergeMaps(leftResultTable, rightResultTable)

	var sFunc *expression.ScalarFunction
	if len(join.OtherConditions) > 0 {
		var ok bool
		sFunc, ok = join.OtherConditions[0].(*expression.ScalarFunction)
		if !ok {
			return fmt.Errorf("buildSemiJoin: type assertion failed")
		}
	}
	if len(join.EqualConditions) > 0 {
		sFunc = join.EqualConditions[0]
	}
	if sFunc.FuncName.L != ast.EQ || len(sFunc.GetArgs()) != 2 {
		return fmt.Errorf("buildSemiJoin: invalid condition func: %v", sFunc)
	}

	leftKey, err := builder.getTensorFromExpression(sFunc.GetArgs()[0], childrenReusltTable, true)
	if err != nil {
		return fmt.Errorf("buildSemiJoin: %v", err)
	}
	rightKey, err := builder.getTensorFromExpression(sFunc.GetArgs()[1], childrenReusltTable, true)
	if err != nil {
		return fmt.Errorf("buildSemiJoin: %v", err)
	}

	inResult, err := builder.addInOp(leftKey, rightKey)
	if err != nil {
		return fmt.Errorf("buildSemiJoin: %v", err)
	}

	selectIn := func(reversed bool) error {
		var filter *TensorMeta
		if reversed {
			filter, err = builder.addFunctionNode(ast.UnaryNot, []*TensorMeta{inResult}, []*expression.Constant{})
			if err != nil {
				return fmt.Errorf("buildSemiJoin: %v", err)
			}
		} else {
			filter = inResult
		}

		resultTable := ResultTable{}
		for _, col := range join.Schema().Columns {
			resultTable[col.UniqueID] = childrenReusltTable[col.UniqueID]
		}
		resultTable[join.Schema().Columns[len(join.Schema().Columns)-1].UniqueID] = filter

		if err := builder.tensorMetaManager.setLPResultTable(join, resultTable, builder.checkFuncs); err != nil {
			return fmt.Errorf("buildSemiJoin: %v", err)
		}
		return nil
	}

	whereIn := func(reversed bool) error {
		var filter *TensorMeta
		if reversed {
			filter, err = builder.addFunctionNode(ast.UnaryNot, []*TensorMeta{inResult}, []*expression.Constant{})
			if err != nil {
				return fmt.Errorf("buildSemiJoin: %v", err)
			}
		} else {
			filter = inResult
		}
		inputs := make([]*TensorMeta, 0, len(join.Schema().Columns))
		for _, col := range join.Schema().Columns {
			input := childrenReusltTable[col.UniqueID]
			inputs = append(inputs, input)
		}
		outputs := builder.addFilterOp(filter, inputs)
		resultTable := ResultTable{}
		for idx, col := range join.Schema().Columns {
			resultTable[col.UniqueID] = outputs[idx]
		}

		if err := builder.tensorMetaManager.setLPResultTable(join, resultTable, builder.checkFuncs); err != nil {
			return fmt.Errorf("buildSemiJoin: %v", err)
		}
		return nil
	}

	switch join.JoinType {
	case core.AntiLeftOuterSemiJoin: // SELECT ta.id NOT IN (select tb.id from tb) as f from ta
		return selectIn(true)
	case core.LeftOuterSemiJoin: // SELECT ta.id IN (select tb.id from tb) as f from ta
		return selectIn(false)
	case core.AntiSemiJoin: // select ta.id, ta.x1 from ta WHERE ta.id NOT IN (select tb.id from tb)
		return whereIn(true)
	case core.SemiJoin: // select ta.id, ta.x1 from ta WHERE ta.id IN (select tb.id from tb)
		return whereIn(false)
	default:
		return fmt.Errorf("buildSemiJoin: invalid join type %s", join.JoinType)
	}
}

func (builder *OperatorGraphBuilder) buildLogicalJoin(join *core.LogicalJoin) error {
	if len(join.Children()) != 2 {
		return fmt.Errorf("buildLogicalJoin: unexpected children number: %d", len(join.Children()))
	}

	switch join.JoinType {
	case core.SemiJoin, core.AntiSemiJoin, core.AntiLeftOuterSemiJoin, core.LeftOuterSemiJoin:
		return builder.buildSemiJoin(join)
	}
	if !slices.Contains(SupportedJoinType, join.JoinType) {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("buildLogicalJoin: unsupported join type: %s", join.JoinType))
	}
	if len(join.OtherConditions) != 0 || len(join.LeftConditions) != 0 || len(join.RightConditions) != 0 {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("buildLogicalJoin doesn't support other condition (%+v), right condition (%+v), left condition (%+v)", join.OtherConditions, join.RightConditions, join.LeftConditions))
	}

	if len(join.EqualConditions) > 0 {
		return builder.buildEQJoin(join)
	}
	return builder.buildCrossJoin(join)
}

func (builder *OperatorGraphBuilder) buildLogicalAggregation(agg *core.LogicalAggregation) error {
	if len(agg.Children()) != 1 {
		return fmt.Errorf("buildLogicalAggregation: unexpected children number: %d", len(agg.Children()))
	}
	if len(agg.GroupByItems) > 0 {
		return builder.buildGroupAggregation(agg)
	}
	// No grouping - use reduce aggregation
	return builder.buildReduceAggregation(agg)
}

func (builder *OperatorGraphBuilder) buildLogicalLimit(limit *core.LogicalLimit) error {
	if len(limit.Children()) != 1 {
		return fmt.Errorf("buildLogicalLimit: unexpected children number: %d", len(limit.Children()))
	}

	resultTable := ResultTable{}

	childResultTable, err := builder.tensorMetaManager.getLPResultTable(limit.Children()[0])
	if err != nil {
		return fmt.Errorf("buildLogicalLimit: %v", err)
	}
	var inputs = make([]*TensorMeta, 0, len(childResultTable))
	var outputs = make([]*TensorMeta, 0, len(childResultTable))
	for colId, input := range sliceutil.SortedMap(childResultTable) {
		output := builder.tensorMetaManager.CreateTensorMetaAs(input)
		inputs = append(inputs, input)
		outputs = append(outputs, output)
		resultTable[colId] = output
	}

	if err := builder.tensorMetaManager.setLPResultTable(limit, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalLimit: %v", err)
	}

	operator := &OperatorLimit{
		inputs:  inputs,
		outputs: outputs,
		offset:  limit.Offset,
		count:   limit.Count,
	}
	builder.addOperator(operator)

	return nil
}

func (builder *OperatorGraphBuilder) buildLogicalProjection(projection *core.LogicalProjection) error {
	resultTable := ResultTable{}

	if len(projection.Children()) != 1 {
		return fmt.Errorf("buildLogicalProjection: unexpected children number: %d", len(projection.Children()))
	}
	child := projection.Children()[0]
	childResultTable, err := builder.tensorMetaManager.getLPResultTable(child)
	if err != nil {
		return fmt.Errorf("buildLogicalProjection: %v", err)
	}

	tensors := []*TensorMeta{}
	for _, expr := range projection.Exprs {
		tensor, err := builder.getTensorFromExpression(expr, childResultTable, false)
		if err != nil {
			return fmt.Errorf("buildLogicalProjection: %v", err)
		}
		tensors = append(tensors, tensor)
	}

	outputTs, err := builder.addBroadcastToOndemand(tensors)
	if err != nil {
		return fmt.Errorf("buildLogicalProjection: %v", err)
	}

	for idx := range projection.Exprs {
		colId := projection.Schema().Columns[idx].UniqueID
		resultTable[colId] = outputTs[idx]
	}

	if err := builder.tensorMetaManager.setLPResultTable(projection, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalProjection: %v", err)
	}
	return nil
}

func (builder *OperatorGraphBuilder) buildLogicalSelection(selection *core.LogicalSelection) error {
	if len(selection.Children()) != 1 {
		return fmt.Errorf("buildLogicalSelection: unexpected children number: %d", len(selection.Children()))
	}

	childResultTable, err := builder.tensorMetaManager.getLPResultTable(selection.Children()[0])
	if err != nil {
		return fmt.Errorf("buildLogicalSelection: %v", err)
	}

	// If there are no filter conditions, then the Selection acts as a no-op
	if len(selection.Conditions) == 0 {
		if err := builder.tensorMetaManager.setLPResultTable(selection, childResultTable, builder.checkFuncs); err != nil {
			return fmt.Errorf("buildLogicalSelection: %v", err)
		}
		return nil
	}

	resultTable := ResultTable{}

	filters := []*TensorMeta{}
	for _, cond := range selection.Conditions {
		filter, err := builder.getTensorFromExpression(cond, childResultTable, false)
		if err != nil {
			return fmt.Errorf("buildLogicalSelection: %v", err)
		}
		filters = append(filters, filter)
	}

	finalFilter := filters[0]
	for i := 1; i < len(filters); i++ {
		finalFilter, err = builder.addFunctionNode(ast.LogicAnd, []*TensorMeta{finalFilter, filters[i]}, []*expression.Constant{})
		if err != nil {
			return fmt.Errorf("buildLogicalSelection: %v", err)
		}
	}

	inputs := []*TensorMeta{}
	for _, col := range selection.Schema().Columns {
		input, err := childResultTable.getColumnTensorMeta(col)
		if err != nil {
			return fmt.Errorf("buildLogicalSelection: %v", err)
		}
		inputs = append(inputs, input)
	}
	outputs := builder.addFilterOp(finalFilter, inputs)
	for idx, col := range selection.Schema().Columns {
		resultTable[col.UniqueID] = outputs[idx]
	}

	if err := builder.tensorMetaManager.setLPResultTable(selection, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalSelection: %v", err)
	}

	return nil
}

func (builder *OperatorGraphBuilder) buildLogicalSort(sort *core.LogicalSort) error {
	if len(sort.Children()) != 1 {
		return fmt.Errorf("buildLogicalSort: unexpected children number: %d", len(sort.Children()))
	}
	if len(sort.ByItems) == 0 {
		return fmt.Errorf("buildLogicalSort: unexpected byItems number: %d", len(sort.ByItems))
	}

	resultTable := ResultTable{}
	childResultTable, err := builder.tensorMetaManager.getLPResultTable(sort.Children()[0])
	if err != nil {
		return fmt.Errorf("buildLogicalSort: %v", err)
	}

	sortKeys := make([]*TensorMeta, 0, len(sort.ByItems))
	var desc []bool
	for _, col := range sort.ByItems {
		desc = append(desc, col.Desc)

		tensor, err := builder.getTensorFromExpression(col.Expr, childResultTable, false)
		if err != nil {
			return fmt.Errorf("buildLogicalSort: %v", err)
		}
		sortKeys = append(sortKeys, tensor)
	}

	payloads, outputs, err := builder.processPayloads(resultTable, sort.Children()[0], sort.Schema().Columns)
	if err != nil {
		return fmt.Errorf("buildLogicalSort: %v", err)
	}

	if err := builder.tensorMetaManager.setLPResultTable(sort, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalSort: %v", err)
	}

	operator := &OperatorSort{
		sortKeys:   sortKeys,
		payloads:   payloads,
		outputs:    outputs,
		descending: desc,
	}
	builder.addOperator(operator)
	return nil
}

func (builder *OperatorGraphBuilder) buildLogicalUnionAll(union *core.LogicalUnionAll) error {
	resultTable := ResultTable{}
	for idx, col := range union.Schema().Columns {
		tensors := make([]*TensorMeta, 0, len(union.Children()))
		for _, child := range union.Children() {
			childResults, err := builder.tensorMetaManager.getLPSchemaTensors(child)
			if err != nil {
				return fmt.Errorf("buildLogicalUnionAll: %v", err)
			}
			tensors = append(tensors, childResults[idx])
		}
		out, err := builder.addConcatOp(tensors)
		if err != nil {
			return fmt.Errorf("buildLogicalUnionAll: %v", err)
		}
		resultTable[col.UniqueID] = out
	}

	if err := builder.tensorMetaManager.setLPResultTable(union, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalUnionAll: %v", err)
	}
	return nil
}

func (builder *OperatorGraphBuilder) buildLogicalWindow(window *core.LogicalWindow) error {
	if len(window.Children()) != 1 {
		return fmt.Errorf("buildLogicalWindow: unsupported children number: %d", len(window.Children()))
	}
	if len(window.WindowFuncDescs) != 1 {
		return fmt.Errorf("buildLogicalWindow: unsupported window function number: %d", len(window.WindowFuncDescs))
	}

	childResultTable, err := builder.tensorMetaManager.getLPResultTable(window.Children()[0])
	if err != nil {
		return fmt.Errorf("buildLogicalWindow: %v", err)
	}
	resultTable := ResultTable{}

	partitionKeys := make([]*TensorMeta, 0, len(window.PartitionBy))
	for _, item := range window.PartitionBy {
		partitionKey, err := childResultTable.getColumnTensorMeta(item.Col)
		if err != nil {
			return fmt.Errorf("buildLogicalWindow: %v", err)
		}
		partitionKeys = append(partitionKeys, partitionKey)
	}

	orderKeys := make([]*TensorMeta, 0, len(window.OrderBy))
	orderDescs := make([]string, 0, len(window.OrderBy))
	for _, item := range window.OrderBy {
		orderKey, err := childResultTable.getColumnTensorMeta(item.Col)
		if err != nil {
			return fmt.Errorf("buildLogicalWindow: %v", err)
		}
		orderKeys = append(orderKeys, orderKey)

		desc := "0"
		if item.Desc {
			desc = "1"
		}
		orderDescs = append(orderDescs, desc)
	}

	payloads, payloadOutputs, err := builder.processPayloads(resultTable, window.Children()[0], window.Schema().Columns[:len(window.Schema().Columns)-1])
	if err != nil {
		return fmt.Errorf("buildLogicalWindow: %v", err)
	}

	desc := window.WindowFuncDescs[0]
	var outputType *graph.DataType
	switch desc.Name {
	case ast.WindowFuncRowNumber:
		outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
	case ast.WindowFuncPercentRank:
		outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
	case ast.WindowFuncRank:
		outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
	default:
		return fmt.Errorf("buildLogicalWindow: unsupported window function: %s", desc.Name)
	}
	outputName := desc.Name

	funcOutput := builder.tensorMetaManager.CreateTensorMeta(outputName, outputType)
	resultTable[window.Schema().Columns[len(window.Schema().Columns)-1].UniqueID] = funcOutput

	if err := builder.tensorMetaManager.setLPResultTable(window, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalWindow: %v", err)
	}

	operator := &OperatorWindow{
		partitionKeys:  partitionKeys,
		orderKeys:      orderKeys,
		payloads:       payloads,
		payloadOutputs: payloadOutputs,
		funcOutput:     funcOutput,
		descs:          orderDescs,
		funcName:       desc.Name,
	}
	builder.addOperator(operator)
	return nil
}

func (builder *OperatorGraphBuilder) buildResult(lp core.LogicalPlan) error {
	resultTs, err := builder.tensorMetaManager.getLPSchemaTensors(lp)
	if err != nil {
		return err
	}
	var outputNames []string
	for idx, column := range lp.Schema().Columns {
		outputName := lp.OutputNames()[idx].ColName.String()
		if outputName == "" {
			outputName = column.String()
		}
		outputNames = append(outputNames, outputName)
	}

	if lp.IntoOpt() != nil && len(lp.IntoOpt().Opt.PartyFiles) == 1 && lp.IntoOpt().Opt.PartyFiles[0].PartyCode == "" {
		lp.IntoOpt().Opt.PartyFiles[0].PartyCode = builder.issuerPartyCode
	}

	operator := &OperatorResult{
		resultTensors:   resultTs,
		outputNames:     outputNames,
		issuerPartyCode: builder.issuerPartyCode,
		intoOpt:         lp.IntoOpt(),
		insertTableOpt:  lp.InsertTableOpt(),
	}
	if operator.intoOpt != nil {
		operator.resultTable, err = builder.tensorMetaManager.getLPResultTable(lp)
		if err != nil {
			return fmt.Errorf("buildResult: %v", err)
		}
	}
	builder.addOperator(operator)
	return nil
}

// ============================================================================
// Handle tensor alignment
// ============================================================================

func (builder *OperatorGraphBuilder) addBroadcastToOndemand(inputTs []*TensorMeta) ([]*TensorMeta, error) {
	if len(inputTs) == 1 {
		return inputTs, nil
	}
	outputTs := make([]*TensorMeta, len(inputTs))

	var shapeT *TensorMeta
	var constScalars []*TensorMeta
	for _, input := range inputTs {
		if input.IsConstScalar {
			constScalars = append(constScalars, input)
			continue
		}
		shapeT = input
	}
	if len(constScalars) == 0 {
		return inputTs, nil
	}
	if shapeT == nil {
		return nil, fmt.Errorf("addBroadcastToOndemand: no shape tensor found")
	}

	boradcastedTs, err := builder.addBroadCastToOp(constScalars, shapeT)
	if err != nil {
		return nil, fmt.Errorf("addBroadcastToOndemand: %v", err)
	}

	broadcastedIdx := 0
	for outputIdx, input := range inputTs {
		if input.IsConstScalar {
			outputTs[outputIdx] = boradcastedTs[broadcastedIdx]
			broadcastedIdx++
		} else {
			outputTs[outputIdx] = input
		}
	}
	return outputTs, nil
}

func (builder *OperatorGraphBuilder) addBroadCastToOp(constScalars []*TensorMeta, shapeT *TensorMeta) ([]*TensorMeta, error) {
	outs := make([]*TensorMeta, 0, len(constScalars))
	for _, scalarIn := range constScalars {
		out := builder.tensorMetaManager.CreateTensorMetaAs(scalarIn)
		out.IsConstScalar = false
		outs = append(outs, out)
	}

	operator := &OperatorBroadcastTo{
		shapeRef: shapeT,
		scalars:  constScalars,
		outputs:  outs,
	}
	builder.addOperator(operator)

	return outs, nil
}

// ============================================================================
// Helpers to handle Logical Plan Nodes
// ============================================================================

// runSQLString create sql string from lp with dialect
func (builder *OperatorGraphBuilder) runSQLString(lp core.LogicalPlan) (sql string, newTableRefs []string, err error) {
	needRewrite := false
	for _, party := range builder.enginesInfo.GetParties() {
		if len(builder.enginesInfo.GetTablesByParty(party)) > 0 {
			needRewrite = true
		}
	}
	var m map[core.DbTable]core.DbTable
	if needRewrite {
		m = builder.enginesInfo.GetDbTableMap()
	}

	return core.RewriteSQLFromLP(lp, m, needRewrite)
}

func (builder *OperatorGraphBuilder) buildSinglePartySQL(lp core.LogicalPlan, sourceParty string) error {
	// sql and newTableRefs are used for building runSQL op in SCQL engine
	sql, newTableRefs, err := builder.runSQLString(lp)
	if err != nil {
		return fmt.Errorf("buildSinglePartySQL: failed to rewrite sql=\"%s\", err: %w", sql, err)
	}

	subGraphBuilder := NewOperatorGraphBuilder(builder.tensorMetaManager, builder.enginesInfo, builder.issuerPartyCode, builder.createdAt)
	// SCQL currently supports limited functions.
	// Some are unsupported by the SCQL Engine and not listed in the compiler's constant.go.
	// Enabling function checks leads to errors.
	// However, these functions can run in single-party setups, so we skip function checks when generating a single-party OperatorGraph.
	subGraphBuilder.checkFuncs = false

	outputs := make([]*TensorMeta, 0, len(lp.Schema().Columns))
	var subGraphNodes []Operator
	var subGraphTracker *TensorConsumerTracker
	if err := subGraphBuilder.buildInternal(lp, true); err != nil {
		logrus.Warnf("buildSinglePartySQL: failed to build the sub plan for party %s, err: %v", sourceParty, err)
		// Build single party SQL failed
		// Just follow the logical plan to get RunSQL output
		// TODO: clean sub-graph tensors in TensorMetaManager
		resultTable := ResultTable{}
		for _, column := range lp.Schema().Columns {
			tp, err := graph.ConvertDataType(column.RetType)
			if err != nil {
				return fmt.Errorf("buildSinglePartySQL: %v", err)
			}
			// Can not infer name from datasource due to sub-plan compilation fail.
			name := column.String()
			tensor := builder.tensorMetaManager.CreateTensorMeta(name, tp)
			outputs = append(outputs, tensor)
			resultTable[column.UniqueID] = tensor
		}
		subGraphNodes = nil
		subGraphTracker = nil

		if err := builder.tensorMetaManager.setLPResultTable(lp, resultTable, false); err != nil {
			return fmt.Errorf("buildSinglePartySQL: %v", err)
		}
	} else {
		resultTable, err := subGraphBuilder.tensorMetaManager.getLPResultTable(lp)
		if err != nil {
			return fmt.Errorf("buildSinglePartySQL: failed to get result table for party %s, err: %w", sourceParty, err)
		}
		for _, column := range lp.Schema().Columns {
			tensor, ok := resultTable[column.UniqueID]
			if !ok {
				return fmt.Errorf("buildSinglePartySQL: failed to get tensor for column %s", column.String())
			}
			tp, err := graph.ConvertDataType(column.RetType)
			if err != nil {
				return fmt.Errorf("buildSinglePartySQL: %v", err)
			}
			// tensor.Name = column.String() // This is the way to get name in old translator. For now, we infer name from datasource.
			tensor.DType = tp
			outputs = append(outputs, tensor)
		}
		subGraphNodes = subGraphBuilder.operators
		subGraphTracker = subGraphBuilder.consumerTracker
	}

	operator := &OperatorRunSQL{
		outputs:         outputs,
		sql:             sql,
		tableRefs:       newTableRefs,
		sourceParty:     sourceParty,
		subGraphNodes:   subGraphNodes,
		subGraphTracker: subGraphTracker,
	}
	builder.addOperator(operator)
	return nil
}

func (builder *OperatorGraphBuilder) buildEQJoin(join *core.LogicalJoin) error {
	resultTable := ResultTable{}
	left, right := join.Children()[0], join.Children()[1]
	var leftKeyTs = []*TensorMeta{}
	var rightKeyTs = []*TensorMeta{}

	for _, equalCondition := range join.EqualConditions {
		cols, err := extractEQColumns(equalCondition)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftKeyCol, rightKeyCol := cols[0], cols[1]
		leftKeyT, err := builder.tensorMetaManager.getLPColumnTensor(left, leftKeyCol)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		leftKeyTs = append(leftKeyTs, leftKeyT)
		rightKeyT, err := builder.tensorMetaManager.getLPColumnTensor(right, rightKeyCol)
		if err != nil {
			return fmt.Errorf("buildEQJoin: %v", err)
		}
		rightKeyTs = append(rightKeyTs, rightKeyT)
	}

	// TODO: support empty payload in SCQL engine

	leftUsed := core.GetUsedList(join.Schema().Columns, left.Schema())
	leftUsedCols := sliceutil.Take(left.Schema().Columns, leftUsed)
	leftPayloads, leftOutputs, err := builder.processPayloads(resultTable, left, leftUsedCols)
	if err != nil {
		return fmt.Errorf("buildEQJoin: %v", err)
	}

	rightUsed := core.GetUsedList(join.Schema().Columns, right.Schema())
	rightUsedCols := sliceutil.Take(right.Schema().Columns, rightUsed)
	rightPayloads, rightOutputs, err := builder.processPayloads(resultTable, right, rightUsedCols)
	if err != nil {
		return fmt.Errorf("buildEQJoin: %v", err)
	}

	if err := builder.tensorMetaManager.setLPResultTable(join, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildEQJoin: %v", err)
	}

	operator := &OperatorEQJoin{
		leftKeys:      leftKeyTs,
		rightKeys:     rightKeyTs,
		leftPayloads:  leftPayloads,
		rightPayloads: rightPayloads,
		leftOutputs:   leftOutputs,
		rightOutputs:  rightOutputs,
		joinType:      join.JoinType,
	}
	builder.addOperator(operator)
	return nil
}

// LogicalMaxOneRow checks if a query returns no more than one row.
func (builder *OperatorGraphBuilder) buildMaxOneRow(plan *core.LogicalMaxOneRow) error {
	if len(plan.Children()) != 1 {
		return fmt.Errorf("buildMaxOneRow: must contain one children but got %d", len(plan.Children()))
	}
	// It is a no-op in the operator graph. Just pass through the result table from its child.
	resultTable, err := builder.tensorMetaManager.getLPResultTable(plan.Children()[0])
	if err != nil {
		return fmt.Errorf("buildMaxOneRow: %v", err)
	}
	if err := builder.tensorMetaManager.setLPResultTable(plan, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildMaxOneRow: %v", err)
	}
	return nil
}

func (builder *OperatorGraphBuilder) buildCrossJoin(join *core.LogicalJoin) error {
	resultTable := ResultTable{}

	lhs := join.Children()[0]
	rhs := join.Children()[1]

	leftOutputs := make([]*TensorMeta, 0, len(lhs.Schema().Columns))
	leftChildResult, err := builder.tensorMetaManager.getLPSchemaTensors(lhs)
	if err != nil {
		return fmt.Errorf("buildCrossJoin: %v", err)
	}
	for idx, tensor := range leftChildResult {
		output := builder.tensorMetaManager.CreateTensorMetaAs(tensor)
		leftOutputs = append(leftOutputs, output)
		resultTable[lhs.Schema().Columns[idx].UniqueID] = output
	}

	rightOutputs := make([]*TensorMeta, 0, len(rhs.Schema().Columns))
	rightChildResult, err := builder.tensorMetaManager.getLPSchemaTensors(rhs)
	if err != nil {
		return fmt.Errorf("buildCrossJoin: %v", err)
	}
	for idx, tensor := range rightChildResult {
		output := builder.tensorMetaManager.CreateTensorMetaAs(tensor)
		rightOutputs = append(rightOutputs, output)
		resultTable[rhs.Schema().Columns[idx].UniqueID] = output
	}

	if err := builder.tensorMetaManager.setLPResultTable(join, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildLogicalJoin: %v", err)
	}

	operator := &OperatorCrossJoin{
		leftInputs:   leftChildResult,
		rightInputs:  rightChildResult,
		leftOutputs:  leftOutputs,
		rightOutputs: rightOutputs,
	}
	builder.addOperator(operator)

	return nil
}

func (builder *OperatorGraphBuilder) buildReduceAggregation(agg *core.LogicalAggregation) error {
	childResultTable, err := builder.tensorMetaManager.getLPResultTable(agg.Children()[0])
	if err != nil {
		return fmt.Errorf("buildReduceAggregation: %v", err)
	}
	resultTable := ResultTable{}

	for idx, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
			return fmt.Errorf("buildReduceAggregation: invalid parameter count for %v", aggFunc)
		}
		if len(aggFunc.Args) != 2 && aggFunc.Name == ast.AggPercentileDisc {
			return fmt.Errorf("buildReduceAggregation: invalid parameter count for %v", aggFunc)
		}
		var argTensor *TensorMeta
		_, constArg := aggFunc.Args[0].(*expression.Constant)
		if aggFunc.Name == ast.AggFuncCount && !aggFunc.HasDistinct && constArg {
			childResults, err := builder.tensorMetaManager.getLPSchemaTensors(agg.Children()[0])
			if err != nil {
				return fmt.Errorf("buildReduceAggregation: %v", err)
			}
			if len(childResults) == 0 {
				return fmt.Errorf("buildReduceAggregation: count(*) with empty child result")
			}
			// when count(*), we use child node's result to get shape
			// here we select the 0th result arbitrarily，since the visibility does not have much impact on performance
			// FIXME？ maybe we should introduce OperatorShape instead of handling special case in OperatorReduce
			argTensor = childResults[0]
		} else {
			argTensor, err = builder.getTensorFromExpression(aggFunc.Args[0], childResultTable, false)
		}
		if err != nil {
			return fmt.Errorf("buildReduceAggregation: %v", err)
		}

		if aggFunc.Name != ast.AggFuncCount {
			if aggFunc.Args[0].GetType().Tp == mysql.TypeString {
				return fmt.Errorf("buildReduceAggregation: unsupported aggregation function %v with a string type argument", aggFunc.Name)
			}
		}

		output, err := builder.addReduceAggOp(aggFunc, argTensor)
		if err != nil {
			return fmt.Errorf("buildReduceAggregation: %v", err)
		}
		resultTable[agg.Schema().Columns[idx].UniqueID] = output
	}

	if err := builder.tensorMetaManager.setLPResultTable(agg, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildReduceAggregation: %v", err)
	}
	return nil
}

func (builder *OperatorGraphBuilder) buildGroupAggregation(agg *core.LogicalAggregation) error {
	childResultTable, err := builder.tensorMetaManager.getLPResultTable(agg.Children()[0])
	if err != nil {
		return fmt.Errorf("buildGroupAggregation: %v", err)
	}
	resultTable := ResultTable{}

	// build group keys
	groupKeys := make([]*TensorMeta, 0, len(agg.GroupByItems))
	for _, key := range agg.GroupByItems {
		tensor, err := builder.getTensorFromExpression(key, childResultTable, false)
		if err != nil {
			return fmt.Errorf("buildGroupAggregation: %v", err)
		}
		groupKeys = append(groupKeys, tensor)
	}

	aggArgs := make([]*TensorMeta, 0, len(agg.AggFuncs))
	simpleCountOutputs := make([]*TensorMeta, 0, len(agg.AggFuncs))
	argFuncOutputs := make([]*TensorMeta, 0, len(agg.AggFuncs))
	aggFuncsWithArg := make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs))

	for idx, aggFunc := range agg.AggFuncs {
		if len(aggFunc.Args) != 1 && aggFunc.Name != ast.AggPercentileDisc {
			return fmt.Errorf("buildGroupAggregation: invalid parameter count for %v", aggFunc)
		}
		if len(aggFunc.Args) != 2 && aggFunc.Name == ast.AggPercentileDisc {
			return fmt.Errorf("buildGroupAggregation: invalid parameter count for %v", aggFunc)
		}

		if aggFunc.Name != ast.AggFuncCount && aggFunc.Name != ast.AggFuncFirstRow && aggFunc.Args[0].GetType().Tp == mysql.TypeString {
			return fmt.Errorf("buildGroupAggregation: unsupported aggregation function %v with a string type argument", aggFunc.Name)
		}

		// deal with simple count, which no need to build expression for agg func argument
		var payload *TensorMeta
		if isSimpleCount(aggFunc) {
			output := builder.tensorMetaManager.CreateTensorMeta("simple_count_result", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
			simpleCountOutputs = append(simpleCountOutputs, output)
			resultTable[agg.Schema().Columns[idx].UniqueID] = output
		} else {
			if aggFunc.Name == ast.AggFuncCount && aggFunc.Mode == aggregation.FinalMode {
				if _, ok := aggFunc.Args[0].(*expression.Column); !ok {
					return fmt.Errorf("buildGroupAggregation: unsupported count final type %v", aggFunc)
				}
			}
			payload, err = builder.getTensorFromExpression(aggFunc.Args[0], childResultTable, false)
			if err != nil {
				return fmt.Errorf("buildGroupAggregation: %v", err)
			}

			aggArgs = append(aggArgs, payload)
			output := builder.tensorMetaManager.CreateTensorMetaAs(payload)
			output.DType = inferAggOutputType(aggFunc, payload.DType)
			output.Name = fmt.Sprintf("%v_%v", output.Name, aggFunc.Name)
			argFuncOutputs = append(argFuncOutputs, output)
			resultTable[agg.Schema().Columns[idx].UniqueID] = output

			aggFuncsWithArg = append(aggFuncsWithArg, aggFunc)
		}
	}

	if err := builder.tensorMetaManager.setLPResultTable(agg, resultTable, builder.checkFuncs); err != nil {
		return fmt.Errorf("buildGroupAggregation: %v", err)
	}

	operator := &OperatorGroupAgg{
		groupKeys:          groupKeys,
		aggArgs:            aggArgs,
		argFuncOutputs:     argFuncOutputs,
		simpleCountOutputs: simpleCountOutputs,
		aggFuncsWithArg:    aggFuncsWithArg,
	}
	builder.addOperator(operator)
	return nil
}

func (builder *OperatorGraphBuilder) addFilterOp(mask *TensorMeta, inputs []*TensorMeta) []*TensorMeta {
	outputs := make([]*TensorMeta, 0, len(inputs))
	for _, input := range inputs {
		output := builder.tensorMetaManager.CreateTensorMetaAs(input)
		outputs = append(outputs, output)
	}

	operator := &OperatorFilter{
		mask:    mask,
		inputs:  inputs,
		outputs: outputs,
	}
	builder.addOperator(operator)

	return outputs
}

func (builder *OperatorGraphBuilder) addConcatOp(inputs []*TensorMeta) (*TensorMeta, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("addConcatOp: unexpected inputs number: %d", len(inputs))
	}

	output := builder.tensorMetaManager.CreateTensorMetaAs(inputs[0])
	for _, input := range inputs[1:] {
		if !input.DType.Equal(output.DType) {
			return nil, fmt.Errorf("addConcatOp: inputs have different types: %s, %s", input.DType, output.DType)
		}
	}

	operator := &OperatorConcat{
		inputs: inputs,
		output: output,
	}
	builder.addOperator(operator)
	return output, nil
}

func (builder *OperatorGraphBuilder) addReduceAggOp(aggFunc *aggregation.AggFuncDesc, input *TensorMeta) (*TensorMeta, error) {
	output := builder.tensorMetaManager.CreateTensorMetaAs(input)
	output.DType = inferAggOutputType(aggFunc, input.DType)
	output.Name = fmt.Sprintf("%v_%v", output.Name, aggFunc.Name)

	operator := &OperatorReduce{
		input:   input,
		output:  output,
		aggFunc: aggFunc,
	}
	builder.addOperator(operator)
	return output, nil
}

func (builder *OperatorGraphBuilder) addInOp(left, right *TensorMeta) (*TensorMeta, error) {
	if !left.DType.Equal(right.DType) {
		return nil, fmt.Errorf("addInOp: type mismatch: %s, %s", left.DType, right.DType)
	}

	output := builder.tensorMetaManager.CreateTensorMeta("in_result", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL))

	operator := &OperatorIn{
		left:   left,
		right:  right,
		output: output,
	}
	builder.addOperator(operator)

	return output, nil
}

func (builder *OperatorGraphBuilder) processPayloads(resultTable ResultTable, lp core.LogicalPlan, payloadCols []*expression.Column) ([]*TensorMeta, []*TensorMeta, error) {
	payloads := make([]*TensorMeta, 0, len(payloadCols))
	outputs := make([]*TensorMeta, 0, len(payloadCols))
	for _, col := range payloadCols {
		payload, err := builder.tensorMetaManager.getLPColumnTensor(lp, col)
		if err != nil {
			return nil, nil, fmt.Errorf("processPayloads: %v", err)
		}
		payloads = append(payloads, payload)

		output := builder.tensorMetaManager.CreateTensorMetaAs(payload)
		outputs = append(outputs, output)
		resultTable[col.UniqueID] = output
	}
	return payloads, outputs, nil
}

// ============================================================================
// Handle Expression
// ============================================================================

func (builder *OperatorGraphBuilder) getTensorFromExpression(expr expression.Expression, tensorTable ResultTable, isApply bool) (*TensorMeta, error) {
	switch x := expr.(type) {
	case *expression.Column:
		return tensorTable.getColumnTensorMeta(x)
	case *expression.ScalarFunction:
		return builder.buildExprScalarFunction(x, tensorTable, isApply)
	case *expression.Constant:
		return builder.buildExprConstant(x)
	default:
		return nil, fmt.Errorf("getTensorFromExpression: unsupported expression type: %T", x)
	}
}

func checkFuncInputsType(funcName string, inputs []*TensorMeta) error {
	switch funcName {
	case ast.UnaryNot, ast.LogicOr, ast.LogicAnd:
		for idx, input := range inputs {
			if input.DType.DType != proto.PrimitiveDataType_BOOL {
				return fmt.Errorf("function %s expect bool input, but got %s for input_%d", funcName, input.DType.String(), idx)
			}
		}
	case ast.Lower, ast.Upper, ast.Trim:
		for idx, input := range inputs {
			if input.DType.DType != proto.PrimitiveDataType_STRING {
				return fmt.Errorf("function %s expect string input, but got %s for input_%d", funcName, input.DType.String(), idx)
			}
		}
	case ast.Sin, ast.Cos, ast.Acos, ast.Asin, ast.Tan, ast.Cot, ast.Atan, ast.GeoDist, ast.Abs, ast.Ceil, ast.Floor, ast.Round, ast.Radians, ast.Degrees, ast.Ln, ast.Log10, ast.Log2, ast.Sqrt, ast.Exp:
		for idx, input := range inputs {
			if !input.DType.IsNumericType() {
				return fmt.Errorf("function %s expect numeric input, but got %s for input_%d", funcName, input.DType.String(), idx)
			}
		}
	case ast.LT, ast.LE, ast.GT, ast.GE, ast.Plus, ast.Minus, ast.Mul, ast.Div:
		for idx, input := range inputs {
			if input.DType.IsStringType() {
				return fmt.Errorf("function %s got string input for input_%d", funcName, idx)
			}
		}
	case ast.IntDiv, ast.Mod:
		if (inputs[0].DType.DType != proto.PrimitiveDataType_INT64 && inputs[0].DType.DType != proto.PrimitiveDataType_DATETIME && inputs[0].DType.DType != proto.PrimitiveDataType_TIMESTAMP) ||
			(inputs[1].DType.DType != proto.PrimitiveDataType_INT64) {
			return fmt.Errorf("function %s: requires both left and right operands be int64-like", funcName)
		}
	case ast.Case, ast.If:
		tp := inputs[len(inputs)-1].DType
		for i := 0; i < len(inputs)/2; i++ {
			if inputs[i*2].DType.IsStringType() {
				return fmt.Errorf("function %s does not support string condition", funcName)
			}
			if !inputs[i*2+1].DType.Equal(tp) {
				return fmt.Errorf("function %s: all values must have the same type", funcName)
			}
		}
	case ast.Ifnull:
		if !inputs[0].DType.Equal(inputs[1].DType) {
			return fmt.Errorf("function Ifnull: both arguments must have the same type")
		}
	}
	return nil
}

func getOutputType(funcName string, inputs []*TensorMeta) (*graph.DataType, error) {
	var outType *graph.DataType
	switch funcName {
	case ast.UnaryNot, ast.IsNull, ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE, ast.LogicAnd, ast.LogicOr:
		outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL)
	case ast.Lower, ast.Upper, ast.Trim, ast.Concat, ast.Substr, ast.Substring:
		outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	case ast.StrToDate:
		outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_DATETIME)
	case ast.Ifnull, ast.Abs, ast.AddDate, ast.SubDate:
		outType = inputs[0].DType
	case ast.Coalesce:
		outType = inputs[0].DType
		if outType.DType == proto.PrimitiveDataType_FLOAT32 {
			outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
		}
	case ast.Case, ast.If:
		outType = inputs[len(inputs)-1].DType
	case ast.Sin, ast.Cos, ast.Acos, ast.Asin, ast.Tan, ast.Cot, ast.Atan, ast.GeoDist, ast.Radians, ast.Degrees, ast.Ln, ast.Log10, ast.Log2, ast.Sqrt, ast.Exp, ast.Div, ast.Atan2:
		outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
	case ast.Ceil, ast.Floor, ast.Round, ast.Mod, ast.IntDiv, ast.DateDiff:
		outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
	case ast.Plus, ast.Minus, ast.Mul, ast.Pow:
		if funcName == ast.Minus && inputs[0].DType.IsTimeType() {
			return graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), nil
		}
		if isFloatOrDoubleType(inputs[0].DType.DType) || isFloatOrDoubleType(inputs[1].DType.DType) {
			return graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64), nil
		}
		if inputs[0].DType.Equal(inputs[1].DType) {
			return inputs[0].DType, nil
		}
		return graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), nil
	case ast.Greatest, ast.Least:
		outType = inputs[0].DType
		isTimestamp := inputs[0].DType.IsTimeType()
		for _, input := range inputs[1:] {
			if isTimestamp {
				if !input.DType.IsTimeType() {
					return nil, fmt.Errorf("function Least/Greatest got wrong type %s", input.DType.String())
				}
			} else {
				if !input.DType.IsNumericType() {
					return nil, fmt.Errorf("function Least/Greatest got wrong type %s", input.DType.String())
				}
				tp, err := graph.GetWiderType(outType, input.DType)
				if err != nil {
					return nil, fmt.Errorf("function Least/Greatest: %v", err)
				}
				outType = tp
			}
		}
	default:
		return graph.NewPrimitiveDataType(proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED), fmt.Errorf("checkAndGetOutputType: unsupported function: %s", funcName)
	}

	return outType, nil
}

func checkArgsNum(funcName string, inputs []*TensorMeta, constParams []*expression.Constant) error {
	if len(inputs) == 0 {
		return fmt.Errorf("checkArgsNum: %s expects at least 1 input tensor, but got 0", funcName)
	}
	argNum, ok := functionArgNum[funcName]
	if !ok {
		return fmt.Errorf("checkArgsNum: unsupported function: %s", funcName)
	}
	if argNum >= 0 && len(inputs) != argNum {
		return fmt.Errorf("checkArgsNum: %s expects %d arguments, but got %d", funcName, argNum, len(inputs))
	}

	// handle special cases
	// TODO: Maintain a detailed function table and unify the check logic
	switch funcName {
	case ast.Substr, ast.Substring:
		if len(inputs) != 1 {
			return fmt.Errorf("checkArgsNum: %s expects 1 input tensor, but got %d", funcName, len(inputs))
		}
		if len(constParams) != 1 && len(constParams) != 2 {
			return fmt.Errorf("checkArgsNum: %s expects 1 or 2 constant parameters, but got %d", funcName, len(constParams))
		}
	case ast.StrToDate:
		if len(inputs) != 1 {
			return fmt.Errorf("checkArgsNum: %s expects 1 input tensor, but got %d", funcName, len(inputs))
		}
		if len(constParams) != 1 {
			return fmt.Errorf("checkArgsNum: %s expects 1 constant parameters, but got %d", funcName, len(constParams))
		}
	case ast.Case:
		if len(inputs)%2 != 1 {
			return fmt.Errorf("checkArgsNum: %s expects odd number of arguments", funcName)
		}
	case ast.GeoDist:
		if len(inputs) != 4 && len(inputs) != 5 {
			return fmt.Errorf("checkArgsNum: %s expects 4 or 5 arguments", funcName)
		}
	}

	return nil
}

func (builder *OperatorGraphBuilder) addFunctionNode(funcName string, inputs []*TensorMeta, constParams []*expression.Constant) (*TensorMeta, error) {

	if builder.checkFuncs {
		if err := checkArgsNum(funcName, inputs, constParams); err != nil {
			return nil, fmt.Errorf("addFunctionNode: %v", err)
		}
		if err := checkFuncInputsType(funcName, inputs); err != nil {
			return nil, fmt.Errorf("addFunctionNode: %v", err)
		}
	}

	outType, err := getOutputType(funcName, inputs)
	if err != nil {
		if builder.checkFuncs {
			return nil, fmt.Errorf("addFunctionNode: %v", err)
		} else {
			outType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED)
		}
	}

	output := builder.tensorMetaManager.CreateTensorMeta(fmt.Sprintf("%s_out", funcName), outType)

	if builder.checkFuncs {
		// check if is scalar
		isScalar := inputs[0].IsConstScalar
		for _, input := range inputs[1:] {
			if input.IsConstScalar != isScalar {
				return nil, fmt.Errorf("addFunctionNode: input tensors should all be scalar or all be tensor")
			}
		}
		output.IsConstScalar = isScalar
	}

	operator := &OperatorFunction{
		inputs:      inputs,
		output:      output,
		constParams: constParams,
		funcName:    funcName,
	}
	builder.addOperator(operator)

	return output, nil
}

func (builder *OperatorGraphBuilder) buildExprScalarFunction(sf *expression.ScalarFunction, tensorTable ResultTable, isApply bool) (*TensorMeta, error) {
	// for functions without arguments, generate OperatorConstant
	if sf.FuncName.L == ast.Now || sf.FuncName.L == ast.Curdate {
		unix_time := builder.createdAt
		var timeDatum types.Datum
		switch sf.FuncName.L {
		case ast.Now:
			timeDatum = types.NewTimeDatum(types.NewTime(types.NewMysqlTime(unix_time), mysql.TypeDate, types.DefaultFsp))
		case ast.Curdate:
			date := time.Date(unix_time.Year(), unix_time.Month(), unix_time.Day(), 0, 0, 0, 0, builder.createdAt.Location())
			timeDatum = types.NewTimeDatum(types.NewTime(types.NewMysqlTime(date), mysql.TypeDate, types.DefaultFsp))
		default:
			return nil, fmt.Errorf("buildExprScalarFunction: unsupported function: %s", sf.FuncName.L)
		}
		return builder.addConstantNode(&timeDatum)
	}

	// for funcitons with arguments, generate OperatorFunction
	args := sf.GetArgs()

	// handle function with const params
	if sf.FuncName.L == ast.Substr || sf.FuncName.L == ast.Substring {
		strInput, err := builder.getTensorFromExpression(args[0], tensorTable, false)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: err input for %s", sf.FuncName.L)
		}
		if strInput.DType.DType != proto.PrimitiveDataType_STRING {
			// TODO: support other types
			return nil, fmt.Errorf("buildScalarFunction: err input type{%s} for %s", strInput.DType, sf.FuncName.L)
		}

		// substr(str, start) or substr(str, start, length)
		constParams := []*expression.Constant{}
		if len(args) >= 2 {
			start, ok := args[1].(*expression.Constant)
			if !ok {
				return nil, fmt.Errorf("buildScalarFunction: args[1] should be constant for %s", sf.FuncName.L)
			}
			constParams = append(constParams, start)
		}
		// substr(str, start, length)
		if len(args) >= 3 {
			length, ok := args[2].(*expression.Constant)
			if !ok {
				return nil, fmt.Errorf("buildScalarFunction: args[2] should be constant for %s", sf.FuncName.L)
			}
			constParams = append(constParams, length)
		}
		return builder.addFunctionNode(sf.FuncName.L, []*TensorMeta{strInput}, constParams)
	}
	if sf.FuncName.L == ast.StrToDate {
		if len(args) != 2 {
			return nil, fmt.Errorf("buildExprScalarFunction StrToDate: incorrect arguments num, expecting 2 but get %v", len(args))
		}
		dateStrExpr, formatStrExpr := args[0], args[1]
		dateStrConst, dateIsConst := dateStrExpr.(*expression.Constant)
		formatStrConst, formatIsConst := formatStrExpr.(*expression.Constant)

		// check format str arg
		if !formatIsConst {
			return nil, fmt.Errorf("buildScalarFunction StrToDate: format string should be constant")
		}
		formatStr := formatStrConst.Value.GetString()
		if formatStrConst.Value.Kind() != types.KindString {
			return nil, fmt.Errorf("buildScalarFunction StrToDate: invalid right argument type, exepecting %d but got %d", types.KindString, formatStrConst.Value.Kind())
		}

		// add constant node for constant dateStr - str_to_date('2025-06-05', '%Y-%m-%d')
		if dateIsConst {
			if dateStrConst.Value.Kind() != types.KindString {
				return nil, fmt.Errorf("buildScalarFunction StrToDate: invalid left argument type, exepecting %d but got %d", types.KindString, dateStrConst.Value.Kind())
			}
			goLayout, err := stringutil.MySQLDateFormatToGoLayout(formatStr)
			if err != nil {
				return nil, fmt.Errorf("buildScalarFunction StrToDate: format string '%s' is invalid: %w", formatStr, err)
			}

			dateStr := dateStrConst.Value.GetString()
			parsedTime, err := time.Parse(goLayout, dateStr)
			if err != nil {
				return nil, fmt.Errorf("buildScalarFunction StrToDate: date string '%s' is invalid: %w", dateStr, err)
			}

			mysqlTime := types.NewTime(
				types.NewMysqlTime(parsedTime),
				mysql.TypeDate,
				types.DefaultFsp,
			)

			var datum types.Datum
			datum.SetMysqlTime(mysqlTime)
			return builder.addConstantNode(&datum)
		}

		// format str is a const expression, and date str is not
		dateTensor, err := builder.getTensorFromExpression(dateStrExpr, tensorTable, false)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		return builder.addFunctionNode(sf.FuncName.L, []*TensorMeta{dateTensor}, []*expression.Constant{formatStrConst})
	}

	// handle input tensors
	inputs := []*TensorMeta{}
	if sf.FuncName.L == ast.AddDate || sf.FuncName.L == ast.DateAdd || sf.FuncName.L == ast.SubDate || sf.FuncName.L == ast.DateSub {
		var err error
		args, err = expression.TransferDateFuncIntervalToSeconds(args)
		if err != nil {
			return nil, err
		}

		// add constant node for constant dateStr - eg. DATE_ADD/SUB('2025-04-05', INTERVAL 10 DAY)
		if dateStrConst, ok := args[0].(*expression.Constant); ok && dateStrConst.Value.Kind() == types.KindString {
			if intervalConst, ok := args[1].(*expression.Constant); ok {
				return builder.buildConstantDateAdd(dateStrConst, intervalConst, sf.FuncName.L)
			}
		}
	}
	for _, arg := range args {
		input, err := builder.getTensorFromExpression(arg, tensorTable, false)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		inputs = append(inputs, input)
	}
	// Inputs may need cast
	if _, exist := constTensorNeedCastFuncs[sf.FuncName.L]; exist {

		castOndemand := func(constTensor, otherTensor *TensorMeta) (*TensorMeta, error) {
			// Only support string to time currently
			if !constTensor.IsConstScalar {
				return constTensor, nil
			}
			if !constTensor.DType.IsStringType() {
				return constTensor, nil
			}
			if !otherTensor.DType.IsTimeType() {
				return constTensor, nil
			}

			// Check passed, do the cast
			casted := builder.tensorMetaManager.CreateTensorMetaAs(inputs[0])
			casted.DType = otherTensor.DType

			castInputs := []*TensorMeta{constTensor}
			operator := &OperatorFunction{
				inputs:      castInputs,
				output:      casted,
				constParams: []*expression.Constant{},
				funcName:    ast.Cast,
			}
			builder.addOperator(operator)

			return casted, nil
		}

		if len(inputs) != 2 {
			return nil, fmt.Errorf("buildExprScalarFunction: %s expects 2 arguments", sf.FuncName.L)
		}
		left, err := castOndemand(inputs[0], inputs[1])
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		right, err := castOndemand(inputs[1], left)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		inputs[0] = left
		inputs[1] = right
	}

	if !isApply && builder.checkFuncs {
		var err error
		inputs, err = builder.addBroadcastToOndemand(inputs)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
	}

	// some functions requires special handling
	switch sf.FuncName.L {
	case ast.Cot:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction: expect 1 argument for %s, got %d", sf.FuncName.L, len(inputs))
		}

		tan, err := builder.addFunctionNode(ast.Tan, inputs, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Cot: %v", err)
		}

		oneDatum := types.NewIntDatum(int64(1))
		oneScalar, err := builder.addConstantNode(&oneDatum)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Cot: %v", err)
		}

		alligned, err := builder.addBroadcastToOndemand([]*TensorMeta{oneScalar, tan})
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Cot: %v", err)
		}

		output, err := builder.addFunctionNode(ast.Div, alligned, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Cot: %v", err)
		}
		return output, nil

	case ast.UnaryMinus:
		zeroDatum := types.NewIntDatum(0)
		zeroScalar, err := builder.addConstantNode(&zeroDatum)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		allignedInputs, err := builder.addBroadcastToOndemand([]*TensorMeta{zeroScalar, inputs[0]})
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		output, err := builder.addFunctionNode(ast.Minus, allignedInputs, []*expression.Constant{})
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		return output, nil
	case ast.UnaryPlus:
		return inputs[0], nil
	case ast.Cast:
		if len(inputs) != 1 {
			return nil, fmt.Errorf("buildScalarFunction:err input for %s expected for %d got %d", sf.FuncName.L, 1, len(inputs))
		}
		output := builder.tensorMetaManager.CreateTensorMetaAs(inputs[0])
		outType, err := graph.ConvertDataType(sf.RetType)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
		}
		output.DType = outType
		operator := &OperatorFunction{
			inputs:      inputs,
			output:      output,
			constParams: []*expression.Constant{},
			funcName:    ast.Cast,
		}
		builder.addOperator(operator)
		return output, nil
	case ast.GeoDist:
		if len(inputs) != 4 && len(inputs) != 5 {
			return nil, fmt.Errorf("buildExprScalarFunction: incorrect arguments for function Geodist, exepcting (longittude1, latitude1, longtitude2, latitude2)")
		}
		// distance = radius * arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtidude2))
		// convertDegreeToRadians
		degreeToRadianCoefficient := math.Pi / 180
		coeffDatum := types.NewFloat64Datum(degreeToRadianCoefficient)
		coeffScalar, err := builder.addConstantNode(&coeffDatum)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		allignedInputs, err := builder.addBroadcastToOndemand([]*TensorMeta{coeffScalar, inputs[0]})
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		coeffTensor := allignedInputs[0]

		longtitude1, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{coeffTensor, inputs[0]}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		latidude1, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{coeffTensor, inputs[1]}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		longtitude2, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{coeffTensor, inputs[2]}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		latidude2, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{coeffTensor, inputs[3]}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		//sin(latitude1)
		sinLatitude1, err := builder.addFunctionNode(ast.Sin, []*TensorMeta{latidude1}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		//sin(latitude2)
		sinLatitude2, err := builder.addFunctionNode(ast.Sin, []*TensorMeta{latidude2}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		// sin(latitude1) * sin(latitude2)
		sinMulSin, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{sinLatitude1, sinLatitude2}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		// cos(latitude1)
		cosLatitude1, err := builder.addFunctionNode(ast.Cos, []*TensorMeta{latidude1}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		// cos(latitude2)
		cosLatitude2, err := builder.addFunctionNode(ast.Cos, []*TensorMeta{latidude2}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		// cos(latittude1) * cos(latitude2)
		cosMulCos, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{cosLatitude1, cosLatitude2}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		// longtitude1 - longtitude2
		longtitudeDiff, err := builder.addFunctionNode(ast.Minus, []*TensorMeta{longtitude1, longtitude2}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		// cos(longtitude1 - longtitude2)
		cosDiff, err := builder.addFunctionNode(ast.Cos, []*TensorMeta{longtitudeDiff}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		// cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2)
		cosMulCosMulCos, err := builder.addFunctionNode(ast.Mul, []*TensorMeta{cosMulCos, cosDiff}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		// sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2)
		sum, err := builder.addFunctionNode(ast.Plus, []*TensorMeta{sinMulSin, cosMulCosMulCos}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		// arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2))
		arcCos, err := builder.addFunctionNode(ast.Acos, []*TensorMeta{sum}, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		radius := &TensorMeta{}
		if len(inputs) == 4 {
			averageRadius := 6371 // average radius of earth from https://simple.wikipedia.org/wiki/Earth_radius
			radiusDatum := types.NewIntDatum(int64(averageRadius))
			radius, err = builder.addConstantNode(&radiusDatum)
			if err != nil {
				return nil, err
			}
		} else {
			radius = inputs[4]
		}

		// radius * arc cos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longtitude1 - longtitude2))
		alligned, err := builder.addBroadcastToOndemand([]*TensorMeta{radius, arcCos})
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}

		output, err := builder.addFunctionNode(ast.Mul, alligned, nil)
		if err != nil {
			return nil, fmt.Errorf("buildExprScalarFunction Geodist: %v", err)
		}
		return output, nil
	}

	output, err := builder.addFunctionNode(sf.FuncName.L, inputs, []*expression.Constant{})
	if err != nil {
		return nil, fmt.Errorf("buildExprScalarFunction: %v", err)
	}
	return output, nil
}

func (builder *OperatorGraphBuilder) buildExprConstant(c *expression.Constant) (*TensorMeta, error) {
	return builder.addConstantNode(&c.Value)
}

func (builder *OperatorGraphBuilder) addConstantNode(value *types.Datum) (*TensorMeta, error) {
	var dType *graph.DataType
	switch value.Kind() {
	case types.KindFloat32:
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32)
	case types.KindFloat64:
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
	case types.KindInt64:
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
	case types.KindMysqlDecimal:
		// NOTE(shunde.csd): SCQL Internal does not distinguish decimal and float,
		// It handles decimal as float.
		// If users have requirements for precision, they should use integers.
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
	case types.KindString:
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	case types.KindMysqlTime:
		dType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_DATETIME)
	default:
		return nil, fmt.Errorf("buildExprConstant: unsupported data{%+v}", value)
	}

	output := builder.tensorMetaManager.CreateTensorMeta("constant_data", dType)
	output.IsConstScalar = true

	operator := &OperatorConstant{
		output: output,
		value:  value,
	}
	builder.addOperator(operator)
	return output, nil
}

// buildConstantDateAdd computes date addition/subtraction for string constant dates
func (builder *OperatorGraphBuilder) buildConstantDateAdd(dateStrConst *expression.Constant, intervalSecondsConst *expression.Constant, funcName string) (*TensorMeta, error) {
	dateStr := dateStrConst.Value.GetString()
	intervalSeconds := intervalSecondsConst.Value.GetInt64()

	formats := []string{
		"2006-01-02",                 // DATE format
		"2006-01-02 15:04:05",        // DATETIME format
		"2006-01-02T15:04:05",        // ISO format
		"2006-01-02 15:04:05.999999", // DATETIME with microseconds
	}

	var parsedTime time.Time
	var err error
	mysqlType := mysql.TypeDate

	for _, format := range formats {
		parsedTime, err = time.Parse(format, dateStr)
		if err == nil {
			// Determine MySQL type based on format
			if strings.Contains(format, "15:04") {
				mysqlType = mysql.TypeDatetime
			}
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("buildConstantDateAdd: invalid date string '%s': %w", dateStr, err)
	}

	// Apply interval (negative for subtraction functions)
	if funcName == ast.SubDate || funcName == ast.DateSub {
		intervalSeconds = -intervalSeconds
	}
	resultTime := parsedTime.Add(time.Duration(intervalSeconds) * time.Second)

	mysqlTime := types.NewTime(
		types.NewMysqlTime(resultTime),
		mysqlType,
		types.DefaultFsp,
	)

	var datum types.Datum
	datum.SetMysqlTime(mysqlTime)
	return builder.addConstantNode(&datum)
}
