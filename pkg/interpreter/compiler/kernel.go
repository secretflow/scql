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
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/compute"

	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
	"github.com/secretflow/scql/pkg/util/stringutil"
)

type Kernel interface {
	// ensureTensorPlace ensures the tensor status of the node meets the requirements of the kernel
	// tensor status conversion maybe performed when required placed tensor does not exist
	// TODO: remove ensureTensorPlace and set placed tensors in kernel directly when resolving kernel
	ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) error
	// toEngineNodes emit engine ops according to the Operator and the Kernel
	toEngineNodes(builder *ExecutionGraphBuilder, node Operator) error
	// String returns the string representation of the kernel
	String() string
	// Cost returns the cost of the kernel
	// Note that comparing Cost values is only meaningful when the original Operator is identical
	// range: [0, 1]
	Cost() float64
}

type KernelRunSQL struct {
}

func (k *KernelRunSQL) Cost() float64 {
	// not related to MPC
	return 0.0
}

func (k *KernelRunSQL) String() string {
	return "KernelRunSQL"
}

func (k *KernelRunSQL) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	// OperatorRunSQL has no inputs, no need to ensure tensor status
	return nil
}

func (k *KernelRunSQL) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	dataSource, ok := node.(*OperatorRunSQL)
	if !ok {
		return fmt.Errorf("KernelRunSQL toEngineNodes: expect data source node, but got %T", node)
	}

	outputs := make([]*graph.Tensor, 0, len(dataSource.outputs))
	for _, meta := range dataSource.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: dataSource.sourceParty})
		if err != nil {
			return fmt.Errorf("KernelRunSQL toEngineNodes: %w", err)
		}
		outputs = append(outputs, tensor)
	}

	if err := builder.addOpRunSQL(outputs, dataSource.sourceParty, dataSource.sql, dataSource.tableRefs); err != nil {
		return fmt.Errorf("KernelRunSQL toEngineNodes: %w", err)
	}
	return nil
}

type KernelPublishResult struct {
	// Bonded tensors in this kernel
	resultTensors []*graph.Tensor
}

func (k *KernelPublishResult) String() string {
	return "KernelPublishResult"
}

func (k *KernelPublishResult) Cost() float64 {
	// not related to MPC
	return 0.0
}

func (k *KernelPublishResult) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	result, ok := node.(*OperatorResult)
	if !ok {
		return fmt.Errorf("KernelPublishResult ensureTensorPlace: expect result node, but got %T", node)
	}

	k.resultTensors, err = createPlacedTensors(builder, result.resultTensors, &privatePlacement{partyCode: result.issuerPartyCode})
	if err != nil {
		return fmt.Errorf("KernelPublishResult ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelPublishResult) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	result, ok := node.(*OperatorResult)
	if !ok {
		return fmt.Errorf("KernelPublishResult toEngineNodes: expect result node, but got %T", node)
	}

	if len(k.resultTensors) != len(result.outputNames) {
		return fmt.Errorf("KernelPublishResult toEngineNodes: result tensors and output names length not match")
	}

	outputs := make([]*graph.Tensor, 0, len(k.resultTensors))
	for idx, tensor := range k.resultTensors {
		output := builder.tm.CreateResultTensor(tensor, result.outputNames[idx])
		outputs = append(outputs, output)
	}

	if err := builder.addOpPublish(k.resultTensors, outputs, result.issuerPartyCode); err != nil {
		return fmt.Errorf("KernelPublishResult toEngineNodes: %w", err)
	}
	return nil
}

type KernelInsertTable struct {
	insertTableOpt *core.InsertTableOption

	// Bonded tensors in this kernel
	resultTensors []*graph.Tensor
}

func (k *KernelInsertTable) Cost() float64 {
	// not related to MPC
	return 0.0
}

func (k *KernelInsertTable) String() string {
	return fmt.Sprintf("KernelInsertTable(insertTableOpt: %v)", k.insertTableOpt)
}

func (k *KernelInsertTable) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	result, ok := node.(*OperatorResult)
	if !ok {
		return fmt.Errorf("KernelInsertTable ensureTensorPlace: expect result node, but got %T", node)
	}

	k.resultTensors, err = createPlacedTensors(builder, result.resultTensors, &privatePlacement{partyCode: result.issuerPartyCode})
	if err != nil {
		return fmt.Errorf("KernelInsertTable ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelInsertTable) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	result, ok := node.(*OperatorResult)
	if !ok {
		return fmt.Errorf("KernelInsertTable toEngineNodes: expect result node, but got %T", node)
	}

	if len(k.resultTensors) != len(result.outputNames) {
		return fmt.Errorf("KernelInsertTable toEngineNodes: result tensors and output names length not match")
	}

	outputs := make([]*graph.Tensor, 0, len(k.resultTensors))
	for idx, tensor := range k.resultTensors {
		output := builder.tm.CreateResultTensor(tensor, result.outputNames[idx])
		outputs = append(outputs, output)
	}

	if err := builder.addOpInsertTable(k.resultTensors, outputs, result.issuerPartyCode, k.insertTableOpt); err != nil {
		return fmt.Errorf("KernelInsertTable toEngineNodes: %w", err)
	}
	return nil
}

type KernelDumpFile struct {
	intoOpt *core.IntoOpt
}

func (k *KernelDumpFile) Cost() float64 {
	// not related to MPC
	return 0.0
}

func (k *KernelDumpFile) String() string {
	return fmt.Sprintf("KernelDumpFile(intoOpt: %v)", k.intoOpt)
}

func (k *KernelDumpFile) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	return nil
}

func (k *KernelDumpFile) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	result, ok := node.(*OperatorResult)
	if !ok {
		return fmt.Errorf("KernelDumpFile toEngineNodes: expect result node, but got %T", node)
	}

	for _, partyFile := range k.intoOpt.Opt.PartyFiles {
		var partyResults []*TensorMeta
		var resultNames []string
		if len(partyFile.FieldList) == 0 {
			partyResults = result.resultTensors
			resultNames = result.outputNames
		} else {
			nameRecords := make(map[string]string)
			for idx, col := range k.intoOpt.PartyColumns[partyFile.PartyCode] {
				tensor, ok := result.resultTable[col.UniqueID]
				if !ok {
					return fmt.Errorf("KernelDumpFile toEngineNodes: result tensor %d not found", idx)
				}
				partyResults = append(partyResults, tensor)

				expr, ok := partyFile.FieldList[idx].Expr.(*ast.ColumnNameExpr)
				if !ok {
					return fmt.Errorf("KernelDumpFile toEngineNodes: custom into field for party %s must be column name expr", partyFile.PartyCode)
				}
				var colName string
				if partyFile.FieldList[idx].AsName.String() != "" {
					colName = partyFile.FieldList[idx].AsName.String() // using as name
				} else {
					colName = expr.Name.Name.String()
				}
				if _, ok := nameRecords[colName]; ok {
					return fmt.Errorf("KernelDumpFile toEngineNodes:  column name: '%s' duplicated in custom into field for party %s, please remove redundant items or using as name like: 'select ta.id ... (ta.id, ta.id as id2) ...' or 'select ta.id as id1, tb.id as id2 ... (id1, id2) ...'", colName, partyFile.PartyCode)
				}
				nameRecords[colName] = expr.Name.String()
				resultNames = append(resultNames, colName)
			}
		}

		inputs, outputs, err := builder.prepareResultForParty(partyResults, resultNames, partyFile.PartyCode)
		if err != nil {
			return fmt.Errorf("KernelDumpFile toEngineNodes: %w", err)
		}

		if err := builder.addOpDumpFile(inputs, outputs, k.intoOpt.Opt, partyFile); err != nil {
			return fmt.Errorf("KernelDumpFile toEngineNodes: %w", err)
		}
	}
	return nil
}

type KernelPsiEQJoin struct {
	leftParty        string
	rightParty       string
	psiAlgorithmType proto.PsiAlgorithmType

	leftPayloadsPlacement  []tensorPlacement
	rightPayloadsPlacement []tensorPlacement

	leftTouchResult  bool
	rightTouchResult bool

	// Bonded tensors in this kernel
	leftKeys      []*graph.Tensor
	rightKeys     []*graph.Tensor
	leftPayloads  []*graph.Tensor
	rightPayloads []*graph.Tensor
	leftOutputs   []*graph.Tensor
	rightOutputs  []*graph.Tensor
}

func (k *KernelPsiEQJoin) Cost() float64 {
	// way faster than secret join
	// considering there will be local join in the future, we keep 0 for local join
	return 0.2
}

func (k *KernelPsiEQJoin) String() string {
	return fmt.Sprintf("KernelPsiEQJoin(leftParty: %s, rightParty: %s, psiAlgorithmType: %v, leftPayloadsPlacement: %v, rightPayloadsPlacement: %v, leftTouchResult: %t, rightTouchResult: %t)",
		k.leftParty, k.rightParty, k.psiAlgorithmType, k.leftPayloadsPlacement, k.rightPayloadsPlacement, k.leftTouchResult, k.rightTouchResult)
}

func (k *KernelPsiEQJoin) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	eqJoin, ok := node.(*OperatorEQJoin)
	if !ok {
		return fmt.Errorf("KernelPsiEQJoin ensureTensorPlace: expect eq join node, but got %T", node)
	}

	k.leftKeys, err = createPlacedTensors(builder, eqJoin.leftKeys, &privatePlacement{partyCode: k.leftParty})
	if err != nil {
		return fmt.Errorf("KernelPsiEQJoin ensureTensorPlace: %w", err)
	}

	k.rightKeys, err = createPlacedTensors(builder, eqJoin.rightKeys, &privatePlacement{partyCode: k.rightParty})
	if err != nil {
		return fmt.Errorf("KernelPsiEQJoin ensureTensorPlace: %w", err)
	}

	k.leftPayloads = make([]*graph.Tensor, len(eqJoin.leftPayloads))
	for idx, meta := range eqJoin.leftPayloads {
		var placement tensorPlacement
		if builder.IsBatched() {
			placement = &privatePlacement{partyCode: k.leftParty}
		} else {
			placement = k.leftPayloadsPlacement[idx]
		}
		tensor, err := builder.getOrCreatePlacedTensor(meta, placement)
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin ensureTensorPlace: %w", err)
		}
		k.leftPayloads[idx] = tensor
	}

	k.rightPayloads = make([]*graph.Tensor, len(eqJoin.rightPayloads))
	for idx, meta := range eqJoin.rightPayloads {
		var placement tensorPlacement
		if builder.IsBatched() {
			placement = &privatePlacement{partyCode: k.rightParty}
		} else {
			placement = k.rightPayloadsPlacement[idx]
		}
		tensor, err := builder.getOrCreatePlacedTensor(meta, placement)
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin ensureTensorPlace: %w", err)
		}
		k.rightPayloads[idx] = tensor
	}
	return nil
}

func (k *KernelPsiEQJoin) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	eqJoin, ok := node.(*OperatorEQJoin)
	if !ok {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: expect eq join node, but got %T", node)
	}

	if len(k.leftPayloads) != len(eqJoin.leftOutputs) {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: left payloads and left outputs length not match")
	}
	if len(k.rightPayloads) != len(eqJoin.rightOutputs) {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: right payloads and right outputs length not match")
	}

	leftKeys := k.leftKeys
	rightKeys := k.rightKeys
	leftPayloads := k.leftPayloads
	rightPayloads := k.rightPayloads

	if builder.IsBatched() {
		makeBuckets := func(keys, payloads []*graph.Tensor) ([]*graph.Tensor, []*graph.Tensor, error) {
			// check all tensors are private and belong to the same party code
			partyCode := keys[0].OwnerPartyCode
			for _, tensor := range append(keys, payloads...) {
				if tensor.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
					return nil, nil, fmt.Errorf("all tensors must be private, but got %s", tensor.Status())
				}
				if tensor.OwnerPartyCode != partyCode {
					return nil, nil, fmt.Errorf("all tensors must belong to the same party code %s, but got %s", partyCode, tensor.OwnerPartyCode)
				}
			}

			// use a map to avoid redundant bucket tensor creation
			bucketTensorMap := make(map[int]*graph.Tensor)
			outputs := make([]*graph.Tensor, 0, len(keys)+len(payloads))
			dedupOutputs := make([]*graph.Tensor, 0, len(keys)+len(payloads))
			dedupInputs := make([]*graph.Tensor, 0, len(keys)+len(payloads))
			for _, tensor := range keys {
				bucketTensorMap[tensor.ID] = builder.tm.CreateTensorAs(tensor)
				outputs = append(outputs, bucketTensorMap[tensor.ID])
				dedupOutputs = append(dedupOutputs, bucketTensorMap[tensor.ID])
				dedupInputs = append(dedupInputs, tensor)
			}
			for _, tensor := range payloads {
				if _, ok := bucketTensorMap[tensor.ID]; !ok {
					bucketTensorMap[tensor.ID] = builder.tm.CreateTensorAs(tensor)
					// only add payload and corresponding output to dedup inputs and outputs when it has not been added tp bucketTensorMap before
					dedupOutputs = append(dedupOutputs, bucketTensorMap[tensor.ID])
					dedupInputs = append(dedupInputs, tensor)
				}
				outputs = append(outputs, bucketTensorMap[tensor.ID])
			}

			if err := builder.addOpBucket(keys, dedupInputs, dedupOutputs, []string{partyCode}); err != nil {
				return nil, nil, err
			}

			return outputs[:len(keys)], outputs[len(keys):], nil
		}

		var err error
		leftKeys, leftPayloads, err = makeBuckets(leftKeys, leftPayloads)
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
		}
		rightKeys, rightPayloads, err = makeBuckets(rightKeys, rightPayloads)
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
		}
	}

	var leftIndex, rightIndex *graph.Tensor
	if k.leftTouchResult {
		leftIndex = builder.tm.CreateUnbondedTensor("left_index", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{partyCode: k.leftParty})
	}
	if k.rightTouchResult {
		rightIndex = builder.tm.CreateUnbondedTensor("right_index", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{partyCode: k.rightParty})
	}

	if err := builder.addOpPsiJoin(leftKeys, rightKeys, leftIndex, rightIndex, []string{k.leftParty, k.rightParty}, JoinTypeLpToEp[eqJoin.joinType], k.psiAlgorithmType); err != nil {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
	}

	k.leftOutputs = make([]*graph.Tensor, len(eqJoin.leftOutputs))
	for idx, meta := range eqJoin.leftOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, extractTensorPlacement(leftPayloads[idx]))
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
		}
		k.leftOutputs[idx] = tensor
	}
	k.rightOutputs = make([]*graph.Tensor, len(eqJoin.rightOutputs))
	for idx, meta := range eqJoin.rightOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, extractTensorPlacement(rightPayloads[idx]))
		if err != nil {
			return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
		}
		k.rightOutputs[idx] = tensor
	}

	addFilterByIndex := func(index *graph.Tensor, payloads, outputs []*graph.Tensor) error {
		partyPayloadIdx := make(map[string][]int)
		for idx, payload := range payloads {
			partyPayloadIdx[payload.OwnerPartyCode] = append(partyPayloadIdx[payload.OwnerPartyCode], idx)
		}

		for party, idxs := range sliceutil.SortedMap(partyPayloadIdx) {
			partyPayloads, err := sliceutil.TakeByIndices(payloads, idxs)
			if err != nil {
				return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
			}
			partyOutputs, err := sliceutil.TakeByIndices(outputs, idxs)

			partyIndex := index
			if party != index.OwnerPartyCode {
				partyIndex, err = builder.convertStatus(index, &privatePlacement{partyCode: party}, false)
				if err != nil {
					return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
				}
			}

			if err != nil {
				return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
			}
			if err := builder.addOpFilterByIndex(partyIndex, partyPayloads, partyOutputs, party); err != nil {
				return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
			}
		}

		return nil
	}

	if err := addFilterByIndex(leftIndex, leftPayloads, k.leftOutputs); err != nil {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
	}
	if err := addFilterByIndex(rightIndex, rightPayloads, k.rightOutputs); err != nil {
		return fmt.Errorf("KernelPsiEQJoin toEngineNodes: %w", err)
	}

	return nil
}

type KernelSecretEQJoin struct {
	// Bonded tensors in this kernel
	leftKeys      []*graph.Tensor
	rightKeys     []*graph.Tensor
	leftPayloads  []*graph.Tensor
	rightPayloads []*graph.Tensor
	leftOutputs   []*graph.Tensor
	rightOutputs  []*graph.Tensor
}

func (k *KernelSecretEQJoin) Cost() float64 {
	// secret join is very expensive
	return 1.0
}

func (k *KernelSecretEQJoin) String() string {
	return "KernelSecretEQJoin"
}

func (k *KernelSecretEQJoin) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	eqJoin, ok := node.(*OperatorEQJoin)
	if !ok {
		return fmt.Errorf("KernelSecretEQJoin ensureTensorPlace: expect eq join node, but got %T", node)
	}

	k.leftKeys, err = createPlacedTensors(builder, eqJoin.leftKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretEQJoin ensureTensorPlace: %w", err)
	}

	k.rightKeys, err = createPlacedTensors(builder, eqJoin.rightKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretEQJoin ensureTensorPlace: %w", err)
	}

	k.leftPayloads, err = createPlacedTensors(builder, eqJoin.leftPayloads, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretEQJoin ensureTensorPlace: %w", err)
	}

	k.rightPayloads, err = createPlacedTensors(builder, eqJoin.rightPayloads, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretEQJoin ensureTensorPlace: %w", err)
	}
	return nil
}

func (k *KernelSecretEQJoin) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	eqJoin, ok := node.(*OperatorEQJoin)
	if !ok {
		return fmt.Errorf("KernelSecretEQJoin toEngineNodes: expect eq join node, but got %T", node)
	}

	k.leftOutputs = make([]*graph.Tensor, len(eqJoin.leftOutputs))
	for idx, meta := range eqJoin.leftOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
		if err != nil {
			return fmt.Errorf("KernelSecretEQJoin toEngineNodes: %w", err)
		}
		k.leftOutputs[idx] = tensor
	}

	k.rightOutputs = make([]*graph.Tensor, len(eqJoin.rightOutputs))
	for idx, meta := range eqJoin.rightOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
		if err != nil {
			return fmt.Errorf("KernelSecretEQJoin toEngineNodes: %w", err)
		}
		k.rightOutputs[idx] = tensor
	}

	if err := builder.addOpSecretJoin(k.leftKeys, k.rightKeys, k.leftPayloads, k.rightPayloads, k.leftOutputs, k.rightOutputs); err != nil {
		return fmt.Errorf("KernelSecretEQJoin toEngineNodes: %w", err)
	}
	return nil
}

type KernelSimpleReduce struct {
	placement tensorPlacement
	aggFunc   *aggregation.AggFuncDesc

	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelSimpleReduce) Cost() float64 {
	if IsSecret(k.placement) {
		// secret reduce is more expensive than private reduce
		return 0.6
	}
	// private reduce, cheaper than private distinct count
	return 0.1
}

func (k *KernelSimpleReduce) String() string {
	return fmt.Sprintf("KernelSimpleReduce(aggName = %s, placement = %s)", k.aggFunc.Name, k.placement)
}

func (k *KernelSimpleReduce) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelSimpleReduce ensureTensorPlace: expect reduce node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(reduce.input, k.placement)
	if err != nil {
		return fmt.Errorf("KernelSimpleReduce ensureTensorPlace: %w", err)
	}
	k.input = tensor
	return nil
}

func (k *KernelSimpleReduce) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelSimpleReduce toEngineNodes: expect reduce node, but got %T", node)
	}

	tensor, err := builder.tm.CreateAndSetFirstTensor(reduce.output, k.placement)
	if err != nil {
		return fmt.Errorf("KernelSimpleReduce toEngineNodes: %w", err)
	}
	k.output = tensor

	attrs := map[string]*graph.Attribute{}
	if k.aggFunc.Name == ast.AggPercentileDisc {
		if len(k.aggFunc.Args) != 2 {
			return fmt.Errorf("KernelSimpleReduce toEngineNodes: expect 2 args for percentile_disc, but got %d", len(k.aggFunc.Args))
		}
		attr := &graph.Attribute{}
		percent, err := strconv.ParseFloat(k.aggFunc.Args[1].String(), 64)
		if err != nil {
			return fmt.Errorf("KernelSimpleReduce: %s is not a valid float value", k.aggFunc.Args[1].String())
		}
		if percent < 0 || percent > 1 {
			return fmt.Errorf("KernelSimpleReduce: percent should be in [0, 1], but got %v", percent)
		}
		attr.SetDouble(percent)
		attrs[operator.PercentAttr] = attr
	}

	reduceName := k.aggFunc.Name
	if reduceName == ast.AggFuncCount && k.aggFunc.Mode == aggregation.FinalMode {
		reduceName = ast.AggFuncSum
	}

	if err := builder.addOpReduce(reduceName, k.input, k.output, attrs); err != nil {
		return fmt.Errorf("KernelSimpleReduce toEngineNodes: %w", err)
	}
	return nil
}

type KernelPrivateDistinctCount struct {
	partyCode string

	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelPrivateDistinctCount) Cost() float64 {
	// private count is relatively cheap
	return 0.3
}

func (k *KernelPrivateDistinctCount) String() string {
	return fmt.Sprintf("KernelPrivateDistinctCount(partyCode = %s)", k.partyCode)
}

func (k *KernelPrivateDistinctCount) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelPrivateDistinctCount ensureTensorPlace: expect reduce node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(reduce.input, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateDistinctCount ensureTensorPlace: %w", err)
	}
	k.input = tensor

	return nil
}

func (k *KernelPrivateDistinctCount) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelPrivateDistinctCount toEngineNodes: expect reduce node, but got %T", node)
	}

	// Create unique tensor using kernel's input
	uniqueResult := builder.tm.CreateTensorAs(k.input)
	uniqueResult.Name = "unique_" + uniqueResult.Name
	if err := builder.addOpUnique(k.input, uniqueResult); err != nil {
		return fmt.Errorf("KernelPrivateDistinctCount toEngineNodes: %w", err)
	}

	output, err := builder.tm.CreateAndSetFirstTensor(reduce.output, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateDistinctCount toEngineNodes: %w", err)
	}
	k.output = output

	if err := builder.addOpReduce(ast.AggFuncCount, uniqueResult, k.output, map[string]*graph.Attribute{}); err != nil {
		return fmt.Errorf("KernelPrivateDistinctCount toEngineNodes: %w", err)
	}

	return nil
}

type KernelSecretDistinctCount struct {
	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelSecretDistinctCount) Cost() float64 {
	// distinct count needs to sort the tensor, which is very expensive
	return 1.0
}

func (k *KernelSecretDistinctCount) String() string {
	return "KernelSecretDistinctCount"
}

func (k *KernelSecretDistinctCount) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelSecretDistinctCount ensureTensorPlace: expect reduce node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(reduce.input, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretDistinctCount ensureTensorPlace: %w", err)
	}
	k.input = tensor

	return nil
}

func (k *KernelSecretDistinctCount) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelSecretDistinctCount toEngineNodes: expect reduce node, but got %T", node)
	}

	// Sort using kernel's input
	sorted := builder.tm.CreateTensorAs(k.input)
	sorted.Name = "sorted_" + sorted.Name
	if err := builder.addOpSort([]*graph.Tensor{k.input}, []*graph.Tensor{k.input}, []*graph.Tensor{sorted}, []bool{false}); err != nil {
		return fmt.Errorf("KernelSecretDistinctCount toEngineNodes: %w", err)
	}

	groupMarkDistinct := builder.tm.CreateUnbondedTensor("group_mark_distinct", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), &secretPlacement{})
	if err := builder.addOpObliviousGroupMark([]*graph.Tensor{sorted}, groupMarkDistinct); err != nil {
		return fmt.Errorf("KernelSecretDistinctCount toEngineNodes: %w", err)
	}

	out, err := builder.tm.CreateAndSetFirstTensor(reduce.output, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretDistinctCount toEngineNodes: %w", err)
	}
	k.output = out
	if err := builder.addOpReduce(ast.AggFuncSum, groupMarkDistinct, k.output, map[string]*graph.Attribute{}); err != nil {
		return fmt.Errorf("KernelSecretDistinctCount toEngineNodes: %w", err)
	}

	return nil
}

type KernelShapeCount struct {
	inputPlacement tensorPlacement
	partyCode      string

	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelShapeCount) Cost() float64 {
	// its' easy to get shape of tensor, whether it's private or secret or public
	return 0.0
}

func (k *KernelShapeCount) String() string {
	return fmt.Sprintf("KernelShapeCount(inputPlacement = %s, partyCode = %s)", k.inputPlacement, k.partyCode)
}

func (k *KernelShapeCount) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelShapeCount ensureTensorPlace: expect reduce node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(reduce.input, k.inputPlacement)
	if err != nil {
		return fmt.Errorf("KernelShapeCount ensureTensorPlace: %w", err)
	}
	k.input = tensor

	return nil
}

func (k *KernelShapeCount) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	reduce, ok := node.(*OperatorReduce)
	if !ok {
		return fmt.Errorf("KernelShapeCount toEngineNodes: expect reduce node, but got %T", node)
	}

	output, err := builder.tm.CreateAndSetFirstTensor(reduce.output, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelShapeCount toEngineNodes: %w", err)
	}
	k.output = output
	if err := builder.addOpShape(k.input, k.output); err != nil {
		return fmt.Errorf("KernelShapeCount toEngineNodes: %w", err)
	}

	return nil
}

type KernelBroadcastTo struct {
	refPlacement tensorPlacement

	// Bonded tensors in this kernel
	shapeRef *graph.Tensor
	scalars  []*graph.Tensor
	outputs  []*graph.Tensor
}

func (k *KernelBroadcastTo) Cost() float64 {
	// The broadcasted tensor is public and the reference tensor is public or private.
	// It's easy to get the shape of reference tensor, whether it's private or secret or public.
	return 0.0
}

func (k *KernelBroadcastTo) String() string {
	return fmt.Sprintf("KernelBroadcastTo(inputPlacement = %s)", k.refPlacement)
}

func (k *KernelBroadcastTo) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	broadcast, ok := node.(*OperatorBroadcastTo)
	if !ok {
		return fmt.Errorf("KernelBroadcastTo ensureTensorPlace: expect BroadcastTo node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(broadcast.shapeRef, k.refPlacement)
	if err != nil {
		return fmt.Errorf("KernelBroadcastTo ensureTensorPlace: %w", err)
	}
	k.shapeRef = tensor

	k.scalars = make([]*graph.Tensor, len(broadcast.scalars))
	for idx, meta := range broadcast.scalars {
		tensor, err := builder.getOrCreatePlacedTensor(meta, &publicPlacement{})
		if err != nil {
			return fmt.Errorf("KernelBroadcastTo ensureTensorPlace: %w", err)
		}
		k.scalars[idx] = tensor
	}

	return nil
}

func (k *KernelBroadcastTo) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	broadcast, ok := node.(*OperatorBroadcastTo)
	if !ok {
		return fmt.Errorf("KernelBroadcastTo toEngineNodes: expect BroadcastTo node, but got %T", node)
	}

	var outputPlacement tensorPlacement
	if pp, ok := k.refPlacement.(*privatePlacement); ok {
		outputPlacement = &privatePlacement{partyCode: pp.partyCode}
	} else {
		outputPlacement = &publicPlacement{}
	}
	k.outputs = make([]*graph.Tensor, len(broadcast.outputs))
	for idx, meta := range broadcast.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, outputPlacement)
		if err != nil {
			return fmt.Errorf("KernelBroadcastTo toEngineNodes: %w", err)
		}
		k.outputs[idx] = tensor
	}
	if err := builder.addOpBroadcastTo(k.shapeRef, k.scalars, k.outputs); err != nil {
		return fmt.Errorf("KernelBroadcastTo toEngineNodes: %w", err)
	}

	return nil
}

type KernelPrivateGroupAgg struct {
	partyCode string
	// PGSParties stores parties suitable for private-groupby secret-agg(PGS) algorithm
	// ref: https://github.com/secretflow/spu/pull/1312
	PGSParties map[int]string
	cost       float64

	// Bonded tensors in this kernel
	groupKeys          []*graph.Tensor
	aggArgs            []*graph.Tensor
	simpleCountOutputs []*graph.Tensor
	argFuncOutputs     []*graph.Tensor
}

func (k *KernelPrivateGroupAgg) Cost() float64 {
	return k.cost
}

func (k *KernelPrivateGroupAgg) String() string {
	return fmt.Sprintf("KernelPrivateGroupAgg(partyCode = %s, PGSParties = %v)", k.partyCode, k.PGSParties)
}

func (k *KernelPrivateGroupAgg) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	groupAgg, ok := node.(*OperatorGroupAgg)
	if !ok {
		return fmt.Errorf("KernelPrivateGroupAgg ensureTensorPlace: expect GroupAgg node, but got %T", node)
	}

	k.groupKeys, err = createPlacedTensors(builder, groupAgg.groupKeys, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateGroupAgg ensureTensorPlace: %w", err)
	}

	k.aggArgs = make([]*graph.Tensor, len(groupAgg.aggArgs))
	for idx, meta := range groupAgg.aggArgs {
		tensorParty := k.partyCode
		if party, ok := k.PGSParties[meta.ID]; ok {
			tensorParty = party
		}
		tensor, err := builder.getOrCreatePlacedTensor(meta, &privatePlacement{partyCode: tensorParty})
		if err != nil {
			return fmt.Errorf("KernelPrivateGroupAgg ensureTensorPlace: %w", err)
		}
		k.aggArgs[idx] = tensor
	}

	return nil
}

func (k *KernelPrivateGroupAgg) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	groupAgg, ok := node.(*OperatorGroupAgg)
	if !ok {
		return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: expect GroupAgg node, but got %T", node)
	}

	groupId := builder.tm.CreateUnbondedTensor("group_id", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{partyCode: k.partyCode})

	groupNum := builder.tm.CreateUnbondedTensor("group_num", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{partyCode: k.partyCode})

	if err := builder.addOpPrivateGroup(k.groupKeys, groupId, groupNum, k.partyCode); err != nil {
		return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
	}

	k.simpleCountOutputs = make([]*graph.Tensor, len(groupAgg.simpleCountOutputs))
	for idx := range groupAgg.simpleCountOutputs {
		output, err := builder.tm.CreateAndSetFirstTensor(groupAgg.simpleCountOutputs[idx], &privatePlacement{partyCode: k.partyCode})
		if err != nil {
			return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
		}
		k.simpleCountOutputs[idx] = output

		if err := builder.addOpPrivateGroupAgg(ast.AggFuncCount, operator.OpNameGroupCount, groupId, groupNum,
			[]*graph.Tensor{groupId}, []*graph.Tensor{k.simpleCountOutputs[idx]}, map[string]*graph.Attribute{}, k.partyCode); err != nil {
			return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
		}
	}

	k.argFuncOutputs = make([]*graph.Tensor, len(groupAgg.argFuncOutputs))
	for idx, aggFunc := range groupAgg.aggFuncsWithArg {
		output, err := builder.tm.CreateAndSetFirstTensor(groupAgg.argFuncOutputs[idx], &privatePlacement{partyCode: k.aggArgs[idx].OwnerPartyCode})
		if err != nil {
			return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
		}
		k.argFuncOutputs[idx] = output

		// TODO handle aggFunc in addOpPrivateGroupAgg
		switch aggFunc.Name {
		case ast.AggFuncCount:
			switch aggFunc.Mode {
			case aggregation.CompleteMode:
				if aggFunc.HasDistinct {
					if err := builder.addOpPrivateGroupAgg("count_distinct", operator.OpNameGroupCountDistinct, groupId, groupNum,
						[]*graph.Tensor{k.aggArgs[idx]}, []*graph.Tensor{k.argFuncOutputs[idx]}, map[string]*graph.Attribute{}, k.partyCode); err != nil {
						return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
					}
				} else {
					// simpleCount: aggFunc.Mode == aggregation.CompleteMode && !aggFunc.HasDistinct
					return fmt.Errorf("KernelPrivateGroupAgg: aggFuncs are not divided properly")
				}
			case aggregation.FinalMode:
				// In AggFunc Count with FinalMode, we need to sum the count of each group to get the final count
				if err := builder.addOpPrivateGroupAgg("final_count_sum", operator.OpNameGroupSum, groupId, groupNum,
					[]*graph.Tensor{k.aggArgs[idx]}, []*graph.Tensor{k.argFuncOutputs[idx]}, map[string]*graph.Attribute{}, k.partyCode); err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
			default:
				return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: unsupported aggregation mode: %v", aggFunc.Mode)
			}
		case ast.AggFuncFirstRow, ast.AggFuncMin, ast.AggFuncMax:
			if err := builder.addOpPrivateGroupAgg(aggFunc.Name, operator.GroupAggOp[aggFunc.Name], groupId, groupNum,
				[]*graph.Tensor{k.aggArgs[idx]}, []*graph.Tensor{k.argFuncOutputs[idx]}, map[string]*graph.Attribute{}, k.partyCode); err != nil {
				return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
			}
		case ast.AggPercentileDisc:
			if len(aggFunc.Args) != 2 {
				return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: AggPercentileDisc args length is not 2")
			}
			attrs := map[string]*graph.Attribute{}
			attr := &graph.Attribute{}
			percent, err := strconv.ParseFloat(aggFunc.Args[1].String(), 64)
			if err != nil {
				return fmt.Errorf("KernelPrivateGroupAgg: %s is not a valid float value", aggFunc.Args[1].String())
			}
			if percent < 0 || percent > 1 {
				return fmt.Errorf("KernelPrivateGroupAgg: percent should be in [0, 1], but got %v", percent)
			}
			attr.SetDouble(percent)
			attrs[operator.PercentAttr] = attr

			if err := builder.addOpPrivateGroupAgg(aggFunc.Name, operator.GroupAggOp[aggFunc.Name], groupId, groupNum,
				[]*graph.Tensor{k.aggArgs[idx]}, []*graph.Tensor{k.argFuncOutputs[idx]}, attrs, k.partyCode); err != nil {
				return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
			}

		case ast.AggFuncSum, ast.AggFuncAvg:
			if k.aggArgs[idx].OwnerPartyCode == k.partyCode {
				if err := builder.addOpPrivateGroupAgg(aggFunc.Name, operator.GroupAggOp[aggFunc.Name], groupId, groupNum,
					[]*graph.Tensor{k.aggArgs[idx]}, []*graph.Tensor{k.argFuncOutputs[idx]}, map[string]*graph.Attribute{}, k.partyCode); err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
			} else {
				// run secret agg for other party's tensor
				// 1. convert to shared tensor
				publicGroupNum, err := builder.convertStatus(groupNum, &publicPlacement{}, false)
				if err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
				secretGroupId, err := builder.convertStatus(groupId, &secretPlacement{}, false)
				if err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
				secretArg, err := builder.convertStatus(k.aggArgs[idx], &secretPlacement{}, false)
				if err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
				secretAgg := builder.tm.CreateTensorAs(secretArg)
				secretAgg.DType = inferAggOutputType(aggFunc, secretAgg.DType)
				secretAgg.Name = fmt.Sprintf("%s_%s", secretAgg.Name, aggFunc.Name)
				// 2. run secret sum
				if err := builder.addOpGroupSecretAgg(aggFunc.Name, secretGroupId, publicGroupNum, secretArg, secretAgg); err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
				}
				// 3. convert result to private result tensor
				attr := &graph.Attribute{}
				attr.SetString(k.argFuncOutputs[idx].OwnerPartyCode)
				if err := builder.addEngineNode("make_private", operator.OpNameMakePrivate, map[string][]*graph.Tensor{"In": {secretAgg}},
					map[string][]*graph.Tensor{"Out": {output}}, map[string]*graph.Attribute{operator.RevealToAttr: attr}, builder.GetAllParties()); err != nil {
					return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: make private failed: %v", err)
				}
			}
		default:
			return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: unsupported aggFunc: %s", aggFunc.Name)
		}

	}

	return nil
}

type KernelObliviousGroupAgg struct {
	privateSortParty string
	revealGroupMark  bool

	// Bonded tensors in this kernel
	groupKeys          []*graph.Tensor
	aggArgs            []*graph.Tensor
	simpleCountOutputs []*graph.Tensor
	argFuncOutputs     []*graph.Tensor
}

func (k *KernelObliviousGroupAgg) Cost() float64 {
	if k.privateSortParty == "" {
		// no private sort party, we need to sort the tensor in secret, which is very expensive
		if k.revealGroupMark {
			// reveal group mark can avoid shuffling
			return 0.95
		}
		return 1.0
	}

	if k.revealGroupMark {
		return 0.75
	}
	return 0.8
}

func (k *KernelObliviousGroupAgg) String() string {
	return fmt.Sprintf("KernelObliviousGroupAgg(privateSortParty = %s, revealGroupMark = %t)", k.privateSortParty, k.revealGroupMark)
}

func (k *KernelObliviousGroupAgg) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	groupAgg, ok := node.(*OperatorGroupAgg)
	if !ok {
		return fmt.Errorf("KernelObliviousGroupAgg ensureTensorPlace: expect GroupAgg node, but got %T", node)
	}

	k.groupKeys, err = createPlacedTensors(builder, groupAgg.groupKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg ensureTensorPlace: %w", err)
	}
	k.aggArgs, err = createPlacedTensors(builder, groupAgg.aggArgs, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelObliviousGroupAgg) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	groupAgg, ok := node.(*OperatorGroupAgg)
	if !ok {
		return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: expect GroupAgg node, but got %T", node)
	}

	// sort inputs by keys
	// FIXME different with buildObliviousGroupAggregation, which sort/shuffle then build expression.
	// Here we build expression before sort/shuffle.
	sortInputs := make([]*graph.Tensor, 0, len(k.groupKeys)+len(k.aggArgs))
	sortInputs = append(sortInputs, k.groupKeys...)
	sortInputs = append(sortInputs, k.aggArgs...)
	if k.privateSortParty != "" {
		shuffledInputs := make([]*graph.Tensor, 0, len(sortInputs))
		for _, input := range sortInputs {
			tensor := builder.tm.CreateTensorAs(input)
			shuffledInputs = append(shuffledInputs, tensor)
		}
		if err := builder.addOpShuffle(sortInputs, shuffledInputs); err != nil {
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
		}

		// privateSortParty is visible for key after groupby, key can reveal to privateSortParty in groupby.
		// which the safety is same with revealing to privateSortParty after groupby

		shuffledKeys := shuffledInputs[:len(k.groupKeys)]
		privateKeys := make([]*graph.Tensor, 0, len(shuffledKeys))
		for _, secretKey := range shuffledKeys {
			privateKey, err := builder.convertStatus(secretKey, &privatePlacement{partyCode: k.privateSortParty}, false)
			if err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
			}
			privateKeys = append(privateKeys, privateKey)
		}

		// convert to secret again to meet the status requirement of op sort
		secretKeys := make([]*graph.Tensor, 0, len(privateKeys))
		for _, privateKey := range privateKeys {
			secretKey, err := builder.convertStatus(privateKey, &secretPlacement{}, false)
			if err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
			}
			secretKeys = append(secretKeys, secretKey)
		}

		sortInputs = sortInputs[:0]
		sortInputs = append(sortInputs, secretKeys...)
		sortInputs = append(sortInputs, shuffledInputs[len(k.groupKeys):]...)
	}

	sortOutputs := make([]*graph.Tensor, 0, len(sortInputs))
	for _, input := range sortInputs {
		// TODO: check sorted tensor status
		tensor := builder.tm.CreateTensorAs(input)
		sortOutputs = append(sortOutputs, tensor)
	}
	sortKeys := sortInputs[:len(k.groupKeys)]
	if err := builder.addOpSort(sortKeys, sortInputs, sortOutputs, slices.Repeat([]bool{false}, len(sortKeys))); err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
	}
	sortedKeys := sortOutputs[:len(k.groupKeys)]
	sortedPayloads := sortOutputs[len(k.groupKeys):]

	// create group mark
	groupMark := builder.tm.CreateUnbondedTensor("group_mark", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), &secretPlacement{})
	if err := builder.addOpObliviousGroupMark(sortedKeys, groupMark); err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
	}

	// handle agg funcs
	aggregatedValues := make([]*graph.Tensor, 0, len(groupAgg.argFuncOutputs)+len(groupAgg.simpleCountOutputs))

	// FIXME simple count once and only
	for range groupAgg.simpleCountOutputs {
		aggregatedValue := builder.tm.CreateUnbondedTensor("aggregated_group_simple_count", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &secretPlacement{})
		aggregatedValues = append(aggregatedValues, aggregatedValue)
		if err := builder.addOpObliviousGroupAgg(ast.AggFuncCount, groupMark, groupMark, aggregatedValue, map[string]*graph.Attribute{}); err != nil {
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
		}
	}

	for idx, aggFunc := range groupAgg.aggFuncsWithArg {
		switch aggFunc.Name {
		case ast.AggFuncFirstRow:
			aggregatedValues = append(aggregatedValues, sortedPayloads[idx])
		case ast.AggPercentileDisc:
			// Sort the column along with group keys for percentile calculation
			keysForPercentileSort := make([]*graph.Tensor, 0, len(sortedKeys)+1)
			keysForPercentileSort = append(keysForPercentileSort, sortedKeys...)
			keysForPercentileSort = append(keysForPercentileSort, sortedPayloads[idx])

			sortedCol := builder.tm.CreateTensorAs(sortedPayloads[idx])
			sortedCol.Name = "sorted_percentile_" + sortedCol.Name
			sortOutputs := []*graph.Tensor{sortedCol}

			if err := builder.addOpSort(keysForPercentileSort, []*graph.Tensor{sortedPayloads[idx]}, sortOutputs, slices.Repeat([]bool{false}, len(keysForPercentileSort))); err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
			}

			// Parse and validate percentile value
			if len(aggFunc.Args) != 2 {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: AggPercentileDisc expects 2 args, got %d", len(aggFunc.Args))
			}

			percent, err := strconv.ParseFloat(aggFunc.Args[1].String(), 64)
			if err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %s is not a valid float value", aggFunc.Args[1].String())
			}

			if percent < 0 || percent > 1 {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: percent should be in [0, 1], but got %v", percent)
			}

			// Create aggregated value tensor with percentile
			aggregatedValue := builder.tm.CreateTensorAs(sortedPayloads[idx])
			aggregatedValue.DType = inferAggOutputType(aggFunc, aggregatedValue.DType)
			aggregatedValue.Name = fmt.Sprintf("%s_%s", aggregatedValue.Name, aggFunc.Name)
			aggregatedValue.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
			aggregatedValues = append(aggregatedValues, aggregatedValue)

			// Create attribute for percentile
			attrs := map[string]*graph.Attribute{}
			attr := &graph.Attribute{}
			attr.SetDouble(percent)
			attrs[operator.PercentAttr] = attr

			// Add oblivious group aggregation with percentile
			if err := builder.addOpObliviousGroupAgg(aggFunc.Name, groupMark, sortedCol, aggregatedValue, attrs); err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
			}

		case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncAvg:
			aggregatedValue := builder.tm.CreateTensorAs(sortedPayloads[idx])
			aggregatedValue.DType = inferAggOutputType(aggFunc, aggregatedValue.DType)
			aggregatedValue.Name = fmt.Sprintf("%s_%s", aggregatedValue.Name, aggFunc.Name)
			aggregatedValue.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
			aggregatedValues = append(aggregatedValues, aggregatedValue)
			if err := builder.addOpObliviousGroupAgg(aggFunc.Name, groupMark, sortedPayloads[idx], aggregatedValue, map[string]*graph.Attribute{}); err != nil {
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
			}
		case ast.AggFuncCount:
			// NOTE(yang.y): There are two mode for count function.
			// - The CompleteMode is the default mode in queries like `select count(*) from t`.
			//   In this mode, count function should be translated to ObliviousGroupCount.
			// - The FinalMode appears at `select count(*) from (t1 union all t2)`.
			//   The aggregation push down optimizer will rewrite the query plan to
			//   `select count_final(*) from (select count(*) from t1 union all select count(*) from t2)`.
			//   In this mode, count function will be translated to ObliviousGroupSum.
			aggregatedValue := builder.tm.CreateTensorAs(sortedPayloads[idx])
			aggregatedValue.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
			aggregatedValue.Name = fmt.Sprintf("%s_%s", aggregatedValue.Name, aggFunc.Name)
			aggregatedValue.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
			aggregatedValues = append(aggregatedValues, aggregatedValue)
			switch aggFunc.Mode {
			case aggregation.CompleteMode:
				// do complete count
				if aggFunc.HasDistinct {
					// Sort with group by key(maybe keyTs or groupIds) and distinct column.
					// Please note that group by key is the major sort key,
					// so the groupMark is still valid.
					keyAndDistinct := make([]*graph.Tensor, 0, len(sortedKeys)+1)
					keyAndDistinct = append(keyAndDistinct, sortedKeys...)
					keyAndDistinct = append(keyAndDistinct, sortedPayloads[idx])

					sortedDistinctCol := builder.tm.CreateTensorAs(sortedPayloads[idx])
					if err := builder.addOpSort(keyAndDistinct, []*graph.Tensor{sortedPayloads[idx]}, []*graph.Tensor{sortedDistinctCol}, slices.Repeat([]bool{false}, len(keyAndDistinct))); err != nil {
						return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
					}

					// Use group by keys and distinctCol to create groupMarkFull,
					// which is equivalent to the result of groupMark logic or groupMarkDistinct
					groupMarkDistinct := builder.tm.CreateUnbondedTensor("group_mark_distinct", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), &secretPlacement{})
					if err := builder.addOpObliviousGroupMark([]*graph.Tensor{sortedDistinctCol}, groupMarkDistinct); err != nil {
						return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
					}

					groupMarkFull := builder.tm.CreateTensorAs(groupMarkDistinct)
					groupMarkFull.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
					groupMarkFull.Name = "group_mark_full"
					if err := builder.addOpBinaryFunc(ast.LogicOr, groupMark, groupMarkDistinct, groupMarkFull); err != nil {
						return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
					}

					if err := builder.addOpObliviousGroupAgg(ast.AggFuncSum, groupMark, groupMarkFull, aggregatedValue, map[string]*graph.Attribute{}); err != nil {
						return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
					}
				} else {
					// simpleCount: aggFunc.Mode == aggregation.CompleteMode && !aggFunc.HasDistinct
					return fmt.Errorf("KernelObliviousGroupAgg: aggFuncs are not divided properly")
				}
			case aggregation.FinalMode:
				// sum up partial count
				if err := builder.addOpObliviousGroupAgg(ast.AggFuncSum, groupMark, sortedPayloads[idx], aggregatedValue, map[string]*graph.Attribute{}); err != nil {
					return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
				}

			default:
				return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: unsupported agg func mode %v", aggFunc.Mode)
			}
		default:
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: unsupported agg func %s", aggFunc.Name)
		}
	}

	// TODO(jingshi): temporary remove shuffle here for simplicity, make group_mark public and support aggregation with public group_mark later for efficiency
	if !k.revealGroupMark {
		valueAndMark := make([]*graph.Tensor, 0, len(aggregatedValues)+1)
		valueAndMark = append(valueAndMark, aggregatedValues...)
		valueAndMark = append(valueAndMark, groupMark)

		shuffledValueAndMark := make([]*graph.Tensor, 0, len(aggregatedValues)+1)
		for _, tensor := range valueAndMark {
			shuffled := builder.tm.CreateTensorAs(tensor)
			shuffledValueAndMark = append(shuffledValueAndMark, shuffled)
		}
		if err := builder.addOpShuffle(valueAndMark, shuffledValueAndMark); err != nil {
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
		}
		aggregatedValues = shuffledValueAndMark[:len(aggregatedValues)]
		groupMark = shuffledValueAndMark[len(shuffledValueAndMark)-1]
	}

	groupMarkPub, err := builder.convertStatus(groupMark, &publicPlacement{}, false)
	if err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
	}

	// filter to only keep group aggregated values
	filtered := make([]*graph.Tensor, 0, len(aggregatedValues))
	k.simpleCountOutputs = make([]*graph.Tensor, 0, len(groupAgg.simpleCountOutputs))
	for idx, meta := range groupAgg.simpleCountOutputs {
		placement := extractTensorPlacement(aggregatedValues[idx])
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, placement)
		if err != nil {
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
		}
		filtered = append(filtered, tensor)
		k.simpleCountOutputs = append(k.simpleCountOutputs, tensor)
	}
	k.argFuncOutputs = make([]*graph.Tensor, 0, len(groupAgg.argFuncOutputs))
	for idx, meta := range groupAgg.argFuncOutputs {
		placement := extractTensorPlacement(aggregatedValues[idx+len(groupAgg.simpleCountOutputs)])
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, placement)
		if err != nil {
			return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
		}
		filtered = append(filtered, tensor)
		k.argFuncOutputs = append(k.argFuncOutputs, tensor)
	}
	if err := builder.addOpFilter(groupMarkPub, aggregatedValues, filtered, builder.GetAllParties()); err != nil {
		return fmt.Errorf("KernelObliviousGroupAgg toEngineNodes: %w", err)
	}
	return nil
}

type KernelCrossJoin struct {
	leftParty, rightParty string

	// Bonded tensors in this kernel
	leftInputs   []*graph.Tensor
	rightInputs  []*graph.Tensor
	leftOutputs  []*graph.Tensor
	rightOutputs []*graph.Tensor
}

func (k *KernelCrossJoin) Cost() float64 {
	// currently, there is only one kernel for cross join
	return 0.0
}

func (k *KernelCrossJoin) String() string {
	return fmt.Sprintf("KernelCrossJoin(leftParty: %s, rightParty: %s)", k.leftParty, k.rightParty)
}

func (k *KernelCrossJoin) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	crossJoin, ok := node.(*OperatorCrossJoin)
	if !ok {
		return fmt.Errorf("KernelCrossJoin ensureTensorPlace: expect CrossJoin node, but got %T", node)
	}

	k.leftInputs, err = createPlacedTensors(builder, crossJoin.leftInputs, &privatePlacement{partyCode: k.leftParty})
	if err != nil {
		return fmt.Errorf("KernelCrossJoin ensureTensorPlace: %w", err)
	}

	k.rightInputs, err = createPlacedTensors(builder, crossJoin.rightInputs, &privatePlacement{partyCode: k.rightParty})
	if err != nil {
		return fmt.Errorf("KernelCrossJoin ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelCrossJoin) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	crossJoin, ok := node.(*OperatorCrossJoin)
	if !ok {
		return fmt.Errorf("KernelCrossJoin toEngineNodes: expect CrossJoin node, but got %T", node)
	}

	k.leftOutputs = make([]*graph.Tensor, len(crossJoin.leftOutputs))
	for idx, meta := range crossJoin.leftOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: k.leftParty})
		if err != nil {
			return fmt.Errorf("KernelCrossJoin toEngineNodes: %w", err)
		}
		k.leftOutputs[idx] = tensor
	}

	k.rightOutputs = make([]*graph.Tensor, len(crossJoin.rightOutputs))
	for idx, meta := range crossJoin.rightOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: k.rightParty})
		if err != nil {
			return fmt.Errorf("KernelCrossJoin toEngineNodes: %w", err)
		}
		k.rightOutputs[idx] = tensor
	}

	if err := builder.addOpReplicate(k.leftInputs, k.rightInputs, k.leftOutputs, k.rightOutputs, []string{k.leftParty, k.rightParty}); err != nil {
		return fmt.Errorf("KernelCrossJoin toEngineNodes: %w", err)
	}

	return nil
}

type KernelLimit struct {
	// Bonded tensors in this kernel
	inputs  []*graph.Tensor
	outputs []*graph.Tensor
}

func (k *KernelLimit) Cost() float64 {
	// the efficiency of applying the limit is similar across various statuses
	return 0.0
}

func (k *KernelLimit) String() string {
	return "KernelLimit"
}

func (k *KernelLimit) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	limit, ok := node.(*OperatorLimit)
	if !ok {
		return fmt.Errorf("KernelLimit ensureTensorPlace: expect Limit node, but got %T", node)
	}

	k.inputs = make([]*graph.Tensor, len(limit.inputs))
	for idx, meta := range limit.inputs {
		tensor, err := builder.tm.getOnePlacedTensor(meta)
		if err != nil {
			return fmt.Errorf("KernelLimit ensureTensorPlace: %w", err)
		}
		k.inputs[idx] = tensor
	}

	return nil
}

func (k *KernelLimit) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	limit, ok := node.(*OperatorLimit)
	if !ok {
		return fmt.Errorf("KernelLimit toEngineNodes: expect Limit node, but got %T", node)
	}

	// Create output placed tensors and store in kernel
	k.outputs = make([]*graph.Tensor, len(limit.outputs))
	var allPartyInputs []*graph.Tensor
	var allPartyOutputs []*graph.Tensor
	privateInputs := make(map[string][]*graph.Tensor)
	privateOutputs := make(map[string][]*graph.Tensor)
	for idx, meta := range limit.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, extractTensorPlacement(k.inputs[idx]))
		if err != nil {
			return fmt.Errorf("KernelLimit toEngineNodes: %w", err)
		}
		k.outputs[idx] = tensor

		// divide input/output tensor into groups to handle different parties
		switch tensor.Status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			privateInputs[tensor.OwnerPartyCode] = append(privateInputs[tensor.OwnerPartyCode], k.inputs[idx])
			privateOutputs[tensor.OwnerPartyCode] = append(privateOutputs[tensor.OwnerPartyCode], tensor)
		case proto.TensorStatus_TENSORSTATUS_PUBLIC, proto.TensorStatus_TENSORSTATUS_SECRET:
			allPartyInputs = append(allPartyInputs, k.inputs[idx])
			allPartyOutputs = append(allPartyOutputs, k.outputs[idx])
		default:
			return fmt.Errorf("KernelLimit toEngineNodes: unexpected tensor status %s", tensor.Status())
		}
	}

	if len(allPartyInputs) != 0 {
		if err := builder.addOpLimit(allPartyInputs, allPartyOutputs, int64(limit.offset), int64(limit.count), builder.GetAllParties()); err != nil {
			return fmt.Errorf("KernelLimit toEngineNodes: %w", err)
		}
	}
	for party, inputs := range sliceutil.SortedMap(privateInputs) {
		if err := builder.addOpLimit(inputs, privateOutputs[party], int64(limit.offset), int64(limit.count), []string{party}); err != nil {
			return fmt.Errorf("KernelLimit toEngineNodes: %w", err)
		}
	}

	return nil
}

type KernelFilter struct {
	inputPlacements []tensorPlacement

	// Bonded tensors in this kernel
	inputs  []*graph.Tensor
	outputs []*graph.Tensor
}

func (k *KernelFilter) Cost() float64 {
	// TODO: benchmark the cost of filter
	return 0.0
}

func (k *KernelFilter) String() string {
	return "KernelFilter"
}

func (k *KernelFilter) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	filter, ok := node.(*OperatorFilter)
	if !ok {
		return fmt.Errorf("KernelFilter ensureTensorPlace: expect Filter node, but got %T", node)
	}

	k.inputs = make([]*graph.Tensor, len(filter.inputs))
	for idx, meta := range filter.inputs {
		tensor, err := builder.getOrCreatePlacedTensor(meta, k.inputPlacements[idx])
		if err != nil {
			return fmt.Errorf("KernelFilter ensureTensorPlace: %w", err)
		}
		k.inputs[idx] = tensor
	}

	return nil
}

func (k *KernelFilter) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	filter, ok := node.(*OperatorFilter)
	if !ok {
		return fmt.Errorf("KernelFilter toEngineNodes: expect Filter node, but got %T", node)
	}

	k.outputs = make([]*graph.Tensor, len(filter.outputs))
	for idx, meta := range filter.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, extractTensorPlacement(k.inputs[idx]))
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}
		k.outputs[idx] = tensor
	}

	partyIndexes := make(map[string][]int)
	secretIndexes := make([]int, 0)
	for idx, input := range k.inputs {
		if input.Status() == proto.TensorStatus_TENSORSTATUS_SECRET {
			secretIndexes = append(secretIndexes, idx)
		} else if input.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyIndexes[input.OwnerPartyCode] = append(partyIndexes[input.OwnerPartyCode], idx)
		}
	}

	for party, indexes := range sliceutil.SortedMap(partyIndexes) {
		sort.Slice(indexes, func(i, j int) bool { return k.inputs[indexes[i]].ID < k.inputs[indexes[j]].ID })

		privateInputs, err := sliceutil.TakeByIndices(k.inputs, indexes)
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}
		privateOutputs, err := sliceutil.TakeByIndices(k.outputs, indexes)
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}

		privateMask, err := builder.getOrCreatePlacedTensor(filter.mask, &privatePlacement{partyCode: party})
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}

		if err := builder.addOpFilter(privateMask, privateInputs, privateOutputs, []string{party}); err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}
	}

	if len(secretIndexes) > 0 {
		secretInputs, err := sliceutil.TakeByIndices(k.inputs, secretIndexes)
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}
		secretOutputs, err := sliceutil.TakeByIndices(k.outputs, secretIndexes)
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}

		publicMask, err := builder.getOrCreatePlacedTensor(filter.mask, &publicPlacement{})
		if err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}

		if err := builder.addOpFilter(publicMask, secretInputs, secretOutputs, builder.GetAllParties()); err != nil {
			return fmt.Errorf("KernelFilter toEngineNodes: %w", err)
		}
	}

	return nil
}

type KernelSecretConcat struct {
	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelSecretConcat) Cost() float64 {
	// only one kernel for concat for now
	return 0.0
}

func (k *KernelSecretConcat) String() string {
	return "KernelSecretConcat"
}

func (k *KernelSecretConcat) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	concat, ok := node.(*OperatorConcat)
	if !ok {
		return fmt.Errorf("KernelSecretConcat ensureTensorPlace: expect Concat node, but got %T", node)
	}

	k.inputs, err = createPlacedTensors(builder, concat.inputs, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretConcat ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelSecretConcat) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	concat, ok := node.(*OperatorConcat)
	if !ok {
		return fmt.Errorf("KernelSecretConcat toEngineNodes: expect Concat node, but got %T", node)
	}

	tensor, err := builder.tm.CreateAndSetFirstTensor(concat.output, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretConcat toEngineNodes: %w", err)
	}
	for _, input := range k.inputs {
		tensor.SecretStringOwners = append(tensor.SecretStringOwners, input.SecretStringOwners...)
	}
	k.output = tensor

	if err := builder.addOpConcat(k.inputs, k.output); err != nil {
		return fmt.Errorf("KernelSecretConcat toEngineNodes: %w", err)
	}

	return nil
}

type KernelSecretSort struct {
	// Bonded tensors in this kernel
	sortKeys []*graph.Tensor
	payloads []*graph.Tensor
	outputs  []*graph.Tensor
}

func (k *KernelSecretSort) Cost() float64 {
	// secret sort is very expensive
	return 1.0
}

func (k *KernelSecretSort) String() string {
	return "KernelSecretSort"
}

func (k *KernelSecretSort) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	sort, ok := node.(*OperatorSort)
	if !ok {
		return fmt.Errorf("KernelSecretSort ensureTensorPlace: expect Sort node, but got %T", node)
	}

	k.sortKeys, err = createPlacedTensors(builder, sort.sortKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretSort ensureTensorPlace: %w", err)
	}

	k.payloads, err = createPlacedTensors(builder, sort.payloads, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelSecretSort ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelSecretSort) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	sort, ok := node.(*OperatorSort)
	if !ok {
		return fmt.Errorf("KernelSecretSort toEngineNodes: expect Sort node, but got %T", node)
	}

	k.outputs = make([]*graph.Tensor, len(sort.outputs))
	for idx, meta := range sort.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
		if err != nil {
			return fmt.Errorf("KernelSecretSort toEngineNodes: %w", err)
		}
		k.outputs[idx] = tensor
	}

	if err := builder.addOpSort(k.sortKeys, k.payloads, k.outputs, sort.descending); err != nil {
		return fmt.Errorf("KernelSecretSort toEngineNodes: %w", err)
	}

	return nil
}

type KernelPrivateSort struct {
	partyCode string

	// Bonded tensors in this kernel
	sortKeys []*graph.Tensor
	payloads []*graph.Tensor
	outputs  []*graph.Tensor
}

func (k *KernelPrivateSort) Cost() float64 {
	// chepeast sort kernel
	return 0.0
}

func (k *KernelPrivateSort) String() string {
	return fmt.Sprintf("KernelPrivateSort(partyCode: %s)", k.partyCode)
}

func (k *KernelPrivateSort) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	sort, ok := node.(*OperatorSort)
	if !ok {
		return fmt.Errorf("KernelPrivateSort ensureTensorPlace: expect Sort node, but got %T", node)
	}

	k.sortKeys, err = createPlacedTensors(builder, sort.sortKeys, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateSort ensureTensorPlace: %w", err)
	}

	k.payloads, err = createPlacedTensors(builder, sort.payloads, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateSort ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelPrivateSort) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	sort, ok := node.(*OperatorSort)
	if !ok {
		return fmt.Errorf("KernelPrivateSort toEngineNodes: expect Sort node, but got %T", node)
	}

	k.outputs = make([]*graph.Tensor, len(sort.outputs))
	for idx, meta := range sort.outputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: k.partyCode})
		if err != nil {
			return fmt.Errorf("KernelPrivateSort toEngineNodes: %w", err)
		}
		k.outputs[idx] = tensor
	}

	if err := builder.addOpSort(k.sortKeys, k.payloads, k.outputs, sort.descending); err != nil {
		return fmt.Errorf("KernelPrivateSort toEngineNodes: %w", err)
	}

	return nil
}

type KernelConstant struct {
}

func (k *KernelConstant) Cost() float64 {
	// always emit public tensor
	return 0.0
}

func (k *KernelConstant) String() string {
	return "KernelConstant"
}

func (k *KernelConstant) ensureTensorPlace(_ *ExecutionGraphBuilder, _ Operator) (err error) {
	return nil
}

func (k *KernelConstant) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	constant, ok := node.(*OperatorConstant)
	if !ok {
		return fmt.Errorf("KernelConstant toEngineNodes: expect Constant node, but got %T", node)
	}

	tensor, err := builder.tm.CreateAndSetFirstTensor(constant.output, &publicPlacement{})
	if err != nil {
		return fmt.Errorf("KernelConstant toEngineNodes: %w", err)
	}

	if err := builder.addOpConstant(constant.value, tensor); err != nil {
		return fmt.Errorf("KernelConstant toEngineNodes: %w", err)
	}

	return nil
}

type KernelLocalIn struct {
	partyCode string

	// Bonded tensors in this kernel
	left   *graph.Tensor
	right  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelLocalIn) Cost() float64 {
	// local in is cheapest kernel for in
	return 0.0
}

func (k *KernelLocalIn) String() string {
	return fmt.Sprintf("KernelLocalIn(partyCode: %s)", k.partyCode)
}

func (k *KernelLocalIn) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	in, ok := node.(*OperatorIn)
	if !ok {
		return fmt.Errorf("KernelLocalIn ensureTensorPlace: expect In node, but got %T", node)
	}

	leftTensor, err := builder.getOrCreatePlacedTensor(in.left, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelLocalIn ensureTensorPlace: %w", err)
	}
	k.left = leftTensor

	rightTensor, err := builder.getOrCreatePlacedTensor(in.right, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelLocalIn ensureTensorPlace: %w", err)
	}
	k.right = rightTensor

	return nil
}

func (k *KernelLocalIn) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	in, ok := node.(*OperatorIn)
	if !ok {
		return fmt.Errorf("KernelLocalIn toEngineNodes: expect In node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(in.output, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelLocalIn toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpIn(k.left, k.right, k.output, proto.PsiAlgorithmType_AUTO); err != nil {
		return fmt.Errorf("KernelLocalIn toEngineNodes: %w", err)
	}

	return nil
}

type KernelPsiIn struct {
	leftParty        string
	rightParty       string
	psiAlgorithmType proto.PsiAlgorithmType

	// Bonded tensors in this kernel
	left   *graph.Tensor
	right  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelPsiIn) Cost() float64 {
	// psi in is more expensive than local in but cheaper than secret in (which will be implemented in the future)
	return 0.5
}

func (k *KernelPsiIn) String() string {
	return fmt.Sprintf("KernelPsiIn(leftParty: %s, rightParty: %s, psiAlgorithmType: %v)", k.leftParty, k.rightParty, k.psiAlgorithmType)
}

func (k *KernelPsiIn) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	in, ok := node.(*OperatorIn)
	if !ok {
		return fmt.Errorf("KernelPsiIn ensureTensorPlace: expect In node, but got %T", node)
	}

	leftTensor, err := builder.getOrCreatePlacedTensor(in.left, &privatePlacement{k.leftParty})
	if err != nil {
		return fmt.Errorf("KernelPsiIn ensureTensorPlace: %w", err)
	}
	k.left = leftTensor

	rightTensor, err := builder.getOrCreatePlacedTensor(in.right, &privatePlacement{k.rightParty})
	if err != nil {
		return fmt.Errorf("KernelPsiIn ensureTensorPlace: %w", err)
	}
	k.right = rightTensor

	return nil
}

func (k *KernelPsiIn) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	in, ok := node.(*OperatorIn)
	if !ok {
		return fmt.Errorf("KernelPsiIn toEngineNodes: expect In node, but got %T", node)
	}

	// TODO condider reveal party
	outputTensor, err := builder.tm.CreateAndSetFirstTensor(in.output, &privatePlacement{k.leftParty})
	if err != nil {
		return fmt.Errorf("KernelPsiIn toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpIn(k.left, k.right, k.output, k.psiAlgorithmType); err != nil {
		return fmt.Errorf("KernelPsiIn toEngineNodes: %w", err)
	}

	return nil
}

type KernelArrowFunction struct {
	partyCode string

	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelArrowFunction) Cost() float64 {
	// Arrow function is always executed under private placement
	return 0.0
}

func (k *KernelArrowFunction) String() string {
	return fmt.Sprintf("KernelArrowFunction(partyCode: %s)", k.partyCode)
}

func (k *KernelArrowFunction) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelArrowFunction ensureTensorPlace: expect Function node, but got %T", node)
	}

	k.inputs, err = createPlacedTensors(builder, function.inputs, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelArrowFunction ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelArrowFunction) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelArrowFunction toEngineNodes: expect ArrowFunction node, but got %T", node)
	}

	tensor, err := builder.tm.CreateAndSetFirstTensor(function.output, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelArrowFunction toEngineNodes: %w", err)
	}
	k.output = tensor

	var arrowFuncName string
	var funcOpt compute.FunctionOptions
	switch function.funcName {
	case ast.Trim:
		arrowFuncName = "utf8_trim"
		funcOpt = TrimOptions{Characters: " "}
	case ast.Lower:
		arrowFuncName = "utf8_lower"
	case ast.Upper:
		arrowFuncName = "utf8_upper"
	case ast.Substr, ast.Substring:
		arrowFuncName = "utf8_slice_codeunits"
		opt := SliceOptions{
			Start: 0,
			Stop:  math.MaxInt64,
			Step:  1,
		}
		if len(function.constParams) >= 1 {
			opt.Start = function.constParams[0].Value.GetInt64() - 1
		}
		if len(function.constParams) >= 2 {
			length := function.constParams[1].Value.GetInt64()
			opt.Stop = opt.Start + length
		}
		funcOpt = opt
	case ast.StrToDate:
		arrowFuncName = "strptime"
		formatStr := function.constParams[0].Value.GetString()
		arrowFormatStr, err := stringutil.MySQLDateFormatToArrowFormat(formatStr)
		if err != nil {
			return fmt.Errorf("KernelArrowFunction toEngineNodes: format string '%s' is invalid: %w", formatStr, err)
		}
		funcOpt = &StrptimeOptions{
			Format:      arrowFormatStr,
			Unit:        arrow.Second,
			ErrorIsNull: true,
		}
	default:
		return fmt.Errorf("KernelArrowFunction toEngineNodes: unsupported function %s", function.funcName)
	}
	if err := builder.addOpArrowFunc(arrowFuncName, funcOpt, k.inputs, k.output); err != nil {
		return fmt.Errorf("KernelArrowFunction toEngineNodes: %w", err)
	}

	return nil
}

type KernelUnary struct {
	place tensorPlacement

	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelUnary) Cost() float64 {
	if IsSecret(k.place) {
		return 1.0
	}
	return 0.0
}

func (k *KernelUnary) String() string {
	return fmt.Sprintf("KernelUnary(place: %s)", k.place)
}

func (k *KernelUnary) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelUnary ensureTensorPlace: expect Function node, but got %T", node)
	}

	inputTensor, err := builder.getOrCreatePlacedTensor(function.inputs[0], k.place)
	if err != nil {
		return fmt.Errorf("KernelUnary ensureTensorPlace: %w", err)
	}
	k.input = inputTensor

	return nil
}

func (k *KernelUnary) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelUnary toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, extractTensorPlacement(k.input))
	if err != nil {
		return fmt.Errorf("KernelUnary toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpBasicFunc(function.funcName, []*graph.Tensor{k.input}, k.output); err != nil {
		return fmt.Errorf("KernelUnary toEngineNodes: %w", err)
	}

	return nil
}

type KernelBinary struct {
	lhsPlace, rhsPlace tensorPlacement
	outPlace           tensorPlacement

	// Bonded tensors in this kernel
	lhs    *graph.Tensor
	rhs    *graph.Tensor
	output *graph.Tensor
}

func (k *KernelBinary) Cost() float64 {
	inputSecretNum := 0
	if IsSecret(k.lhsPlace) {
		inputSecretNum++
	}
	if IsSecret(k.rhsPlace) {
		inputSecretNum++
	}
	return float64(inputSecretNum) / 2.0
}

func (k *KernelBinary) String() string {
	return fmt.Sprintf("KernelBinary(lhs: %s, rhs: %s, out: %s)", k.lhsPlace, k.rhsPlace, k.outPlace)
}

func (k *KernelBinary) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelBinary ensureTensorPlace: expect Function node, but got %T", node)
	}

	lhsTensor, err := builder.getOrCreatePlacedTensor(function.inputs[0], k.lhsPlace)
	if err != nil {
		return fmt.Errorf("KernelBinary ensureTensorPlace: %w", err)
	}
	k.lhs = lhsTensor

	rhsTensor, err := builder.getOrCreatePlacedTensor(function.inputs[1], k.rhsPlace)
	if err != nil {
		return fmt.Errorf("KernelBinary ensureTensorPlace: %w", err)
	}
	k.rhs = rhsTensor

	return nil
}

func (k *KernelBinary) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelBinary toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, k.outPlace)
	if err != nil {
		return fmt.Errorf("KernelBinary toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpBinaryFunc(function.funcName, k.lhs, k.rhs, k.output); err != nil {
		return fmt.Errorf("KernelBinary toEngineNodes: %w", err)
	}

	return nil
}

type KernelVariadicCompare struct {
	placement tensorPlacement

	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelVariadicCompare) Cost() float64 {
	if IsSecret(k.placement) {
		return 1.0
	}
	return 0.0
}

func (k *KernelVariadicCompare) String() string {
	return fmt.Sprintf("KernelVariadicCompare(placement: %s)", k.placement)
}

func (k *KernelVariadicCompare) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelVariadicCompare ensureTensorPlace: expect Function node, but got %T", node)
	}

	k.inputs, err = createPlacedTensors(builder, function.inputs, k.placement)
	if err != nil {
		return fmt.Errorf("KernelVariadicCompare ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelVariadicCompare) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelVariadicCompare toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, k.placement)
	if err != nil {
		return fmt.Errorf("KernelVariadicCompare toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpBasicFunc(function.funcName, k.inputs, k.output); err != nil {
		return fmt.Errorf("KernelVariadicCompare toEngineNodes: %w", err)
	}

	return nil
}

type KernelPrivateFunc struct {
	partyCode string

	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelPrivateFunc) Cost() float64 {
	return 0.0
}

func (k *KernelPrivateFunc) String() string {
	return fmt.Sprintf("KernelPrivateFunc(partyCode: %s)", k.partyCode)
}

func (k *KernelPrivateFunc) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelPrivateFunc ensureTensorPlace: expect Function node, but got %T", node)
	}

	k.inputs, err = createPlacedTensors(builder, function.inputs, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateFunc ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelPrivateFunc) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelPrivateFunc toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateFunc toEngineNodes: %w", err)
	}
	k.output = outputTensor

	switch function.funcName {
	case ast.Ifnull:
		if err := builder.addOpIfNull(k.inputs[0], k.inputs[1], k.output); err != nil {
			return fmt.Errorf("KernelPrivateFunc toEngineNodes: %w", err)
		}
	case ast.IsNull:
		if err := builder.addOpBasicFunc(function.funcName, k.inputs, k.output); err != nil {
			return fmt.Errorf("KernelPrivateFunc toEngineNodes: %w", err)
		}
	case ast.Coalesce:
		if err := builder.addOpCoalece(k.inputs, k.output); err != nil {
			return fmt.Errorf("KernelPrivateFunc toEngineNodes: %w", err)
		}
	default:
		return fmt.Errorf("KernelPrivateFunc toEngineNodes: not support function %s", function.funcName)
	}

	return nil
}

type KernelCast struct {
	placement tensorPlacement

	// Bonded tensors in this kernel
	input  *graph.Tensor
	output *graph.Tensor
}

func (k *KernelCast) Cost() float64 {
	if IsSecret(k.placement) {
		return 1.0
	}
	return 0.0
}

func (k *KernelCast) String() string {
	return fmt.Sprintf("KernelCast(placement: %s)", k.placement)
}

func (k *KernelCast) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelCast ensureTensorPlace: expect Function node, but got %T", node)
	}

	tensor, err := builder.getOrCreatePlacedTensor(function.inputs[0], k.placement)
	if err != nil {
		return fmt.Errorf("KernelCast ensureTensorPlace: %w", err)
	}
	k.input = tensor

	return nil
}

func (k *KernelCast) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelCast toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, k.placement)
	if err != nil {
		return fmt.Errorf("KernelCast toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpBasicFunc(function.funcName, []*graph.Tensor{k.input}, k.output); err != nil {
		return fmt.Errorf("KernelCast toEngineNodes: %w", err)
	}

	return nil
}

type KernelIf struct {
	condPlace       tensorPlacement
	trueValuePlace  tensorPlacement
	falseValuePlace tensorPlacement
	outputPlace     tensorPlacement

	// Bonded tensors in this kernel
	cond       *graph.Tensor
	trueValue  *graph.Tensor
	falseValue *graph.Tensor
	output     *graph.Tensor
}

func (k *KernelIf) Cost() float64 {
	inputSecretNum := 0
	if IsSecret(k.condPlace) {
		inputSecretNum++
	}
	if IsSecret(k.trueValuePlace) {
		inputSecretNum++
	}
	if IsSecret(k.falseValuePlace) {
		inputSecretNum++
	}
	return float64(inputSecretNum) / 3.0
}

func (k *KernelIf) String() string {
	return fmt.Sprintf("KernelIf(condPlace: %s, trueValuePlace: %s, falseValuePlace: %s, outputPlace: %s)", k.condPlace, k.trueValuePlace, k.falseValuePlace, k.outputPlace)
}

func (k *KernelIf) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelIf ensureTensorPlace: expect Function node, but got %T", node)
	}

	condTensor, err := builder.getOrCreatePlacedTensor(function.inputs[0], k.condPlace)
	if err != nil {
		return fmt.Errorf("KernelIf ensureTensorPlace: %w", err)
	}
	k.cond = condTensor

	trueValueTensor, err := builder.getOrCreatePlacedTensor(function.inputs[1], k.trueValuePlace)
	if err != nil {
		return fmt.Errorf("KernelIf ensureTensorPlace: %w", err)
	}
	k.trueValue = trueValueTensor

	falseValueTensor, err := builder.getOrCreatePlacedTensor(function.inputs[2], k.falseValuePlace)
	if err != nil {
		return fmt.Errorf("KernelIf ensureTensorPlace: %w", err)
	}
	k.falseValue = falseValueTensor

	return nil
}

func (k *KernelIf) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelIf toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, k.outputPlace)
	if err != nil {
		return fmt.Errorf("KernelIf toEngineNodes: %w", err)
	}
	k.output = outputTensor

	if err := builder.addOpIf(k.cond, k.trueValue, k.falseValue, k.output); err != nil {
		return fmt.Errorf("KernelIf toEngineNodes: %w", err)
	}

	return nil
}

type KernelCase struct {
	inputPlacements []tensorPlacement
	outputPlacement tensorPlacement

	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelCase) Cost() float64 {
	inputSecretNum := 0
	if IsSecret(k.outputPlacement) {
		inputSecretNum++
	}
	return float64(inputSecretNum) / float64(len(k.inputPlacements))
}

func (k *KernelCase) String() string {
	return fmt.Sprintf("KernelCase(inputPlacements: %s, outputPlacement: %s)", k.inputPlacements, k.outputPlacement)
}

func (k *KernelCase) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelCase ensureTensorPlace: expect Function node, but got %T", node)
	}

	if len(function.inputs) != len(k.inputPlacements) {
		return fmt.Errorf("KernelCase ensureTensorPlace: input placements info does not match input tensors")
	}

	k.inputs = make([]*graph.Tensor, len(function.inputs))
	for idx, meta := range function.inputs {
		tensor, err := builder.getOrCreatePlacedTensor(meta, k.inputPlacements[idx])
		if err != nil {
			return fmt.Errorf("KernelCase ensureTensorPlace: %w", err)
		}
		k.inputs[idx] = tensor
	}

	return nil
}

func (k *KernelCase) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelCase toEngineNodes: expect Function node, but got %T", node)
	}

	outputTensor, err := builder.tm.CreateAndSetFirstTensor(function.output, k.outputPlacement)
	if err != nil {
		return fmt.Errorf("KernelCase toEngineNodes: %w", err)
	}
	k.output = outputTensor

	conditions := make([]*graph.Tensor, 0, len(k.inputs)/2)
	values := make([]*graph.Tensor, 0, len(k.inputs)/2)
	for i := range len(k.inputs) / 2 {
		conditions = append(conditions, k.inputs[i*2])
		values = append(values, k.inputs[i*2+1])
	}
	if err := builder.addOpCaseWhen(conditions, values, k.inputs[len(k.inputs)-1], k.output); err != nil {
		return fmt.Errorf("KernelCase toEngineNodes: %w", err)
	}

	return nil
}

type KernelConcatString struct {
	partyCode string

	// Bonded tensors in this kernel
	inputs []*graph.Tensor
	output *graph.Tensor
}

func (k *KernelConcatString) Cost() float64 {
	// ConcatString is always executed under private placement
	return 0.0
}

func (k *KernelConcatString) String() string {
	return fmt.Sprintf("KernelConcatString(partyCode: %s)", k.partyCode)
}

func (k *KernelConcatString) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelConcatString ensureTensorPlace: expect Function node, but got %T", node)
	}

	k.inputs, err = createPlacedTensors(builder, function.inputs, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelConcatString ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelConcatString) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	function, ok := node.(*OperatorFunction)
	if !ok {
		return fmt.Errorf("KernelConcatString toEngineNodes: expect Function node, but got %T", node)
	}

	separator := types.NewStringDatum("") // only support nil separator now
	sepScalar := builder.tm.CreateUnbondedTensor("concat_separator", graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING), &publicPlacement{})
	sepScalar.IsConstScalar = true
	if err := builder.addOpConstant(&separator, sepScalar); err != nil {
		return nil
	}

	refTensor := k.inputs[0]
	sepTensor := builder.tm.CreateTensorAs(refTensor)
	sepTensor.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	sepTensor.Name = "broadcasted_concat_separator"
	if err := builder.addOpBroadcastTo(refTensor, []*graph.Tensor{sepScalar}, []*graph.Tensor{sepTensor}); err != nil {
		return fmt.Errorf("KernelConcatString toEngineNodes: %w", err)
	}

	tensor, err := builder.tm.CreateAndSetFirstTensor(function.output, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelArrowFunction toEngineNodes: %w", err)
	}
	k.output = tensor

	if err := builder.addOpArrowFunc("binary_join_element_wise", nil, append(k.inputs, sepTensor), k.output); err != nil {
		return fmt.Errorf("KernelArrowFunction toEngineNodes: %w", err)
	}

	return nil
}

type KernelPrivateWindow struct {
	partyCode string

	// Bonded tensors in this kernel
	orderKeys     []*graph.Tensor
	partitionKeys []*graph.Tensor
}

func (k *KernelPrivateWindow) Cost() float64 {
	return 0.0
}

func (k *KernelPrivateWindow) String() string {
	return "KernelPrivateRankWindow"
}

func (k *KernelPrivateWindow) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	window, ok := node.(*OperatorWindow)
	if !ok {
		return fmt.Errorf("KernelPrivateRankWindow ensureTensorPlace: expect RankWindow node, but got %T", node)
	}

	k.orderKeys, err = createPlacedTensors(builder, window.orderKeys, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateRankWindow ensureTensorPlace: %w", err)
	}
	k.partitionKeys, err = createPlacedTensors(builder, window.partitionKeys, &privatePlacement{k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateRankWindow ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelPrivateWindow) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	window, ok := node.(*OperatorWindow)
	if !ok {
		return fmt.Errorf("KernelPrivateRankWindow toEngineNodes: expect RankWindow node, but got %T", node)
	}

	for idx := range window.payloads {
		// the payloads are not modified in private rank window
		if err := builder.tensorMetaManager.setTensorsEquivalent(window.payloads[idx], window.payloadOutputs[idx]); err != nil {
			return fmt.Errorf("KernelPrivateRankWindow toEngineNodes: %w", err)
		}
	}

	partitionId := builder.tm.CreateUnbondedTensor("window_group_id", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{k.partyCode})
	partitionNum := builder.tm.CreateUnbondedTensor("window_group_num", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{k.partyCode})

	if err := builder.addOpPrivateGroup(k.partitionKeys, partitionId, partitionNum, k.partyCode); err != nil {
		return fmt.Errorf("KernelPrivateGroupAgg toEngineNodes: %w", err)
	}

	rankTensor, err := builder.tm.CreateAndSetFirstTensor(window.funcOutput, &privatePlacement{partyCode: k.partyCode})
	if err != nil {
		return fmt.Errorf("KernelPrivateRankWindow toEngineNodes: %w", err)
	}
	if err := builder.addOpPrivateWindow(window.funcName, k.orderKeys, partitionId, partitionNum, rankTensor, window.descs); err != nil {
		return fmt.Errorf("KernelPrivateRankWindow toEngineNodes: %w", err)
	}

	return nil
}

type KernelObliviousWindow struct {
	// Bonded tensors in this kernel
	orderKeys      []*graph.Tensor
	partitionKeys  []*graph.Tensor
	payloads       []*graph.Tensor
	payloadOutputs []*graph.Tensor
	funcOutput     *graph.Tensor
}

func (k *KernelObliviousWindow) Cost() float64 {
	return 1.0
}

func (k *KernelObliviousWindow) String() string {
	return "KernelObliviousRankWindow"
}

func (k *KernelObliviousWindow) ensureTensorPlace(builder *ExecutionGraphBuilder, node Operator) (err error) {
	window, ok := node.(*OperatorWindow)
	if !ok {
		return fmt.Errorf("KernelObliviousWindow ensureTensorPlace: expect RankWindow node, but got %T", node)
	}

	k.partitionKeys, err = createPlacedTensors(builder, window.partitionKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousWindow ensureTensorPlace: %w", err)
	}

	k.orderKeys, err = createPlacedTensors(builder, window.orderKeys, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousWindow ensureTensorPlace: %w", err)
	}

	k.payloads, err = createPlacedTensors(builder, window.payloads, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousWindow ensureTensorPlace: %w", err)
	}

	return nil
}

func (k *KernelObliviousWindow) toEngineNodes(builder *ExecutionGraphBuilder, node Operator) (err error) {
	window, ok := node.(*OperatorWindow)
	if !ok {
		return fmt.Errorf("KernelObliviousWindow toEngineNodes: expect RankWindow node, but got %T", node)
	}

	sortedPartitionKeys := make([]*graph.Tensor, 0, len(k.partitionKeys))
	for _, key := range k.partitionKeys {
		sortedKey := builder.tm.CreateTensorAs(key)
		sortedPartitionKeys = append(sortedPartitionKeys, sortedKey)
	}
	sortedOrderKeys := make([]*graph.Tensor, 0, len(k.orderKeys))
	for _, key := range k.orderKeys {
		sortedKey := builder.tm.CreateTensorAs(key)
		sortedOrderKeys = append(sortedOrderKeys, sortedKey)
	}
	k.payloadOutputs = make([]*graph.Tensor, len(window.payloadOutputs))
	for idx, meta := range window.payloadOutputs {
		tensor, err := builder.tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
		if err != nil {
			return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
		}
		k.payloadOutputs[idx] = tensor
	}
	sortKeys := append(k.partitionKeys, k.orderKeys...)
	sortPayloads := append(sortKeys, k.payloads...)
	sortResults := append(sortedPartitionKeys, sortedOrderKeys...)
	sortResults = append(sortResults, k.payloadOutputs...)
	if err := builder.addOpSort(sortKeys, sortPayloads, sortResults, slices.Repeat([]bool{false}, len(sortKeys))); err != nil {
		return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
	}

	groupMark := builder.tm.CreateUnbondedTensor("group_mark", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), &secretPlacement{})
	if err := builder.addOpObliviousGroupMark(sortedPartitionKeys, groupMark); err != nil {
		return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
	}

	funcOutput, err := builder.tm.CreateAndSetFirstTensor(window.funcOutput, &secretPlacement{})
	if err != nil {
		return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
	}
	k.funcOutput = funcOutput

	switch window.funcName {
	case ast.WindowFuncRowNumber:
		if err := builder.addOpObliviousGroupAgg(ast.AggFuncCount, groupMark, groupMark, k.funcOutput, map[string]*graph.Attribute{}); err != nil {
			return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
		}
	case ast.WindowFuncRank, ast.WindowFuncPercentRank:
		orderMarkKeys := sortResults[0:len(sortKeys)]
		orderMark := builder.tm.CreateUnbondedTensor("order_mark", graph.NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), &secretPlacement{})
		if err := builder.addOpObliviousGroupMark(orderMarkKeys, orderMark); err != nil {
			return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
		}
		if err := builder.addOpObliviousGroupAgg(window.funcName, groupMark, orderMark, k.funcOutput, map[string]*graph.Attribute{}); err != nil {
			return fmt.Errorf("KernelObliviousWindow toEngineNodes: %w", err)
		}
	default:
		return fmt.Errorf("KernelObliviousWindow toEngineNodes: unknown function name %s", window.funcName)
	}

	return nil
}

// ============================================================================
//                             Helper Functions
// ============================================================================

func createPlacedTensors(builder *ExecutionGraphBuilder, tensorMetas []*TensorMeta, placement tensorPlacement) ([]*graph.Tensor, error) {
	placedTensors := make([]*graph.Tensor, len(tensorMetas))
	for idx, meta := range tensorMetas {
		tensor, err := builder.getOrCreatePlacedTensor(meta, placement)
		if err != nil {
			return nil, err
		}
		placedTensors[idx] = tensor
	}
	return placedTensors, nil
}
