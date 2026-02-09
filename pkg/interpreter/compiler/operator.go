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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/types"
)

// Operator represents a computational node in the SCQL operator graph.
// It defines the interface for all operators that perform data transformations.
type Operator interface {
	ID() int
	SetID(id int)
	SetKernel(k Kernel)
	Kernel() Kernel

	String() string

	// Label returns a human-readable name for this operator type.
	// Used for debugging and visualization purposes.
	Label() string

	// Inputs returns a map of input tensors grouped by logical names.
	// Each key represents a logical input group (e.g., "left", "right" for joins),
	// and the value is a slice of tensor metadata for that group.
	Inputs() map[string][]*TensorMeta

	// Outputs returns a map of output tensors grouped by logical names.
	// Similar to Inputs(), but represents the results produced by this operator.
	Outputs() map[string][]*TensorMeta

	// Attrs returns operator-specific attributes and configuration parameters.
	// These attributes control the behavior of the operator during execution.
	Attrs() map[string]any

	// Infer the visibility of the output tensors based on the visibility of the input tensors
	// Handle the basic operator-specific visibility inference part in Infer
	InferVis(vr VisibilityRegistry) error
	// Infer the CSR of the output tensors based on the CSR of the input tensors
	// Handle CSR updating part in Infer
	InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error
	// Infer the visibility of the output tensors based on the visibility of the input tensors
	// Security relaxation will be considered if applySecurityRelaxation is true
	// In addition, the CSC will also be updated during the inference
	Infer(v *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error)
	// Infer the visibility of the input tensors based on the visibility of the output tensors
	ReverseInfer(v *VisibilitySolver) ([]*TensorMeta, error)
}

// GetNodeInputs returns all input tensors flattened from node's Inputs
func GetNodeInputs(node Operator) []*TensorMeta {
	var inputs []*TensorMeta
	for _, tensors := range node.Inputs() {
		inputs = append(inputs, tensors...)
	}
	return inputs
}

// GetNodeOutputs returns all output tensors flattened from node's Outputs
func GetNodeOutputs(node Operator) []*TensorMeta {
	var outputs []*TensorMeta
	for _, tensors := range node.Outputs() {
		outputs = append(outputs, tensors...)
	}
	return outputs
}

type baseOperator struct {
	id     int
	label  string // human-friendly node label
	kernel Kernel
}

func (n *baseOperator) ID() int {
	return n.id
}

func (n *baseOperator) SetID(id int) {
	n.id = id
}

func (n *baseOperator) SetKernel(k Kernel) {
	n.kernel = k
}

func (n *baseOperator) Kernel() Kernel {
	return n.kernel
}

func (n *baseOperator) Label() string {
	return n.label
}

// toSerializable converts complex types to simple, JSON-serializable formats.
func toSerializable(v any) any {
	switch t := v.(type) {
	case *TensorMeta:
		if t == nil {
			return nil
		}
		return t.ID
	case []*TensorMeta:
		result := make([]int, len(t))
		for i, tensor := range t {
			result[i] = tensor.ID
		}
		return result
	case *aggregation.AggFuncDesc:
		if t == nil {
			return nil
		}
		return formatAggFuncDesc(t)
	case []*aggregation.AggFuncDesc:
		return stringifyAggFuncDescs(t)
	case *types.Datum:
		if t == nil {
			return nil
		}
		return formatDatum(t)
	default:
		return v
	}
}

// formatNodeString provides a consistent string representation for all Operator types
func formatNodeString(nodeType string, node Operator) string {
	inputs := node.Inputs()
	outputs := node.Outputs()
	attrs := node.Attrs()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("{\n%s:\n", nodeType))

	// Inputs
	sb.WriteString("  Inputs:\n")
	if len(inputs) == 0 {
		sb.WriteString("    no input tensors\n")
	} else {
		for key, tensors := range inputs {
			sb.WriteString(fmt.Sprintf("    %s: %v\n", key, tensors))
		}
	}

	// Outputs
	sb.WriteString("  Outputs:\n")
	if len(outputs) == 0 {
		sb.WriteString("    no output tensors\n")
	} else {
		for key, tensors := range outputs {
			sb.WriteString(fmt.Sprintf("    %s: %v\n", key, tensors))
		}
	}

	// Attrs
	sb.WriteString("  Attrs:\n")
	if len(attrs) == 0 {
		sb.WriteString("    no attrs\n")
	} else {
		for key, val := range attrs {
			sb.WriteString(fmt.Sprintf("    %s: %v\n", key, val))
		}
	}

	sb.WriteString("}")
	return sb.String()
}

func makeNodeJSON(id int, label string, inputTensors map[string]any, outputTensors map[string]any, otherAttrs map[string]any) ([]byte, error) {
	// Process tensor attributes
	for k, v := range inputTensors {
		inputTensors[k] = toSerializable(v)
	}
	for k, v := range outputTensors {
		outputTensors[k] = toSerializable(v)
	}
	for k, v := range otherAttrs {
		otherAttrs[k] = toSerializable(v)
	}

	nodeData := struct {
		ID            int            `json:"id"`
		Label         string         `json:"label"`
		InputTensors  map[string]any `json:"inputTensors"`
		OutputTensors map[string]any `json:"outputTensors"`
		Attrs         map[string]any `json:"attrs,omitempty"`
	}{
		ID:            id,
		Label:         label,
		InputTensors:  inputTensors,
		OutputTensors: outputTensors,
		Attrs:         otherAttrs,
	}

	return json.Marshal(nodeData)
}

type OperatorResult struct {
	baseOperator

	resultTensors []*TensorMeta

	outputNames     []string
	issuerPartyCode string
	intoOpt         *core.IntoOpt
	insertTableOpt  *core.InsertTableOption
	resultTable     map[int64]*TensorMeta
}

func (n *OperatorResult) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"resultTensors": n.resultTensors,
	}
}

func (n *OperatorResult) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{} // Result node has no outputs
}

func (n *OperatorResult) Attrs() map[string]any {
	return map[string]any{
		"outputNames": n.outputNames,
		"issuerParty": n.issuerPartyCode,
		"intoOpt":     n.intoOpt,
		"insertOpt":   n.insertTableOpt,
		"resultTable": n.resultTable,
	}
}

func (n *OperatorResult) Label() string {
	return "Result"
}

func (n *OperatorResult) String() string {
	return formatNodeString("OperatorResult", n)
}

// JSON serialization for OperatorResult
func (n *OperatorResult) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"resultTensors": n.resultTensors,
	}
	otherAttrs := map[string]any{
		"outputNames": n.outputNames,
	}
	return makeNodeJSON(n.id, "Result", inputTensors, map[string]any{}, otherAttrs)
}

type OperatorDataSource struct {
	baseOperator

	outputs []*TensorMeta

	sourceParty string
	originNames []string
}

func (n *OperatorDataSource) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{} // DataSource node has no inputs
}

func (n *OperatorDataSource) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorDataSource) Attrs() map[string]any {
	return map[string]any{
		"sourceParty": n.sourceParty,
		"originNames": n.originNames,
	}
}

func (n *OperatorDataSource) Label() string {
	return "DataSource"
}

func (n *OperatorDataSource) String() string {
	return formatNodeString("OperatorDataSource", n)
}

// JSON serialization for OperatorDataSource
func (n *OperatorDataSource) MarshalJSON() ([]byte, error) {
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	otherAttrs := map[string]any{
		"sourceParty": n.sourceParty,
		"originNames": n.originNames,
	}
	return makeNodeJSON(n.id, "DataSource", map[string]any{}, outputTensors, otherAttrs)
}

type OperatorRunSQL struct {
	baseOperator

	outputs []*TensorMeta

	sql             string
	sourceParty     string
	tableRefs       []string
	subGraphNodes   []Operator
	subGraphTracker *TensorConsumerTracker
}

func (n *OperatorRunSQL) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{} // RunSQL node has no inputs
}

func (n *OperatorRunSQL) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorRunSQL) Attrs() map[string]any {
	return map[string]any{
		"sql":           n.sql,
		"sourceParty":   n.sourceParty,
		"tableRefs":     n.tableRefs,
		"subGraphNodes": n.subGraphNodes,
		// Ignore subGraphTracker to avoid too much details
	}
}

func (n *OperatorRunSQL) Label() string {
	return "RunSQL"
}

func (n *OperatorRunSQL) String() string {
	return formatNodeString("OperatorRunSQL", n)
}

// JSON serialization for OperatorRunSQL
func (n *OperatorRunSQL) MarshalJSON() ([]byte, error) {
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	otherAttrs := map[string]any{
		"sql":         n.sql,
		"sourceParty": n.sourceParty,
		"tableRefs":   n.tableRefs,
	}
	return makeNodeJSON(n.id, "RunSQL", map[string]any{}, outputTensors, otherAttrs)
}

type OperatorEQJoin struct {
	baseOperator

	leftKeys      []*TensorMeta
	rightKeys     []*TensorMeta
	leftPayloads  []*TensorMeta
	rightPayloads []*TensorMeta
	leftOutputs   []*TensorMeta
	rightOutputs  []*TensorMeta

	joinType core.JoinType
}

func (n *OperatorEQJoin) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"leftKeys":      n.leftKeys,
		"rightKeys":     n.rightKeys,
		"leftPayloads":  n.leftPayloads,
		"rightPayloads": n.rightPayloads,
	}
}

func (n *OperatorEQJoin) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"leftOutputs":  n.leftOutputs,
		"rightOutputs": n.rightOutputs,
	}
}

func (n *OperatorEQJoin) Attrs() map[string]any {
	return map[string]any{
		"joinType": n.joinType.String(),
	}
}

func (n *OperatorEQJoin) Label() string {
	return "EQJoin"
}

func (n *OperatorEQJoin) String() string {
	return formatNodeString("OperatorEQJoin", n)
}

// JSON serialization for OperatorEQJoin
func (n *OperatorEQJoin) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"leftKeys":      n.leftKeys,
		"rightKeys":     n.rightKeys,
		"leftPayloads":  n.leftPayloads,
		"rightPayloads": n.rightPayloads,
	}
	outputTensors := map[string]any{
		"leftOutputs":  n.leftOutputs,
		"rightOutputs": n.rightOutputs,
	}
	otherAttrs := map[string]any{
		"joinType": n.joinType.String(),
	}
	return makeNodeJSON(n.id, "EQJoin", inputTensors, outputTensors, otherAttrs)
}

type OperatorCrossJoin struct {
	baseOperator

	leftInputs   []*TensorMeta
	rightInputs  []*TensorMeta
	leftOutputs  []*TensorMeta
	rightOutputs []*TensorMeta
}

func (n *OperatorCrossJoin) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"leftInputs":  n.leftInputs,
		"rightInputs": n.rightInputs,
	}
}

func (n *OperatorCrossJoin) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"leftOutputs":  n.leftOutputs,
		"rightOutputs": n.rightOutputs,
	}
}

func (n *OperatorCrossJoin) Attrs() map[string]any {
	return nil // No additional attributes
}

func (n *OperatorCrossJoin) Label() string {
	return "CrossJoin"
}

func (n *OperatorCrossJoin) String() string {
	return formatNodeString("OperatorCrossJoin", n)
}

// JSON serialization for OperatorCrossJoin
func (n *OperatorCrossJoin) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"leftInputs":  n.leftInputs,
		"rightInputs": n.rightInputs,
	}
	outputTensors := map[string]any{
		"leftOutputs":  n.leftOutputs,
		"rightOutputs": n.rightOutputs,
	}
	return makeNodeJSON(n.id, "CrossJoin", inputTensors, outputTensors, map[string]any{})
}

type OperatorLimit struct {
	baseOperator

	inputs  []*TensorMeta
	outputs []*TensorMeta

	offset uint64
	count  uint64
}

func (n *OperatorLimit) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"inputs": n.inputs,
	}
}

func (n *OperatorLimit) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorLimit) Attrs() map[string]any {
	return map[string]any{
		"offset": n.offset,
		"count":  n.count,
	}
}

func (n *OperatorLimit) Label() string {
	return "Limit"
}

func (n *OperatorLimit) String() string {
	return formatNodeString("OperatorLimit", n)
}

// JSON serialization for OperatorLimit
func (n *OperatorLimit) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"inputs": n.inputs,
	}
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	otherAttrs := map[string]any{
		"offset": n.offset,
		"count":  n.count,
	}
	return makeNodeJSON(n.id, "Limit", inputTensors, outputTensors, otherAttrs)
}

type OperatorFilter struct {
	baseOperator

	mask    *TensorMeta
	inputs  []*TensorMeta
	outputs []*TensorMeta
}

func (n *OperatorFilter) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"mask":   {n.mask},
		"inputs": n.inputs,
	}
}

func (n *OperatorFilter) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorFilter) Attrs() map[string]any {
	return nil // No additional attributes
}

func (n *OperatorFilter) Label() string {
	return "Filter"
}

func (n *OperatorFilter) String() string {
	return formatNodeString("OperatorFilter", n)
}

// JSON serialization for OperatorFilter
func (n *OperatorFilter) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"mask":   n.mask,
		"inputs": n.inputs,
	}
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	return makeNodeJSON(n.id, "Filter", inputTensors, outputTensors, map[string]any{})
}

type OperatorBroadcastTo struct {
	baseOperator

	shapeRef *TensorMeta
	scalars  []*TensorMeta
	outputs  []*TensorMeta
}

func (n *OperatorBroadcastTo) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"shapeRef": {n.shapeRef},
		"scalars":  n.scalars,
	}
}

func (n *OperatorBroadcastTo) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorBroadcastTo) Attrs() map[string]any {
	return nil // No additional attributes
}

func (n *OperatorBroadcastTo) Label() string {
	return "BroadcastTo"
}

func (n *OperatorBroadcastTo) String() string {
	return formatNodeString("OperatorBroadcastTo", n)
}

// JSON serialization for OperatorBroadcastTo
func (n *OperatorBroadcastTo) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"shapeRef": n.shapeRef,
		"scalars":  n.scalars,
	}
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	return makeNodeJSON(n.id, "BroadcastTo", inputTensors, outputTensors, map[string]any{})
}

type OperatorConcat struct {
	baseOperator

	inputs []*TensorMeta
	output *TensorMeta
}

func (n *OperatorConcat) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"inputs": n.inputs,
	}
}

func (n *OperatorConcat) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"output": {n.output},
	}
}

func (n *OperatorConcat) Attrs() map[string]any {
	return nil // No additional attributes
}

func (n *OperatorConcat) Label() string {
	return "Concat"
}

func (n *OperatorConcat) String() string {
	return formatNodeString("OperatorConcat", n)
}

// JSON serialization for OperatorConcat
func (n *OperatorConcat) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"inputs": n.inputs,
	}
	outputTensors := map[string]any{
		"output": n.output,
	}
	return makeNodeJSON(n.id, "Concat", inputTensors, outputTensors, map[string]any{})
}

type OperatorSort struct {
	baseOperator

	sortKeys   []*TensorMeta
	payloads   []*TensorMeta
	outputs    []*TensorMeta
	descending []bool
}

func (n *OperatorSort) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"sortKeys": n.sortKeys,
		"payloads": n.payloads,
	}
}

func (n *OperatorSort) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"outputs": n.outputs,
	}
}

func (n *OperatorSort) Attrs() map[string]any {
	return map[string]any{
		"descending": n.descending,
	}
}

func (n *OperatorSort) Label() string {
	return "Sort"
}

func (n *OperatorSort) String() string {
	return formatNodeString("OperatorSort", n)
}

// JSON serialization for OperatorSort
func (n *OperatorSort) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"sortKeys": n.sortKeys,
		"payloads": n.payloads,
	}
	outputTensors := map[string]any{
		"outputs": n.outputs,
	}
	otherAttrs := map[string]any{
		"descending": n.descending,
	}
	return makeNodeJSON(n.id, "Sort", inputTensors, outputTensors, otherAttrs)
}

type OperatorReduce struct {
	baseOperator

	input   *TensorMeta
	output  *TensorMeta
	aggFunc *aggregation.AggFuncDesc
}

func (n *OperatorReduce) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"input": {n.input},
	}
}

func (n *OperatorReduce) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"output": {n.output},
	}
}

func (n *OperatorReduce) Attrs() map[string]any {
	return map[string]any{
		"aggFunc": n.aggFunc,
	}
}

func (n *OperatorReduce) Label() string {
	return "Reduce"
}

func (n *OperatorReduce) String() string {
	return formatNodeString("OperatorReduce", n)
}

// JSON serialization for OperatorReduce
func (n *OperatorReduce) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"input": n.input,
	}
	outputTensors := map[string]any{
		"output": n.output,
	}
	otherAttrs := map[string]any{
		"aggFunc": n.aggFunc,
	}
	return makeNodeJSON(n.id, "Reduce", inputTensors, outputTensors, otherAttrs)
}

type OperatorGroupAgg struct {
	baseOperator

	groupKeys          []*TensorMeta
	aggArgs            []*TensorMeta
	argFuncOutputs     []*TensorMeta
	simpleCountOutputs []*TensorMeta

	aggFuncsWithArg []*aggregation.AggFuncDesc

	keysVisAfterAgg *OverlayVisibilityTable
}

func (n *OperatorGroupAgg) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"groupKeys": n.groupKeys,
		"payloads":  n.aggArgs,
	}
}

func (n *OperatorGroupAgg) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"argFuncOutputs":     n.argFuncOutputs,
		"simpleCountOutputs": n.simpleCountOutputs,
	}
}

func (n *OperatorGroupAgg) Attrs() map[string]any {
	return map[string]any{
		"aggFuncsWithArg": n.aggFuncsWithArg,
	}
}

func (n *OperatorGroupAgg) Label() string {
	return "GroupAgg"
}

func (n *OperatorGroupAgg) String() string {
	return formatNodeString("OperatorGroupAgg", n)
}

// JSON serialization for OperatorGroupAgg
func (n *OperatorGroupAgg) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"groupKeys": n.groupKeys,
		"aggArgs":   n.aggArgs,
	}
	outputTensors := map[string]any{
		"argFuncOutputs":     n.argFuncOutputs,
		"simpleCountOutputs": n.simpleCountOutputs,
	}
	otherAttrs := map[string]any{
		"aggFuncsWithArg": n.aggFuncsWithArg,
	}
	return makeNodeJSON(n.id, "GroupAgg", inputTensors, outputTensors, otherAttrs)
}

type OperatorWindow struct {
	baseOperator

	partitionKeys []*TensorMeta
	orderKeys     []*TensorMeta
	payloads      []*TensorMeta

	// In the current implementation, payloads are sorted in the secret window rank, while the private window rank outputs the raw payloads.
	// We treat payloads and outputs as different tensors in OperatorGraph, although they maybe the same tensors in the ExecutionGraph.
	payloadOutputs []*TensorMeta
	// Output tensor for window function
	// Currently, we only support one window function in a window node.
	funcOutput *TensorMeta

	// Window function name
	funcName string
	// order by desc attrs, currently effective only in private rank window.
	descs []string
}

func (n *OperatorWindow) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"partitionKeys": n.partitionKeys,
		"orderKeys":     n.orderKeys,
		"payloads":      n.payloads,
	}
}

func (n *OperatorWindow) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"payloadOutputs": n.payloadOutputs,
		"funcOutput":     {n.funcOutput},
	}
}

func (n *OperatorWindow) Attrs() map[string]any {
	return map[string]any{
		"descs":    n.descs,
		"funcName": n.funcName,
	}
}

func (n *OperatorWindow) Label() string {
	return "Window"
}

func (n *OperatorWindow) String() string {
	return formatNodeString("OperatorWindow", n)
}

// JSON serialization for OperatorRankWindow
func (n *OperatorWindow) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"partitionKeys": n.partitionKeys,
		"orderKeys":     n.orderKeys,
		"payloads":      n.payloads,
	}
	outputTensors := map[string]any{
		"payloadOutputs": n.payloadOutputs,
		"funcOutput":     n.funcOutput,
	}
	otherAttrs := map[string]any{
		"descs": n.descs,
	}
	return makeNodeJSON(n.id, "Window", inputTensors, outputTensors, otherAttrs)
}

type OperatorIn struct {
	baseOperator

	left   *TensorMeta
	right  *TensorMeta
	output *TensorMeta
}

func (n *OperatorIn) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"left":  {n.left},
		"right": {n.right},
	}
}

func (n *OperatorIn) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"output": {n.output},
	}
}

func (n *OperatorIn) Attrs() map[string]any {
	return nil // No additional attributes
}

func (n *OperatorIn) Label() string {
	return "In"
}

func (n *OperatorIn) String() string {
	return formatNodeString("OperatorIn", n)
}

// JSON serialization for OperatorIn
func (n *OperatorIn) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"left":  n.left,
		"right": n.right,
	}
	outputTensors := map[string]any{
		"output": n.output,
	}
	return makeNodeJSON(n.id, "In", inputTensors, outputTensors, map[string]any{})
}

type OperatorConstant struct {
	baseOperator

	output *TensorMeta

	value *types.Datum
}

func (n *OperatorConstant) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{} // No inputs
}

func (n *OperatorConstant) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"output": {n.output},
	}
}

func (n *OperatorConstant) Attrs() map[string]any {
	return map[string]any{
		"value": n.value,
	}
}

func (n *OperatorConstant) Label() string {
	return "Constant"
}

func (n *OperatorConstant) String() string {
	return formatNodeString("OperatorConstant", n)
}

// JSON serialization for OperatorConstant
func (n *OperatorConstant) MarshalJSON() ([]byte, error) {
	outputTensors := map[string]any{
		"output": n.output,
	}
	otherAttrs := map[string]any{
		"value": n.value,
	}
	return makeNodeJSON(n.id, "Constant", map[string]any{}, outputTensors, otherAttrs)
}

type OperatorFunction struct {
	baseOperator

	inputs []*TensorMeta
	output *TensorMeta

	constParams []*expression.Constant
	funcName    string
}

func (n *OperatorFunction) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"inputs": n.inputs,
	}
}

func (n *OperatorFunction) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{
		"output": {n.output},
	}
}

func (n *OperatorFunction) Attrs() map[string]any {
	return map[string]any{
		"constParams": n.constParams,
		"funcName":    n.funcName,
	}
}

func (n *OperatorFunction) Label() string {
	return "Func " + n.funcName
}

func (n *OperatorFunction) String() string {
	return formatNodeString("OperatorFunction", n)
}

// JSON serialization for OperatorFunction
func (n *OperatorFunction) MarshalJSON() ([]byte, error) {
	inputTensors := map[string]any{
		"inputs": n.inputs,
	}
	outputTensors := map[string]any{
		"output": n.output,
	}
	otherAttrs := map[string]any{
		"constParams": n.constParams,
		"funcName":    n.funcName,
	}
	label := "Func " + strings.ToUpper(n.funcName)
	return makeNodeJSON(n.id, label, inputTensors, outputTensors, otherAttrs)
}

// Helper function to format AggFuncDesc with name and suffixes
func formatAggFuncDesc(aggFunc *aggregation.AggFuncDesc) string {
	name := aggFunc.Name
	if aggFunc.HasDistinct {
		name += "_dist"
	}
	if aggFunc.Mode == aggregation.FinalMode {
		name += "_final"
	}
	return name
}

// Helper function to convert AggFuncDesc slice to their string representations
func stringifyAggFuncDescs(aggFuncs []*aggregation.AggFuncDesc) []string {
	result := make([]string, len(aggFuncs))
	for i, aggFunc := range aggFuncs {
		result[i] = formatAggFuncDesc(aggFunc)
	}
	return result
}

// Helper function to format Datum with type and value information
func formatDatum(datum *types.Datum) string {
	if datum.IsNull() {
		return "NULL"
	}

	// Use the existing String() method which already formats as "Type Value"
	return datum.String()
}
