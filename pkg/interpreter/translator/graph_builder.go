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
	"strings"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

// GraphBuilder struct
type GraphBuilder struct {
	partyInfo *PartyInfo

	ExecutionNodes []*ExecutionNode
	Tensors        []*Tensor
	tensorNum      int
	outputName     []string
	converter      *statusConverter
}

// NewGraphBuilder returns a graph builder instance
func NewGraphBuilder(partyInfo *PartyInfo) *GraphBuilder {
	result := &GraphBuilder{
		partyInfo:      partyInfo,
		ExecutionNodes: make([]*ExecutionNode, 0),
		Tensors:        make([]*Tensor, 0),
	}
	result.converter = newStatusConverter(result)
	return result
}

// AddTensor adds a tensor
func (plan *GraphBuilder) AddTensor(name string) *Tensor {
	t := NewTensor(plan.tensorNum, name)
	plan.tensorNum++
	plan.Tensors = append(plan.Tensors, t)
	return t
}

// AddTensorAs adds a tensor giving a reference tensor
func (plan *GraphBuilder) AddTensorAs(it *Tensor) *Tensor {
	t := plan.AddTensor(it.Name)
	t.DType = it.DType
	t.Option = it.Option
	t.Status = it.Status
	t.OwnerPartyCode = it.OwnerPartyCode
	t.SecretStringOwners = it.SecretStringOwners
	t.cc = it.cc.Clone()
	t.skipDTypeCheck = it.skipDTypeCheck
	t.isConstScalar = it.isConstScalar
	return t
}

// AddColumn adds a column tensor
func (plan *GraphBuilder) AddColumn(name string, status proto.TensorStatus,
	option proto.TensorOptions, dType proto.PrimitiveDataType) *Tensor {
	t := plan.AddTensor(name)
	t.Status = status
	t.Option = option
	t.DType = dType
	t.cc = ccl.NewCCL()
	return t
}

// checkParamStatusConstraint check parameter status constraint strictly
func checkParamStatusConstraint[T statusConstraint](op *operator.OperatorDef, inputs map[string][]T, outputs map[string][]T) error {
	opDef := op.GetOperatorDefProto()
	if len(opDef.InputParams) != len(inputs) {
		return fmt.Errorf("checkParamStatusConstraint: op %v len(opDef.InputParams):%v != len(inputs):%v",
			opDef.Name, len(opDef.InputParams), len(inputs))
	}
	if len(opDef.OutputParams) != len(outputs) {
		return fmt.Errorf("checkParamStatusConstraint: op %v len(opDef.OutputParams):%v != len(outputs):%v",
			opDef.Name, len(opDef.OutputParams), len(outputs))
	}

	constraintNameToStatus := map[string]proto.TensorStatus{}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, inputs, opDef.InputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, outputs, opDef.OutputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	return nil
}

func checkParamStatusConstraintInternal[T statusConstraint](constraintNameToStatus map[string]proto.TensorStatus,
	args map[string][]T, params []*proto.FormalParameter,
	paramStatusConstraint map[string]*proto.TensorStatusList) error {
	for _, param := range params {
		arguments, ok := args[param.ParamName]
		if !ok {
			return fmt.Errorf("can't find param:%v in arguments", param.ParamName)
		}

		if len(arguments) == 0 && param.Option == proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_OPTIONAL {
			continue
		}

		if len(arguments) == 0 {
			return fmt.Errorf("param:%v must contains at least one argument", param.ParamName)
		}
		expectedStatus, ok := constraintNameToStatus[param.ParameterStatusConstraintName]
		if !ok {
			statusConstraint, ok := paramStatusConstraint[param.ParameterStatusConstraintName]
			if !ok {
				return fmt.Errorf("checkParamStatusConstraint: can't find constraint for param:%v, constraintName:%v",
					param.ParamName, param.ParameterStatusConstraintName)
			}
			found := false
			for _, s := range statusConstraint.Status {
				if args[param.ParamName][0].status() == s {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("checkParamStatusConstraint: invalid param status actual:%v, expected:%v", args[param.ParamName][0].status(), statusConstraint)
			}
			constraintNameToStatus[param.ParameterStatusConstraintName] = args[param.ParamName][0].status()
			continue
		}
		if args[param.ParamName][0].status() != expectedStatus {
			return fmt.Errorf("param status mismatch, actual:%v, expected:%v", args[param.ParamName][0].status(), expectedStatus)
		}
	}
	return nil
}

// AddExecutionNode adds an execution node
func (plan *GraphBuilder) AddExecutionNode(name string, opType string,
	inputs map[string][]*Tensor, outputs map[string][]*Tensor, attributes map[string]*Attribute, partyCodes []string) (*ExecutionNode, error) {
	opDef, err := operator.FindOpDef(opType)
	if err != nil {
		return nil, fmt.Errorf("addExecutionNode: %v", err)
	}

	if err := checkParamStatusConstraint(opDef, inputs, outputs); err != nil {
		return nil, fmt.Errorf("addExecutionNode: %v", err)
	}

	node := &ExecutionNode{
		Name:       name,
		OpType:     opType,
		Inputs:     make(map[string][]*Tensor),
		Outputs:    make(map[string][]*Tensor),
		Attributes: make(map[string]*Attribute),
		Parties:    partyCodes,
	}
	for k, is := range inputs {
		node.Inputs[k] = append([]*Tensor{}, is...)
	}
	for k, os := range outputs {
		node.Outputs[k] = append([]*Tensor{}, os...)
	}
	for k, attr := range attributes {
		node.Attributes[k] = attr
	}
	for k, defaultAttr := range opDef.GetDefaultAttribute() {
		if _, ok := node.Attributes[k]; !ok {
			node.Attributes[k] = &Attribute{
				TensorValue: newTensorFromProto(defaultAttr.GetT()),
			}
		}
	}
	plan.ExecutionNodes = append(plan.ExecutionNodes, node)
	return node, nil
}

// ToString dumps a debug string of the graph builder
func (plan *GraphBuilder) ToString() string {
	var builder strings.Builder
	fmt.Fprint(&builder, "execution plan:{")
	for _, node := range plan.ExecutionNodes {
		fmt.Fprintf(&builder, "%s,", node.ToString())
	}

	for _, t := range plan.Tensors {
		fmt.Fprintf(&builder, "%s,", t.ToString())
	}
	fmt.Fprint(&builder, "}")
	return builder.String()
}

// Build builds an execution plan dag
func (plan *GraphBuilder) Build() *Graph {
	graph := &Graph{
		Nodes:     make(map[*ExecutionNode]bool),
		PartyInfo: plan.partyInfo,
	}
	// 1. create node
	for _, node := range plan.ExecutionNodes {
		node.ID = graph.NodeCnt
		node.Edges = make(map[*Edge]bool)
		graph.Nodes[node] = true
		graph.NodeCnt++
	}

	// 2. create edge
	inputTensorTo := make(map[int][]*ExecutionNode)
	outputTensorFrom := make(map[int]*ExecutionNode)
	tensorId2Tensor := make(map[int]*Tensor)

	for node := range graph.Nodes {
		for _, ts := range node.Inputs {
			for _, t := range ts {
				tensorId2Tensor[t.ID] = t
				_, ok := inputTensorTo[t.ID]
				if !ok {
					inputTensorTo[t.ID] = make([]*ExecutionNode, 0)
				}
				inputTensorTo[t.ID] = append(inputTensorTo[t.ID], node)
			}
		}
		for _, ts := range node.Outputs {
			for _, t := range ts {
				tensorId2Tensor[t.ID] = t
				outputTensorFrom[t.ID] = node
			}
		}
	}

	for k, v := range tensorId2Tensor {
		for _, input := range inputTensorTo[k] {
			edge := &Edge{
				From:  outputTensorFrom[k],
				To:    input,
				Value: v,
			}
			outputTensorFrom[k].Edges[edge] = true
		}
	}

	graph.OutputNames = plan.outputName
	return graph
}

func (plan *GraphBuilder) AddRunSQLNode(name string, output []*Tensor,
	sql string, tableRefs []string, partyCode string) error {
	sqlAttr := &Attribute{}
	sqlAttr.SetString(sql)

	tableRefsAttr := &Attribute{}
	tableRefsAttr.SetStrings(tableRefs)
	_, err := plan.AddExecutionNode(name, operator.OpNameRunSQL, map[string][]*Tensor{}, map[string][]*Tensor{"Out": output},
		map[string]*Attribute{operator.SqlAttr: sqlAttr, operator.TableRefsAttr: tableRefsAttr}, []string{partyCode})
	return err
}

func (plan *GraphBuilder) AddPublishNode(name string, input []*Tensor, output []*Tensor, partyCodes []string) error {
	_, err := plan.AddExecutionNode(name, operator.OpNamePublish, map[string][]*Tensor{"In": input},
		map[string][]*Tensor{"Out": output}, map[string]*Attribute{}, partyCodes)
	return err
}

func (plan *GraphBuilder) AddDumpFileNode(name string, in []*Tensor, out []*Tensor, filepath, deliminator, partyCode string) error {
	fp := &Attribute{}
	fp.SetString(filepath)
	del := &Attribute{}
	del.SetString(deliminator)
	_, err := plan.AddExecutionNode(name, operator.OpNameDumpFile,
		map[string][]*Tensor{"In": in},
		map[string][]*Tensor{"Out": out},
		map[string]*Attribute{
			operator.FilePathAttr:    fp,
			operator.DeliminatorAttr: del,
		}, []string{partyCode})
	if err != nil {
		return fmt.Errorf("addDumpFileNode: %v", err)
	}
	return nil
}

func (plan *GraphBuilder) AddCopyNode(name string, in *Tensor, inputPartyCode, outputPartyCode string) (*Tensor, error) {
	inPartyAttr := &Attribute{}
	inPartyAttr.SetString(inputPartyCode)
	outPartyAttr := &Attribute{}
	outPartyAttr.SetString(outputPartyCode)
	if !in.cc.IsVisibleFor(outputPartyCode) {
		return nil, fmt.Errorf("fail to copy node, in tensor (%+v) is not visible for %s", in, outputPartyCode)
	}
	out := plan.AddTensorAs(in)
	out.OwnerPartyCode = outputPartyCode
	if _, err := plan.AddExecutionNode(name, operator.OpNameCopy,
		map[string][]*Tensor{"In": {in}},
		map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{
			operator.InputPartyCodesAttr:  inPartyAttr,
			operator.OutputPartyCodesAttr: outPartyAttr,
		},
		[]string{inputPartyCode, outputPartyCode}); err != nil {
		return nil, fmt.Errorf("fail to add copy node: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddMakePrivateNode(name string, input *Tensor, revealTo string, partyCodes []string) (*Tensor, error) {
	// check ccl
	if !input.cc.IsVisibleFor(revealTo) {
		return nil, fmt.Errorf("fail to check ccl: input %+v is not visible for %s when making private", input, revealTo)
	}
	output := plan.AddTensorAs(input)
	output.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	output.OwnerPartyCode = revealTo
	output.SecretStringOwners = nil
	attr := &Attribute{}
	attr.SetString(revealTo)
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakePrivate, map[string][]*Tensor{"In": []*Tensor{input}},
		map[string][]*Tensor{"Out": []*Tensor{output}}, map[string]*Attribute{operator.RevealToAttr: attr}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// MakeShare node doesn't reveal more info, so no need check ccl here
func (plan *GraphBuilder) AddMakeShareNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	output := plan.AddTensorAs(input)
	output.Status = proto.TensorStatus_TENSORSTATUS_SECRET
	output.OwnerPartyCode = ""
	if input.DType == proto.PrimitiveDataType_STRING {
		output.SecretStringOwners = []string{input.OwnerPartyCode}
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakeShare, map[string][]*Tensor{"In": []*Tensor{input}},
		map[string][]*Tensor{"Out": []*Tensor{output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddMakePublicNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	for _, p := range partyCodes {
		if !input.cc.IsVisibleFor(p) {
			return nil, fmt.Errorf("fail to check ccl: input %+v is not visible for %s when making public", input, p)
		}
	}
	output := plan.AddTensorAs(input)
	output.Status = proto.TensorStatus_TENSORSTATUS_PUBLIC
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakePublic, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// AddJoinNode adds a Join node, used in EQ join
func (plan *GraphBuilder) AddJoinNode(name string, left []*Tensor, right []*Tensor, partyCodes []string, joinType int) (*Tensor, *Tensor, error) {
	partyAttr := &Attribute{}
	partyAttr.SetStrings(partyCodes)
	joinTypeAttr := &Attribute{}
	joinTypeAttr.SetInt(joinType)

	inputs := make(map[string][]*Tensor)
	inputs["Left"] = left
	inputs["Right"] = right
	leftOutput := plan.AddTensorAs(left[0])
	leftOutput.DType = proto.PrimitiveDataType_INT64
	leftOutput.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	leftOutput.OwnerPartyCode = partyCodes[0]

	rightOutput := plan.AddTensorAs(right[0])
	rightOutput.DType = proto.PrimitiveDataType_INT64
	rightOutput.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	rightOutput.OwnerPartyCode = partyCodes[1]

	outputs := make(map[string][]*Tensor)
	outputs["LeftJoinIndex"] = []*Tensor{leftOutput}
	outputs["RightJoinIndex"] = []*Tensor{rightOutput}
	if _, err := plan.AddExecutionNode(name, operator.OpNameJoin, inputs, outputs,
		map[string]*Attribute{operator.InputPartyCodesAttr: partyAttr, operator.JoinTypeAttr: joinTypeAttr}, partyCodes); err != nil {
		return nil, nil, err
	}
	return leftOutput, rightOutput, nil
}

func (plan *GraphBuilder) AddFilterByIndexNode(name string, filter *Tensor, ts []*Tensor, partyCode string) ([]*Tensor, error) {
	inputs := make(map[string][]*Tensor)
	inputs["RowsIndexFilter"] = []*Tensor{filter}
	inputs["Data"] = ts
	outputs := make([]*Tensor, len(ts))
	if !filter.cc.IsVisibleFor(partyCode) {
		return nil, fmt.Errorf("failed to check filter ccl, filter is not visible for %s", partyCode)
	}
	for i, t := range ts {
		if t.OwnerPartyCode != partyCode {
			return nil, fmt.Errorf("failed to check tensor owner party code, tensor %+v is not in %s", t, partyCode)
		}
		outputs[i] = plan.AddTensorAs(t)
		outputs[i].OwnerPartyCode = partyCode
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameFilterByIndex, inputs,
		map[string][]*Tensor{"Out": outputs}, map[string]*Attribute{}, []string{partyCode}); err != nil {
		return nil, err
	}
	return outputs, nil
}

var strTypeUnsupportedOpM = map[string]bool{
	operator.OpNameLess:         true,
	operator.OpNameLessEqual:    true,
	operator.OpNameGreater:      true,
	operator.OpNameGreaterEqual: true,
	operator.OpNameAdd:          true,
	operator.OpNameMinus:        true,
	operator.OpNameMul:          true,
	operator.OpNameDiv:          true,
	operator.OpNameIntDiv:       true,
	operator.OpNameMod:          true,
}

func checkBinaryOpInputType(opType string, left, right *Tensor) error {
	// for datetime: left type maybe datetime or timestamp, right type maybe int64
	if (opType == operator.OpNameIntDiv || opType == operator.OpNameMod) &&
		((left.DType != proto.PrimitiveDataType_INT64 && left.DType != proto.PrimitiveDataType_DATETIME && left.DType != proto.PrimitiveDataType_TIMESTAMP) ||
			(right.DType != proto.PrimitiveDataType_INT64)) {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("op %v requires both left and right operands be int64", opType))
	}
	if _, ok := strTypeUnsupportedOpM[opType]; ok &&
		(left.DType == proto.PrimitiveDataType_STRING || right.DType == proto.PrimitiveDataType_STRING) {
		return status.Wrap(proto.Code_NOT_SUPPORTED, fmt.Errorf("op %v doesn't support input type %v", opType, proto.PrimitiveDataType_STRING))
	}
	return nil
}

func checkInputTypeNotString(inputs []*Tensor) error {
	for _, input := range inputs {
		if input.DType == proto.PrimitiveDataType_STRING {
			return fmt.Errorf("input type %v is not supported", input.DType)
		}
	}
	return nil
}

var binaryOpBoolOutputMap = map[string]bool{
	operator.OpNameLess:         true,
	operator.OpNameLessEqual:    true,
	operator.OpNameGreater:      true,
	operator.OpNameGreaterEqual: true,
	operator.OpNameEqual:        true,
	operator.OpNameNotEqual:     true,
	operator.OpNameLogicalAnd:   true,
	operator.OpNameLogicalOr:    true,
	operator.OpNameIn:           true,
}
var arithOpMap = map[string]bool{
	operator.OpNameAdd:    true,
	operator.OpNameMinus:  true,
	operator.OpNameMul:    true,
	operator.OpNameDiv:    true,
	operator.OpNameIntDiv: true,
	operator.OpNameMod:    true,
}

func isFloatOrDoubleType(tp proto.PrimitiveDataType) bool {
	return tp == proto.PrimitiveDataType_FLOAT32 || tp == proto.PrimitiveDataType_FLOAT64
}

func inferBinaryOpOutputType(opType string, left, right *Tensor) (proto.PrimitiveDataType, error) {
	if _, ok := binaryOpBoolOutputMap[opType]; ok {
		return proto.PrimitiveDataType_BOOL, nil
	}
	if left.DType == proto.PrimitiveDataType_DATETIME || left.DType == proto.PrimitiveDataType_TIMESTAMP {
		if opType == operator.OpNameIntDiv {
			// the result of datetime/int is no longer time but int (days)
			return proto.PrimitiveDataType_INT64, nil
		}
		// if left and right are both datetime then func must be datediff
		if left.DType == right.DType {
			return proto.PrimitiveDataType_INT64, nil
		}
		return left.DType, nil
	}
	if _, ok := arithOpMap[opType]; ok {
		if opType == operator.OpNameDiv {
			return proto.PrimitiveDataType_FLOAT64, nil
		}
		if opType == operator.OpNameIntDiv || opType == operator.OpNameMod {
			return proto.PrimitiveDataType_INT64, nil
		}

		// prompt result to double if any arguments is float data types (float/double/...)
		if isFloatOrDoubleType(left.DType) || isFloatOrDoubleType(right.DType) {
			return proto.PrimitiveDataType_FLOAT64, nil
		}

		// for op +/-/*
		if left.DType == right.DType {
			return left.DType, nil
		}

		return proto.PrimitiveDataType_INT64, nil
	}
	return proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED, fmt.Errorf("cannot infer output type for opType=%s", opType)
}

func (plan *GraphBuilder) AddBinaryNode(name string, opType string,
	left *Tensor, right *Tensor, outputCCL *ccl.CCL, partyCodes []string) (*Tensor, error) {
	if ok := slices.Contains(operator.BinaryOps, opType); !ok {
		return nil, fmt.Errorf("failed to check op type AddBinaryNode: invalid opType %v", opType)
	}
	if err := checkBinaryOpInputType(opType, left, right); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %w", err)
	}
	creator := algorithmCreator.getCreator(opType)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", opType)
	}
	algs, err := creator(map[string][]*ccl.CCL{Left: []*ccl.CCL{left.cc},
		Right: []*ccl.CCL{right.cc}}, map[string][]*ccl.CCL{Out: []*ccl.CCL{outputCCL}}, plan.partyInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	// select algorithm with the lowest cost
	bestAlg, err := plan.getBestAlg(opType, map[string][]*Tensor{Left: []*Tensor{left}, Right: []*Tensor{right}}, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	if left, err = plan.converter.convertTo(left, bestAlg.inputPlacement[Left][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	if right, err = plan.converter.convertTo(right, bestAlg.inputPlacement[Right][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	output := plan.AddTensor(name + "_out")
	output.Option = proto.TensorOptions_REFERENCE
	output.Status = bestAlg.outputPlacement[Out][0].status()
	if output.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		output.OwnerPartyCode = bestAlg.outputPlacement[Out][0].partyList()[0]
	}
	outputType, err := inferBinaryOpOutputType(opType, left, right)
	if err != nil {
		return nil, err
	}
	output.DType = outputType
	output.cc = outputCCL
	if _, err := plan.AddExecutionNode(name, opType, map[string][]*Tensor{Left: []*Tensor{left}, Right: []*Tensor{right}},
		map[string][]*Tensor{Out: []*Tensor{output}}, map[string]*Attribute{}, bestAlg.outputPlacement[Out][0].partyList()); err != nil {
		return nil, err
	}
	return output, nil
}

// best algorithm has lowest total cost which include data conversion cost and operator cost
func (plan *GraphBuilder) getBestAlg(opType string, inputs map[string][]*Tensor, algs []*materializedAlgorithm) (*materializedAlgorithm, error) {
	var possibleAlgorithms []*materializedAlgorithm
	op, err := operator.FindOpDef(opType)
	if err != nil {
		return nil, err
	}
	for _, alg := range algs {
		unsupportedAlgFlag := false
		for _, key := range sliceutil.SortMapKeyForDeterminism(inputs) {
			tensors := inputs[key]
			for i, tensor := range tensors {
				newPlacement := alg.inputPlacement[key][i]
				cost, err := plan.converter.getStatusConversionCost(tensor, newPlacement)
				if err != nil {
					unsupportedAlgFlag = true
					break
				}
				if checkParamStatusConstraint(op, alg.inputPlacement, alg.outputPlacement) != nil {
					unsupportedAlgFlag = true
					break
				}
				alg.cost.addCost(cost)
			}
		}
		if !unsupportedAlgFlag {
			possibleAlgorithms = append(possibleAlgorithms, alg)
		}
	}
	if len(possibleAlgorithms) == 0 {
		return nil, fmt.Errorf("getBestAlg: failed to find a algorithm")
	}

	bestAlg := possibleAlgorithms[0]
	for _, alg := range possibleAlgorithms[1:] {
		if alg.cost.calculateTotalCost() < bestAlg.cost.calculateTotalCost() {
			bestAlg = alg
		}
	}
	return bestAlg, nil
}

func (plan *GraphBuilder) AddFilterNode(name string, input []*Tensor, mask *Tensor, partyCodes []string) ([]*Tensor, error) {
	output := []*Tensor{}
	for _, it := range input {
		output = append(output, plan.AddTensorAs(it))
	}
	_, err := plan.AddExecutionNode(name, operator.OpNameFilter, map[string][]*Tensor{"Filter": {mask}, "In": input},
		map[string][]*Tensor{"Out": output}, map[string]*Attribute{}, partyCodes)
	if err != nil {
		return nil, fmt.Errorf("addFilterNode: %v", err)
	}
	return output, nil
}

// compare node used in least and greatest function
func (plan *GraphBuilder) AddCompareNode(name string, opType string, inputs []*Tensor) (*Tensor, error) {
	// TODO(xiaoyuan) implement when algorithm related code merged
	return nil, fmt.Errorf("compare operators(least/greatest) are unimplemented")
}

func (plan *GraphBuilder) AddConstantNode(name string, value *types.Datum, partyCodes []string) (*Tensor, error) {
	var dType proto.PrimitiveDataType
	attr := &Attribute{}
	switch value.Kind() {
	case types.KindFloat32, types.KindFloat64:
		dType = proto.PrimitiveDataType_FLOAT32
		attr.SetFloat(float32(value.GetFloat64()))
	case types.KindInt64:
		dType = proto.PrimitiveDataType_INT64
		attr.SetInt(int(value.GetInt64()))
	case types.KindMysqlDecimal:
		// NOTE(shunde.csd): SCQL Internal does not distinguish decimal and float,
		// It handles decimal as float.
		// If users have requirements for precision, they should use integers.
		dType = proto.PrimitiveDataType_FLOAT32
		v, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return nil, fmt.Errorf("addConstantNode: convert decimal to float error: %v", err)
		}
		attr.SetFloat(float32(v))
	case types.KindString:
		dType = proto.PrimitiveDataType_STRING
		attr.SetString(value.GetString())
	default:
		return nil, fmt.Errorf("addConstantNode: unsupported data{%+v}", value)
	}
	// constant in query can be seen by all party
	cc := ccl.NewCCL()
	for _, p := range partyCodes {
		cc.SetLevelForParty(p, ccl.Plain)
	}

	output := plan.AddColumn(
		"constant_data", proto.TensorStatus_TENSORSTATUS_PUBLIC,
		proto.TensorOptions_REFERENCE, dType)
	output.cc = cc
	output.isConstScalar = true

	attr1 := &Attribute{}
	statusPublic := 1
	attr1.SetInt(statusPublic)
	if _, err := plan.AddExecutionNode(name, operator.OpNameConstant, map[string][]*Tensor{},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{operator.ScalarAttr: attr, operator.ToStatusAttr: attr1}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// AddNotNode adds a Not node
func (plan *GraphBuilder) AddNotNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	output := plan.AddTensorAs(input)
	if _, err := plan.AddExecutionNode(name, operator.OpNameNot, map[string][]*Tensor{"In": []*Tensor{input}},
		map[string][]*Tensor{"Out": []*Tensor{output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddCastNode(name string, outputDType proto.PrimitiveDataType, input *Tensor, partyCodes []string) (*Tensor, error) {
	if len(partyCodes) > 1 && (input.DType == proto.PrimitiveDataType_STRING || outputDType == proto.PrimitiveDataType_STRING) {
		return nil, fmt.Errorf("AddCastNode: not support cast for string in spu, which exists in hash form")
	}
	output := plan.AddTensorAs(input)
	output.DType = outputDType
	if _, err := plan.AddExecutionNode(name, operator.OpNameCast, map[string][]*Tensor{"In": []*Tensor{input}},
		map[string][]*Tensor{"Out": []*Tensor{output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// For now, only support psi in
func (plan *GraphBuilder) AddInNode(left *Tensor, right *Tensor, outCCL *ccl.CCL) (*Tensor, error) {
	creator := algorithmCreator.getCreator(operator.OpNameIn)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameIn)
	}
	algs, err := creator(map[string][]*ccl.CCL{Left: []*ccl.CCL{left.cc},
		Right: []*ccl.CCL{right.cc}}, map[string][]*ccl.CCL{Out: []*ccl.CCL{outCCL}}, plan.partyInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("addInNode: %v", err)
	}
	for _, alg := range algs {
		pl := alg.outputPlacement[Out][0]
		if pl.status() == proto.TensorStatus_TENSORSTATUS_PRIVATE && pl.partyList()[0] != left.OwnerPartyCode {
			// result maybe need copy to issuer party, so the alg which out party is issuer party should be preferred
			alg.cost.communicationCost += 1
		}
	}
	// select algorithm with the lowest cost
	bestAlg, err := plan.getBestAlg(operator.OpNameIn, map[string][]*Tensor{Left: []*Tensor{left}, Right: []*Tensor{right}}, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	if left, err = plan.converter.convertTo(left, bestAlg.inputPlacement[Left][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	if right, err = plan.converter.convertTo(right, bestAlg.inputPlacement[Right][0]); err != nil {
		return nil, fmt.Errorf("addBinaryNode: %v", err)
	}
	// TODO(xiaoyuan) add share in/local in later
	return plan.addPSIInNode(left, right, outCCL, bestAlg.outputPlacement[Out][0].partyList()[0])
}

// AddPSIInNode adds psi-in node
func (plan *GraphBuilder) addPSIInNode(left *Tensor, right *Tensor, outCCL *ccl.CCL, revealParty string) (*Tensor, error) {
	nodeName := "psi_in"
	output := plan.AddTensor(nodeName + "_out")
	output.Option = proto.TensorOptions_REFERENCE
	output.DType = proto.PrimitiveDataType_BOOL
	output.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	output.OwnerPartyCode = revealParty
	output.cc = outCCL
	attr0 := &Attribute{}
	attr0.SetInt(PSIIn)
	attr1 := &Attribute{}
	attr1.SetStrings([]string{left.OwnerPartyCode, right.OwnerPartyCode})
	attr2 := &Attribute{}
	attr2.SetStrings([]string{revealParty})
	attrs := map[string]*Attribute{operator.AlgorithmAttr: attr0, operator.InputPartyCodesAttr: attr1, operator.RevealToAttr: attr2}
	if _, err := plan.AddExecutionNode(nodeName, operator.OpNameIn, map[string][]*Tensor{Left: []*Tensor{left}, Right: []*Tensor{right}},
		map[string][]*Tensor{Out: []*Tensor{output}}, attrs, []string{left.OwnerPartyCode, right.OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

// AddReduceAggNode adds a ReduceAgg node
func (plan *GraphBuilder) AddReduceAggNode(aggName string, in *Tensor) (*Tensor, error) {
	opType, ok := operator.ReduceAggOp[aggName]
	if !ok {
		return nil, fmt.Errorf("addReduceAggNode: unsupported aggregation fucntion %v", aggName)
	}
	partyCodes := plan.partyInfo.GetParties()
	if in.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{in.OwnerPartyCode}
	}
	out := plan.AddTensorAs(in)
	if aggName == ast.AggFuncAvg {
		out.DType = proto.PrimitiveDataType_FLOAT64
	} else if aggName == ast.AggFuncSum {
		if in.DType == proto.PrimitiveDataType_BOOL {
			out.DType = proto.PrimitiveDataType_INT64
		} else if isFloatOrDoubleType(in.DType) {
			out.DType = proto.PrimitiveDataType_FLOAT64
		}
	}
	if _, err := plan.AddExecutionNode("reduce_"+aggName, opType,
		map[string][]*Tensor{"In": {in}}, map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("addReduceAggNode: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddUniqueNode(name string, input *Tensor, partyCode string) (*Tensor, error) {
	inputP, err := plan.converter.convertTo(input, &privatePlacement{partyCode: partyCode})
	if err != nil {
		return nil, fmt.Errorf("addUniqueNode: %v", err)
	}

	output := plan.AddTensorAs(inputP)
	output.Name = "unique_" + output.Name
	output.OwnerPartyCode = partyCode
	if _, err := plan.AddExecutionNode(name, operator.OpNameUnique, map[string][]*Tensor{"Key": {inputP}},
		map[string][]*Tensor{"UniqueKey": {output}}, map[string]*Attribute{}, []string{partyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

// AddShapeNode adds a Shape node
func (plan *GraphBuilder) AddShapeNode(name string, in *Tensor, axis int, partyCode string) (*Tensor, error) {
	out := plan.AddTensorAs(in)
	out.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	out.DType = proto.PrimitiveDataType_INT64
	out.OwnerPartyCode = partyCode
	attr := &Attribute{}
	attr.SetInt(axis)
	if _, err := plan.AddExecutionNode(name, operator.OpNameShape,
		map[string][]*Tensor{"In": {in}}, map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{operator.AxisAttr: attr},
		[]string{partyCode}); err != nil {
		return nil, fmt.Errorf("addShapeNode: %v", err)
	}
	return out, nil
}

// if inputs tensors include share data or private tensors coming from different party, make all tensor share
func (plan *GraphBuilder) AddSortNode(name string, key, in []*Tensor) ([]*Tensor, error) {
	makeShareFlag := false
	privatePartyCodes := make(map[string]bool)
	for _, tensor := range append(in, key...) {
		switch tensor.Status {
		case proto.TensorStatus_TENSORSTATUS_SECRET, proto.TensorStatus_TENSORSTATUS_PUBLIC:
			makeShareFlag = true
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			privatePartyCodes[tensor.OwnerPartyCode] = true
		}
		if makeShareFlag {
			break
		}
		if len(privatePartyCodes) > 1 {
			makeShareFlag = true
			break
		}
	}
	var keyA, inA []*Tensor
	// convert inputs to share
	if makeShareFlag {
		for _, in := range key {
			out, err := plan.converter.convertTo(
				in, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
			if err != nil {
				return nil, fmt.Errorf("addSortNode: %v", err)
			}
			keyA = append(keyA, out)
		}
		for _, in := range in {
			out, err := plan.converter.convertTo(
				in, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
			if err != nil {
				return nil, fmt.Errorf("addSortNode: %v", err)
			}
			inA = append(inA, out)
		}
	} else {
		keyA = key
		inA = in
	}

	outA := []*Tensor{}
	for _, in := range inA {
		outA = append(outA, plan.AddTensorAs(in))
	}
	var partyCodes []string
	if makeShareFlag {
		partyCodes = plan.partyInfo.GetParties()
	} else {
		for p := range privatePartyCodes {
			partyCodes = append(partyCodes, p)
			break
		}
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameSort,
		map[string][]*Tensor{"Key": keyA, "In": inA}, map[string][]*Tensor{"Out": outA},
		map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("addSortNode: %v", err)
	}

	return outA, nil
}

func (plan *GraphBuilder) AddObliviousGroupMarkNode(name string, key []*Tensor) (*Tensor, error) {
	var keyA []*Tensor
	// convert inputs to share
	for _, in := range key {
		out, err := plan.converter.convertTo(
			in, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addObliviousGroupMarkNode: %v", err)
		}
		keyA = append(keyA, out)
	}

	out := plan.AddTensor("group_mark")
	out.Option = proto.TensorOptions_REFERENCE
	out.Status = proto.TensorStatus_TENSORSTATUS_SECRET
	out.DType = proto.PrimitiveDataType_BOOL

	if _, err := plan.AddExecutionNode(name, operator.OpNameObliviousGroupMark,
		map[string][]*Tensor{"Key": keyA}, map[string][]*Tensor{"Group": {out}},
		map[string]*Attribute{}, plan.partyInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addObliviousGroupMarkNode: %v", err)
	}

	return out, nil
}

func (plan *GraphBuilder) AddBroadcastToNode(name string, ins []*Tensor, shapeRefTensor *Tensor) ([]*Tensor, error) {
	partyCodes := plan.partyInfo.GetParties()
	var outs []*Tensor
	for _, in := range ins {
		out := plan.AddTensorAs(in)
		out.isConstScalar = false
		if shapeRefTensor.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyCodes = []string{shapeRefTensor.OwnerPartyCode}
			out.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
			out.OwnerPartyCode = shapeRefTensor.OwnerPartyCode
		}
		outs = append(outs, out)
	}

	if _, err := plan.AddExecutionNode(name, operator.OpNameBroadcastTo, map[string][]*Tensor{"In": ins, "ShapeRefTensor": {shapeRefTensor}}, map[string][]*Tensor{"Out": outs}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("graphBuilder.AddBroadcastToNode: %v", err)
	}
	return outs, nil
}

func (plan *GraphBuilder) AddObliviousGroupAggNode(funcName string, group *Tensor, in *Tensor) (*Tensor, error) {
	opName, ok := operator.ObliviousGroupAggOp[funcName]
	if !ok {
		return nil, fmt.Errorf("addObliviousGroupAggNode: unsupported op %v", funcName)
	}

	// convert inputs to share
	inA, err := plan.converter.convertTo(
		in, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
	if err != nil {
		return nil, fmt.Errorf("addObliviousGroupAggNode: %v", err)
	}

	outA := plan.AddTensorAs(inA)
	if funcName == ast.AggFuncAvg {
		outA.DType = proto.PrimitiveDataType_FLOAT64
	} else if funcName == ast.AggFuncCount {
		outA.DType = proto.PrimitiveDataType_INT64
	} else if funcName == ast.AggFuncSum {
		if inA.DType == proto.PrimitiveDataType_BOOL {
			outA.DType = proto.PrimitiveDataType_INT64
		} else if isFloatOrDoubleType(inA.DType) {
			outA.DType = proto.PrimitiveDataType_FLOAT64
		}
	}
	if _, err := plan.AddExecutionNode(funcName, opName,
		map[string][]*Tensor{"Group": {group}, "In": {inA}}, map[string][]*Tensor{"Out": {outA}},
		map[string]*Attribute{}, plan.partyInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addObliviousGroupAggNode: %v", err)
	}

	return outA, nil
}

func (plan *GraphBuilder) AddShuffleNode(name string, inTs []*Tensor) ([]*Tensor, error) {
	var inA []*Tensor
	// convert inputs to share
	for _, in := range inTs {
		out, err := plan.converter.convertTo(
			in, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addSortNode: %v", err)
		}
		inA = append(inA, out)
	}
	outA := []*Tensor{}
	for _, in := range inA {
		outA = append(outA, plan.AddTensorAs(in))
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameShuffle,
		map[string][]*Tensor{"In": inA}, map[string][]*Tensor{"Out": outA},
		map[string]*Attribute{}, plan.partyInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addShuffleNode: %v", err)
	}
	return outA, nil
}

func (plan *GraphBuilder) AddConcatNode(name string, in []*Tensor) (*Tensor, error) {
	newIn := []*Tensor{}
	for _, it := range in {
		// concat tensors coming from different party by default
		// make share before adding concat node
		ot, err := plan.converter.convertTo(
			it, &sharePlacement{partyCodes: plan.partyInfo.GetParties()})
		if err != nil {
			return nil, fmt.Errorf("addConcatNode: %v", err)
		}
		newIn = append(newIn, ot)
	}
	out := plan.AddTensorAs(newIn[0])
	for _, t := range newIn {
		out.SecretStringOwners = append(out.SecretStringOwners, t.SecretStringOwners...)
		if t.DType != out.DType {
			return nil, fmt.Errorf("addConcatNode: not support concat different type, please cast before union")
		}
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameConcat,
		map[string][]*Tensor{"In": newIn}, map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{}, plan.partyInfo.GetParties()); err != nil {
		return nil, fmt.Errorf("addConcatNode: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddGroupNode(name string, inputs []*Tensor, partyCode string) (*Tensor, *Tensor, error) {
	newInputs := []*Tensor{}
	for _, it := range inputs {
		ot, err := plan.converter.convertTo(it, &privatePlacement{partyCode: partyCode})
		if err != nil {
			return nil, nil, fmt.Errorf("AddGroupNode: %v", err)
		}
		newInputs = append(newInputs, ot)
	}
	outputId := plan.AddColumn("group_id", proto.TensorStatus_TENSORSTATUS_PRIVATE,
		proto.TensorOptions_REFERENCE, proto.PrimitiveDataType_INT64)
	cc := inputs[0].cc.Clone()
	for _, i := range inputs[1:] {
		cc.UpdateMoreRestrictedCCLFrom(i.cc)
	}
	outputId.OwnerPartyCode = partyCode
	outputId.cc = cc
	outputNum := plan.AddTensorAs(outputId)
	outputNum.Name = "group_num"
	if _, err := plan.AddExecutionNode(name, operator.OpNameGroup, map[string][]*Tensor{"Key": newInputs},
		map[string][]*Tensor{"GroupId": {outputId}, "GroupNum": {outputNum}}, map[string]*Attribute{}, []string{partyCode}); err != nil {
		return nil, nil, err
	}
	return outputId, outputNum, nil
}

func (plan *GraphBuilder) AddGroupAggNode(name string, opType string, groupId, groupNum *Tensor, in []*Tensor, partyCode string) ([]*Tensor, error) {
	placement := &privatePlacement{partyCode: partyCode}
	inP := []*Tensor{}
	for i, it := range in {
		ot, err := plan.converter.convertTo(it, placement)
		if err != nil {
			return nil, fmt.Errorf("AddGroupAggNode: name %v, opType %v, convert in#%v err: %v", name, opType, i, err)
		}
		inP = append(inP, ot)
	}
	groupIdP, err := plan.converter.convertTo(groupId, placement)
	if err != nil {
		return nil, fmt.Errorf("AddGroupAggNode: name %v, opType %v, convert group id err: %v", name, opType, err)
	}
	groupNumP, err := plan.converter.convertTo(groupNum, placement)
	if err != nil {
		return nil, fmt.Errorf("AddGroupAggNode: name %v, opType %v, convert group num err: %v", name, opType, err)
	}

	outputs := []*Tensor{}
	for _, it := range inP {
		output := plan.AddTensorAs(it)
		output.Name = output.Name + "_" + name
		if opType == operator.OpNameGroupAvg {
			output.DType = proto.PrimitiveDataType_FLOAT64
		} else if opType == operator.OpNameGroupSum {
			if isFloatOrDoubleType(it.DType) {
				output.DType = proto.PrimitiveDataType_FLOAT64
			} else {
				output.DType = proto.PrimitiveDataType_INT64
			}
		} else if opType == operator.OpNameGroupCount || opType == operator.OpNameGroupCountDistinct {
			output.DType = proto.PrimitiveDataType_INT64
		}
		outputs = append(outputs, output)
	}
	if _, err := plan.AddExecutionNode(name, opType,
		map[string][]*Tensor{
			"GroupId":  {groupIdP},
			"GroupNum": {groupNumP},
			"In":       inP,
		}, map[string][]*Tensor{"Out": outputs},
		map[string]*Attribute{}, []string{partyCode}); err != nil {
		return nil, fmt.Errorf("AddGroupAggNode: name %v, opType %v, err %v", name, opType, err)
	}
	return outputs, nil
}

func (plan *GraphBuilder) AddCaseWhenNode(name string, conds, value []*Tensor, valueElse *Tensor) (*Tensor, error) {
	if err := checkInputTypeNotString(conds); err != nil {
		return nil, fmt.Errorf("AddCaseWhenNode: %v", err)
	}
	// check all value are of the same type
	for _, v := range value {
		if v.DType != valueElse.DType {
			return nil, fmt.Errorf("AddCaseWhenNode: unmatched dtype in CASE WHEN else dtype(%v) != then dtype(%v)",
				shortStatus(proto.PrimitiveDataType_name[int32(valueElse.DType)]),
				shortStatus(proto.PrimitiveDataType_name[int32(v.DType)]),
			)
		}
	}
	out := plan.AddTensorAs(valueElse)
	var cond_ccl []*ccl.CCL
	for _, t := range conds {
		out.cc.UpdateMoreRestrictedCCLFrom(t.cc)
		cond_ccl = append(cond_ccl, t.cc)
	}
	var value_ccl []*ccl.CCL
	for _, t := range value {
		out.cc.UpdateMoreRestrictedCCLFrom(t.cc)
		value_ccl = append(value_ccl, t.cc)
	}

	creator := algorithmCreator.getCreator(operator.OpNameCaseWhen)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameCaseWhen)
	}

	algs, err := creator(map[string][]*ccl.CCL{Condition: cond_ccl,
		Value: value_ccl, ValueElse: {valueElse.cc}}, map[string][]*ccl.CCL{Out: {out.cc}}, plan.partyInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("AddCaseWhenNode: %v", err)
	}

	inputTs := map[string][]*Tensor{Condition: conds, Value: value, ValueElse: {valueElse}}
	// select algorithm with the lowest cost
	bestAlg, err := plan.getBestAlg(operator.OpNameCaseWhen, inputTs, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	convertedTs, err := plan.converter.convertStatusForMap(inputTs, bestAlg.inputPlacement)
	if err != nil {
		return nil, err
	}
	out.Status = bestAlg.outputPlacement[Out][0].status()
	if out.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		out.OwnerPartyCode = bestAlg.outputPlacement[Out][0].partyList()[0]
	}

	if _, err := plan.AddExecutionNode(name, operator.OpNameCaseWhen, convertedTs, map[string][]*Tensor{"Out": {out}}, map[string]*Attribute{}, bestAlg.outputPlacement[Out][0].partyList()); err != nil {
		return nil, fmt.Errorf("AddIfNode: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddGroupHeSumNode(name string, groupId, groupNum, in *Tensor, groupParty, inParty string) (*Tensor, error) {
	partyAttr := &Attribute{}
	partyAttr.SetStrings([]string{groupParty, inParty})

	placement := &privatePlacement{partyCode: groupParty}
	groupIdP, err := plan.converter.convertTo(groupId, placement)
	if err != nil {
		return nil, fmt.Errorf("AddGroupHeSumNode: name %v, convert group id err: %v", name, err)
	}
	groupNumP, err := plan.converter.convertTo(groupNum, placement)
	if err != nil {
		return nil, fmt.Errorf("AddGroupHeSumNode: name %v, convert group num err: %v", name, err)
	}
	placement = &privatePlacement{partyCode: inParty}
	inP, err := plan.converter.convertTo(in, placement)
	if err != nil {
		return nil, fmt.Errorf("AddGroupHeSumNode: name %v, convert agg err: %v", name, err)
	}

	output := plan.AddTensorAs(in)
	output.Name = output.Name + "_sum"
	if isFloatOrDoubleType(in.DType) {
		output.DType = proto.PrimitiveDataType_FLOAT64
	} else {
		output.DType = proto.PrimitiveDataType_INT64
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameGroupHeSum,
		map[string][]*Tensor{
			"GroupId":  {groupIdP},
			"GroupNum": {groupNumP},
			"In":       {inP},
		}, map[string][]*Tensor{"Out": {output}},
		map[string]*Attribute{operator.InputPartyCodesAttr: partyAttr}, []string{groupParty, inParty}); err != nil {
		return nil, fmt.Errorf("AddGroupHeSumNode: name %v, opType %v, err %v", name, operator.OpNameGroupHeSum, err)
	}
	return output, nil
}

func (plan *GraphBuilder) AddIfNode(name string, cond, tTrue, tFalse *Tensor) (*Tensor, error) {
	if tTrue.DType != tFalse.DType {
		return nil, fmt.Errorf("failed to check data type for true (%s), false (%s)", proto.PrimitiveDataType_name[int32(tTrue.DType)], proto.PrimitiveDataType_name[int32(tFalse.DType)])
	}
	if err := checkInputTypeNotString([]*Tensor{cond}); err != nil {
		return nil, fmt.Errorf("AddIfNode: %v", err)
	}
	out := plan.AddTensorAs(tTrue)
	// inference
	// result cond -> part of tTrue, part of tFalse
	// result tTrue -> cond, part of tFalse
	out.cc.UpdateMoreRestrictedCCLFrom(tFalse.cc)
	out.cc.UpdateMoreRestrictedCCLFrom(cond.cc)
	creator := algorithmCreator.getCreator(operator.OpNameIf)
	if creator == nil {
		return nil, fmt.Errorf("fail to get algorithm creator for op type %s", operator.OpNameIf)
	}
	algs, err := creator(map[string][]*ccl.CCL{Condition: {cond.cc},
		ValueIfTrue: {tTrue.cc}, ValueIfFalse: {tTrue.cc}}, map[string][]*ccl.CCL{Out: {out.cc}}, plan.partyInfo.GetParties())
	if err != nil {
		return nil, fmt.Errorf("AddIfNode: %v", err)
	}
	inputTs := map[string][]*Tensor{Condition: {cond}, ValueIfTrue: {tTrue}, ValueIfFalse: {tFalse}}
	// select algorithm with the lowest cost
	bestAlg, err := plan.getBestAlg(operator.OpNameIf, inputTs, algs)
	if err != nil {
		return nil, err
	}
	// Convert Tensor Status if needed
	convertedTs, err := plan.converter.convertStatusForMap(inputTs, bestAlg.inputPlacement)
	if err != nil {
		return nil, err
	}
	out.Status = bestAlg.outputPlacement[Out][0].status()
	if out.Status == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		out.OwnerPartyCode = bestAlg.outputPlacement[Out][0].partyList()[0]
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameIf, convertedTs, map[string][]*Tensor{"Out": {out}}, map[string]*Attribute{}, bestAlg.outputPlacement[Out][0].partyList()); err != nil {
		return nil, fmt.Errorf("AddIfNode: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddLimitNode(name string, input []*Tensor, offset int, count int, partyCodes []string) ([]*Tensor, error) {
	output := []*Tensor{}
	for _, it := range input {
		output = append(output, plan.AddTensorAs(it))
	}
	offsetAttr := &Attribute{}
	offsetAttr.SetInt(offset)
	countAttr := &Attribute{}
	countAttr.SetInt(count)
	_, err := plan.AddExecutionNode(name, operator.OpNameLimit, map[string][]*Tensor{"In": input},
		map[string][]*Tensor{"Out": output}, map[string]*Attribute{operator.LimitOffsetAttr: offsetAttr, operator.LimitCountAttr: countAttr}, partyCodes)
	if err != nil {
		return nil, fmt.Errorf("AddLimitNode: %v", err)
	}
	return output, nil
}
