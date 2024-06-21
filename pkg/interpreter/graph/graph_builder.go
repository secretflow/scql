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

package graph

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/types"
)

// GraphBuilder struct
type GraphBuilder struct {
	partyInfo *PartyInfo

	ExecutionNodes []*ExecutionNode
	Tensors        []*Tensor
	tensorNum      int
	OutputName     []string
}

// NewGraphBuilder returns a graph builder instance
func NewGraphBuilder(partyInfo *PartyInfo) *GraphBuilder {
	result := &GraphBuilder{
		partyInfo:      partyInfo,
		ExecutionNodes: make([]*ExecutionNode, 0),
		Tensors:        make([]*Tensor, 0),
	}
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
	t.SetStatus(it.Status())
	t.OwnerPartyCode = it.OwnerPartyCode
	t.SecretStringOwners = it.SecretStringOwners
	t.CC = it.CC.Clone()
	t.SkipDTypeCheck = it.SkipDTypeCheck
	t.IsConstScalar = it.IsConstScalar
	return t
}

// AddColumn adds a column tensor
func (plan *GraphBuilder) AddColumn(name string, status proto.TensorStatus,
	option proto.TensorOptions, dType proto.PrimitiveDataType) *Tensor {
	t := plan.AddTensor(name)
	t.SetStatus(status)
	t.Option = option
	t.DType = dType
	return t
}

func (plan *GraphBuilder) GetPartyInfo() *PartyInfo {
	return plan.partyInfo
}

type statusConstraint interface {
	Status() scql.TensorStatus
}

// CheckParamStatusConstraint check parameter status constraint strictly
func CheckParamStatusConstraint[T statusConstraint](op *operator.OperatorDef, inputs map[string][]T, outputs map[string][]T) error {
	opDef := op.GetOperatorDefProto()
	if len(opDef.InputParams) != len(inputs) {
		return fmt.Errorf("CheckParamStatusConstraint: op %v len(opDef.InputParams):%v != len(inputs):%v",
			opDef.Name, len(opDef.InputParams), len(inputs))
	}
	if len(opDef.OutputParams) != len(outputs) {
		return fmt.Errorf("CheckParamStatusConstraint: op %v len(opDef.OutputParams):%v != len(outputs):%v",
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
				return fmt.Errorf("CheckParamStatusConstraint: can't find constraint for param:%v, constraintName:%v",
					param.ParamName, param.ParameterStatusConstraintName)
			}
			found := false
			for _, s := range statusConstraint.Status {
				if args[param.ParamName][0].Status() == s {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("CheckParamStatusConstraint: invalid param status actual:%v, expected:%v", args[param.ParamName][0].Status(), statusConstraint)
			}
			constraintNameToStatus[param.ParameterStatusConstraintName] = args[param.ParamName][0].Status()
			continue
		}
		if args[param.ParamName][0].Status() != expectedStatus {
			return fmt.Errorf("param status mismatch, actual:%v, expected:%v", args[param.ParamName][0].Status(), expectedStatus)
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

	if err := CheckParamStatusConstraint(opDef, inputs, outputs); err != nil {
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

	graph.OutputNames = plan.OutputName
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

// refer to Arrow QuotingStyle: https://github.com/apache/arrow/blob/apache-arrow-14.0.0/cpp/src/arrow/csv/options.h#L174
const (
	_quotingNone     int64 = 0
	_quotingNeeded   int64 = 1
	_quotingAllValid int64 = 2
)

func (plan *GraphBuilder) AddDumpFileNode(name string, in []*Tensor, out []*Tensor, intoOpt *ast.SelectIntoOption) error {
	fp := &Attribute{}
	fp.SetString(intoOpt.FileName)
	terminator := &Attribute{}
	terminator.SetString(intoOpt.LinesInfo.Terminated)
	del := &Attribute{}
	del.SetString(intoOpt.FieldsInfo.Terminated)
	qs := &Attribute{}
	if intoOpt.FieldsInfo.Enclosed == 0 {
		qs.SetInt64(_quotingNone) // default no quotes
	} else {
		// Limitations from Arrow CSV Writer C++ API: only support (ENCLOSED BY '"') and not support (ESCAPED BY) option
		// refer to: https://github.com/apache/arrow/blob/apache-arrow-14.0.0/cpp/src/arrow/csv/options.h#L187
		if intoOpt.FieldsInfo.Enclosed != '"' {
			return fmt.Errorf("AddDumpFileNode: only support \", not support: %v", intoOpt.FieldsInfo.Enclosed)
		}
		logrus.Warn("not support 'ESCAPED BY' Option, default ignored")

		if intoOpt.FieldsInfo.OptEnclosed {
			qs.SetInt64(_quotingNeeded) // optionally enclosed valid string data
		} else {
			qs.SetInt64(_quotingAllValid) // enclosed all valid data
		}
	}
	_, err := plan.AddExecutionNode(name, operator.OpNameDumpFile,
		map[string][]*Tensor{"In": in},
		map[string][]*Tensor{"Out": out},
		map[string]*Attribute{
			operator.FilePathAttr:         fp,
			operator.FieldDeliminatorAttr: del,
			operator.QuotingStyleAttr:     qs,
			operator.LineTerminatorAttr:   terminator,
		}, []string{intoOpt.PartyCode})
	if err != nil {
		return fmt.Errorf("AddDumpFileNode: %v", err)
	}
	return nil
}

func (plan *GraphBuilder) AddCopyNode(name string, in *Tensor, inputPartyCode, outputPartyCode string) (*Tensor, error) {
	inPartyAttr := &Attribute{}
	inPartyAttr.SetString(inputPartyCode)
	outPartyAttr := &Attribute{}
	outPartyAttr.SetString(outputPartyCode)
	if !in.CC.IsVisibleFor(outputPartyCode) {
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
	if !input.CC.IsVisibleFor(revealTo) {
		return nil, fmt.Errorf("fail to check ccl: input %+v is not visible for %s when making private", input, revealTo)
	}
	output := plan.AddTensorAs(input)
	output.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	output.OwnerPartyCode = revealTo
	output.SecretStringOwners = nil
	attr := &Attribute{}
	attr.SetString(revealTo)
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakePrivate, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{operator.RevealToAttr: attr}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// MakeShare node doesn't reveal more info, so no need check ccl here
func (plan *GraphBuilder) AddMakeShareNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	output := plan.AddTensorAs(input)
	output.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
	output.OwnerPartyCode = ""
	if input.DType == proto.PrimitiveDataType_STRING {
		output.SecretStringOwners = []string{input.OwnerPartyCode}
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakeShare, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddMakePublicNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	for _, p := range partyCodes {
		if !input.CC.IsVisibleFor(p) {
			return nil, fmt.Errorf("fail to check ccl: input %+v is not visible for %s when making public", input, p)
		}
	}
	output := plan.AddTensorAs(input)
	output.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC)
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakePublic, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// AddJoinNode adds a Join node, used in EQ join
func (plan *GraphBuilder) AddJoinNode(name string, left []*Tensor, right []*Tensor, partyCodes []string, joinType int, psiAlg proto.PsiAlgorithmType) (*Tensor, *Tensor, error) {
	partyAttr := &Attribute{}
	partyAttr.SetStrings(partyCodes)
	joinTypeAttr := &Attribute{}
	joinTypeAttr.SetInt64(int64(joinType))
	psiAlgAttr := &Attribute{}
	psiAlgAttr.SetInt64(int64(psiAlg))

	inputs := make(map[string][]*Tensor)
	inputs["Left"] = left
	inputs["Right"] = right
	leftOutput := plan.AddTensorAs(left[0])
	leftOutput.DType = proto.PrimitiveDataType_INT64
	leftOutput.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	leftOutput.OwnerPartyCode = partyCodes[0]

	rightOutput := plan.AddTensorAs(right[0])
	rightOutput.DType = proto.PrimitiveDataType_INT64
	rightOutput.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	rightOutput.OwnerPartyCode = partyCodes[1]

	outputs := make(map[string][]*Tensor)
	outputs["LeftJoinIndex"] = []*Tensor{leftOutput}
	outputs["RightJoinIndex"] = []*Tensor{rightOutput}
	if _, err := plan.AddExecutionNode(name, operator.OpNameJoin, inputs, outputs,
		map[string]*Attribute{operator.InputPartyCodesAttr: partyAttr, operator.JoinTypeAttr: joinTypeAttr, operator.PsiAlgorithmAttr: psiAlgAttr}, partyCodes); err != nil {
		return nil, nil, err
	}
	return leftOutput, rightOutput, nil
}

func (plan *GraphBuilder) AddFilterByIndexNode(name string, filter *Tensor, ts []*Tensor, partyCode string) ([]*Tensor, error) {
	inputs := make(map[string][]*Tensor)
	inputs["RowsIndexFilter"] = []*Tensor{filter}
	inputs["Data"] = ts
	outputs := make([]*Tensor, len(ts))
	if !filter.CC.IsVisibleFor(partyCode) {
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

func CheckBinaryOpInputType(opType string, left, right *Tensor) error {
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
	case types.KindFloat32:
		dType = proto.PrimitiveDataType_FLOAT32
		attr.SetFloat(float32(value.GetFloat64()))
	case types.KindFloat64:
		dType = proto.PrimitiveDataType_FLOAT64
		attr.SetDouble(value.GetFloat64())
	case types.KindInt64:
		dType = proto.PrimitiveDataType_INT64
		attr.SetInt64(value.GetInt64())
	case types.KindMysqlDecimal:
		// NOTE(shunde.csd): SCQL Internal does not distinguish decimal and float,
		// It handles decimal as float.
		// If users have requirements for precision, they should use integers.
		dType = proto.PrimitiveDataType_FLOAT64
		v, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return nil, fmt.Errorf("addConstantNode: convert decimal to float error: %v", err)
		}
		attr.SetDouble(v)
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
	output.CC = cc
	output.IsConstScalar = true

	attr1 := &Attribute{}
	var statusPublic int64 = 1
	attr1.SetInt64(statusPublic)
	if _, err := plan.AddExecutionNode(name, operator.OpNameConstant, map[string][]*Tensor{},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{operator.ScalarAttr: attr, operator.ToStatusAttr: attr1}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

// AddNotNode adds a Not node
func (plan *GraphBuilder) AddNotNode(name string, input *Tensor, partyCodes []string) (*Tensor, error) {
	output := plan.AddTensorAs(input)
	if _, err := plan.AddExecutionNode(name, operator.OpNameNot, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddIsNullNode(name string, input *Tensor) (*Tensor, error) {
	if input.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
		return nil, fmt.Errorf("AddIsNullNode: only support private input now")
	}
	output := plan.AddTensorAs(input)
	output.Name = "isnull_out"
	output.DType = proto.PrimitiveDataType_BOOL
	if _, err := plan.AddExecutionNode(name, operator.OpNameIsNull, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, []string{input.OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddIfNullNode(name string, expr *Tensor, altValue *Tensor) (output *Tensor, err error) {
	if expr.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
		return nil, fmt.Errorf("AddIfNullNode: only support private expr now")
	}
	if altValue.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
		return nil, fmt.Errorf("AddIfNullNode: only support private altValue now")
	}
	if altValue.OwnerPartyCode != expr.OwnerPartyCode {
		return nil, fmt.Errorf("AddIfNullNode: altValue and expr must belong to the same party")
	}

	output = plan.AddTensorAs(expr)
	output.Name = "ifnull_out"
	if _, err := plan.AddExecutionNode(name, operator.OpNameIfNull, map[string][]*Tensor{"Expr": {expr}, "AltValue": {altValue}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, []string{expr.OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddCoalesceNode(name string, inputs []*Tensor) (*Tensor, error) {
	for i, in := range inputs {
		// TODO: check data type
		if in.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
			return nil, fmt.Errorf("AddCoalesceNode: only support private inputs now")
		}
		if i > 0 && in.OwnerPartyCode != inputs[0].OwnerPartyCode {
			return nil, fmt.Errorf("AddCoalesceNode: inputs must belong to the same party")
		}
	}

	output := plan.AddTensorAs(inputs[0])
	output.Name = "coalesce_out"
	if inputs[0].DType == proto.PrimitiveDataType_FLOAT32 {
		// Coalesce using DOUBLE as retType for FLOAT inputs, see coalesceFunctionClass.getFunction for details
		output.DType = proto.PrimitiveDataType_FLOAT64
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameCoalesce, map[string][]*Tensor{"Exprs": inputs},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, []string{inputs[0].OwnerPartyCode}); err != nil {
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
	if _, err := plan.AddExecutionNode(name, operator.OpNameCast, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
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
	if in.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{in.OwnerPartyCode}
	}
	out := plan.AddTensorAs(in)
	if aggName == ast.AggFuncAvg {
		out.DType = proto.PrimitiveDataType_FLOAT64
	} else if aggName == ast.AggFuncSum {
		if in.DType == proto.PrimitiveDataType_BOOL {
			out.DType = proto.PrimitiveDataType_INT64
		} else if IsFloatOrDoubleType(in.DType) {
			out.DType = proto.PrimitiveDataType_FLOAT64
		}
	} else if aggName == ast.AggFuncCount {
		out.DType = proto.PrimitiveDataType_INT64
	}

	if _, err := plan.AddExecutionNode("reduce_"+aggName, opType,
		map[string][]*Tensor{"In": {in}}, map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("addReduceAggNode: %v", err)
	}
	return out, nil
}

// AddShapeNode adds a Shape node
func (plan *GraphBuilder) AddShapeNode(name string, in *Tensor, axis int, partyCode string) (*Tensor, error) {
	out := plan.AddTensorAs(in)
	out.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	out.DType = proto.PrimitiveDataType_INT64
	out.OwnerPartyCode = partyCode
	attr := &Attribute{}
	attr.SetInt64(int64(axis))
	if _, err := plan.AddExecutionNode(name, operator.OpNameShape,
		map[string][]*Tensor{"In": {in}}, map[string][]*Tensor{"Out": {out}},
		map[string]*Attribute{operator.AxisAttr: attr},
		[]string{partyCode}); err != nil {
		return nil, fmt.Errorf("addShapeNode: %v", err)
	}
	return out, nil
}

func (plan *GraphBuilder) AddBroadcastToNode(name string, ins []*Tensor, shapeRefTensor *Tensor) ([]*Tensor, error) {
	partyCodes := plan.partyInfo.GetParties()
	var outs []*Tensor
	for _, in := range ins {
		out := plan.AddTensorAs(in)
		out.IsConstScalar = false
		if shapeRefTensor.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			partyCodes = []string{shapeRefTensor.OwnerPartyCode}
			out.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
			out.OwnerPartyCode = shapeRefTensor.OwnerPartyCode
		}
		outs = append(outs, out)
	}

	if _, err := plan.AddExecutionNode(name, operator.OpNameBroadcastTo, map[string][]*Tensor{"In": ins, "ShapeRefTensor": {shapeRefTensor}}, map[string][]*Tensor{"Out": outs}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("graphBuilder.AddBroadcastToNode: %v", err)
	}
	return outs, nil
}

func IsFloatOrDoubleType(tp proto.PrimitiveDataType) bool {
	return tp == proto.PrimitiveDataType_FLOAT32 || tp == proto.PrimitiveDataType_FLOAT64
}

func (plan *GraphBuilder) AddLimitNode(name string, input []*Tensor, offset int, count int, partyCodes []string) ([]*Tensor, error) {
	output := []*Tensor{}
	for _, it := range input {
		output = append(output, plan.AddTensorAs(it))
	}
	offsetAttr := &Attribute{}
	offsetAttr.SetInt64(int64(offset))
	countAttr := &Attribute{}
	countAttr.SetInt64(int64(count))
	_, err := plan.AddExecutionNode(name, operator.OpNameLimit, map[string][]*Tensor{"In": input},
		map[string][]*Tensor{"Out": output}, map[string]*Attribute{operator.LimitOffsetAttr: offsetAttr, operator.LimitCountAttr: countAttr}, partyCodes)
	if err != nil {
		return nil, fmt.Errorf("AddLimitNode: %v", err)
	}
	return output, nil
}
