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
	"encoding/base64"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/types"
	typeutil "github.com/secretflow/scql/pkg/util/type"
)

type PipelineExecNode struct {
	Batched        bool
	ExecutionNodes []*ExecutionNode
}

// GraphBuilder struct
type GraphBuilder struct {
	partyInfo *PartyInfo

	PipelineExeNodes   []*PipelineExecNode
	Tensors            []*Tensor
	tensorNum          int
	OutputName         []string
	preOpStreamingType operator.StreamingOpType
	batched            bool
}

// NewGraphBuilder returns a graph builder instance
func NewGraphBuilder(partyInfo *PartyInfo, batched bool) *GraphBuilder {
	result := &GraphBuilder{
		partyInfo: partyInfo,
		batched:   batched,
	}
	return result
}

func (plan *GraphBuilder) GetLastPipelineExeNode() *PipelineExecNode {
	return plan.PipelineExeNodes[len(plan.PipelineExeNodes)-1]
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
func (plan *GraphBuilder) AddColumn(name string, status pb.TensorStatus,
	option pb.TensorOptions, dType pb.PrimitiveDataType) *Tensor {
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
	Status() pb.TensorStatus
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

	constraintNameToStatus := map[string]pb.TensorStatus{}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, inputs, opDef.InputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	if err := checkParamStatusConstraintInternal(constraintNameToStatus, outputs, opDef.OutputParams, opDef.ParamStatusConstraints); err != nil {
		return fmt.Errorf("opName %s %v", opDef.Name, err)
	}
	return nil
}

func checkParamStatusConstraintInternal[T statusConstraint](constraintNameToStatus map[string]pb.TensorStatus,
	args map[string][]T, params []*pb.FormalParameter,
	paramStatusConstraint map[string]*pb.TensorStatusList) error {
	for _, param := range params {
		arguments, ok := args[param.ParamName]
		if !ok {
			return fmt.Errorf("can't find param:%v in arguments", param.ParamName)
		}

		if len(arguments) == 0 && param.Option == pb.FormalParameterOptions_FORMALPARAMETEROPTIONS_OPTIONAL {
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

			if !slices.Contains(statusConstraint.Status, args[param.ParamName][0].Status()) {
				return fmt.Errorf("CheckParamStatusConstraint: invalid status for param[%v] actual:%v, expected:%v", param.ParamName, args[param.ParamName][0].Status(), statusConstraint)
			}
			constraintNameToStatus[param.ParameterStatusConstraintName] = args[param.ParamName][0].Status()
		} else {
			if args[param.ParamName][0].Status() != expectedStatus {
				return fmt.Errorf("param status mismatch, actual:%v, expected:%v", args[param.ParamName][0].Status(), expectedStatus)
			}
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
	if len(plan.PipelineExeNodes) == 0 ||
		(plan.batched && len(plan.GetLastPipelineExeNode().ExecutionNodes) > 0 && plan.preOpStreamingType != opDef.GetStreamingType()) {
		plan.PipelineExeNodes = append(plan.PipelineExeNodes, &PipelineExecNode{})
	}
	curPipelineNode := plan.GetLastPipelineExeNode()
	plan.preOpStreamingType = opDef.GetStreamingType()
	curPipelineNode.ExecutionNodes = append(curPipelineNode.ExecutionNodes, node)
	if plan.batched && len(curPipelineNode.ExecutionNodes) == 1 {
		curPipelineNode.Batched = (opDef.GetStreamingType() == operator.StreamingOp)
	}
	return node, nil
}

// ToString dumps a debug string of the graph builder
func (plan *GraphBuilder) ToString() string {
	var builder strings.Builder
	fmt.Fprint(&builder, "execution plan:{")
	for _, pipelineNode := range plan.PipelineExeNodes {
		for _, node := range pipelineNode.ExecutionNodes {
			fmt.Fprintf(&builder, "%s,", node.ToString())
		}
	}

	for _, t := range plan.Tensors {
		fmt.Fprintf(&builder, "%s,", t.ToString())
	}
	fmt.Fprint(&builder, "}")
	return builder.String()
}

// fill pipeline input tensors and output tensors
func (plan *GraphBuilder) FillPipeline(graph *Graph, idToTensor map[int]*Tensor) {
	var pipelineCreatedTensors []map[int]bool
	for _, pipeline := range graph.Pipelines {
		curPipeOutputTensor := make(map[int]bool)
		curPipeInputTensor := make(map[int]bool)
		for node := range pipeline.Nodes {
			for _, ts := range node.Inputs {
				for _, t := range ts {
					// TODO: Support share tensors
					if t.Status() != pb.TensorStatus_TENSORSTATUS_PRIVATE {
						pipeline.Batched = false
					}
					curPipeInputTensor[t.ID] = true
				}
			}
			for _, ts := range node.Outputs {
				for _, t := range ts {
					// TODO: Support share tensors
					if t.Status() != pb.TensorStatus_TENSORSTATUS_PRIVATE {
						pipeline.Batched = false
					}
					curPipeOutputTensor[t.ID] = true
				}
			}
		}

		var pipelineInputTs []*Tensor
		for id := range curPipeInputTensor {
			if _, ok := curPipeOutputTensor[id]; !ok {
				tmpT := idToTensor[id]
				pipelineInputTs = append(pipelineInputTs, tmpT)
			}
		}
		// sort to be determinism
		sort.Slice(pipelineInputTs, func(i, j int) bool { return pipelineInputTs[i].ID < pipelineInputTs[j].ID })
		pipeline.InputTensors = pipelineInputTs
		pipelineCreatedTensors = append(pipelineCreatedTensors, curPipeOutputTensor)
	}

	// choose tensor created by current pipeline but consumed by downstream pipeline as current pipeline's output tensors
	for i, outputTensors := range pipelineCreatedTensors {
		pipelineOutputTensors := make(map[int]*Tensor, 0)
		for j := i + 1; j < len(graph.Pipelines); j++ {
			for _, t := range graph.Pipelines[j].InputTensors {
				if _, ok := outputTensors[t.ID]; ok {
					pipelineOutputTensors[t.ID] = t
				}
			}
		}
		for _, t := range pipelineOutputTensors {
			graph.Pipelines[i].OutputTensors = append(graph.Pipelines[i].OutputTensors, t)
		}
		// sort to be determinism
		sort.Slice(graph.Pipelines[i].OutputTensors, func(m, n int) bool {
			return graph.Pipelines[i].OutputTensors[m].ID < graph.Pipelines[i].OutputTensors[n].ID
		})
	}
}

// Build builds an execution plan dag
func (plan *GraphBuilder) Build() *Graph {
	graph := &Graph{
		Pipelines: make([]*Pipeline, 0),
		PartyInfo: plan.partyInfo,
	}
	inputTensorTo := make(map[int][]*ExecutionNode)
	outputTensorFrom := make(map[int]*ExecutionNode)
	tensorId2Tensor := make(map[int]*Tensor)
	for _, pipelineNode := range plan.PipelineExeNodes {
		graphPipeline := &Pipeline{Nodes: make(map[*ExecutionNode]bool), Batched: pipelineNode.Batched}
		// 1. create node
		for _, node := range pipelineNode.ExecutionNodes {
			node.ID = graph.NodeCnt
			node.Edges = make(map[*Edge]bool)
			graphPipeline.Nodes[node] = true
			graph.NodeCnt++
		}
		// 2. create edge
		for node := range graphPipeline.Nodes {
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
		graph.Pipelines = append(graph.Pipelines, graphPipeline)
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
	// for batched pipeline
	if plan.batched {
		plan.FillPipeline(graph, tensorId2Tensor)
	}
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

func (plan *GraphBuilder) AddInsertTableNode(name string, input []*Tensor, output []*Tensor, partyCode string, opt *core.InsertTableOption) error {
	tn := &Attribute{}
	tn.SetString(opt.TableName)
	cn := &Attribute{}
	cn.SetStrings(opt.Columns)
	_, err := plan.AddExecutionNode(name, operator.OpNameInsertTable,
		map[string][]*Tensor{"In": input},
		map[string][]*Tensor{"Out": output},
		map[string]*Attribute{operator.TableNameAttr: tn, operator.ColumnNamesAttr: cn},
		[]string{partyCode})
	return err
}

// refer to Arrow QuotingStyle: https://github.com/apache/arrow/blob/apache-arrow-14.0.0/cpp/src/arrow/csv/options.h#L174
const (
	_quotingNone     int64 = 0
	_quotingNeeded   int64 = 1
	_quotingAllValid int64 = 2
)

func (plan *GraphBuilder) AddDumpFileNodeForParty(name string, in []*Tensor, out []*Tensor, intoOpt *ast.SelectIntoOption, partyFile *ast.PartyFile) error {
	fp := &Attribute{}
	fp.SetString(partyFile.FileName)
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
		}, []string{partyFile.PartyCode})
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
	output.SetStatus(pb.TensorStatus_TENSORSTATUS_PRIVATE)
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
	output.SetStatus(pb.TensorStatus_TENSORSTATUS_SECRET)
	output.OwnerPartyCode = ""
	if input.DType == pb.PrimitiveDataType_STRING {
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
	output.SetStatus(pb.TensorStatus_TENSORSTATUS_PUBLIC)
	if _, err := plan.AddExecutionNode(name, operator.OpNameMakePublic, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}
	return output, nil
}

type JoinNodeArgs struct {
	PartyCodes     []string
	JoinType       int
	PsiAlg         pb.PsiAlgorithmType
	HasLeftResult  bool
	HasRightResult bool
}

// AddJoinNode adds a Join node, used in EQ join
func (plan *GraphBuilder) AddJoinNode(name string, left []*Tensor, right []*Tensor, joinArgs *JoinNodeArgs) (*Tensor, *Tensor, error) {
	partyAttr := &Attribute{}
	partyAttr.SetStrings(joinArgs.PartyCodes)
	joinTypeAttr := &Attribute{}
	joinTypeAttr.SetInt64(int64(joinArgs.JoinType))
	psiAlgAttr := &Attribute{}
	psiAlgAttr.SetInt64(int64(joinArgs.PsiAlg))

	inputs := make(map[string][]*Tensor)
	inputs["Left"] = left
	inputs["Right"] = right

	outputs := make(map[string][]*Tensor)
	outputs["LeftJoinIndex"] = []*Tensor{}
	outputs["RightJoinIndex"] = []*Tensor{}
	var leftOutput *Tensor = nil
	var rightOutput *Tensor = nil
	if joinArgs.HasLeftResult {
		leftOutput = plan.AddTensorAs(left[0])
		leftOutput.DType = pb.PrimitiveDataType_INT64
		leftOutput.SetStatus(pb.TensorStatus_TENSORSTATUS_PRIVATE)
		leftOutput.OwnerPartyCode = joinArgs.PartyCodes[0]
		outputs["LeftJoinIndex"] = append(outputs["LeftJoinIndex"], leftOutput)
	}
	if joinArgs.HasRightResult {
		rightOutput = plan.AddTensorAs(right[0])
		rightOutput.DType = pb.PrimitiveDataType_INT64
		rightOutput.SetStatus(pb.TensorStatus_TENSORSTATUS_PRIVATE)
		rightOutput.OwnerPartyCode = joinArgs.PartyCodes[1]
		outputs["RightJoinIndex"] = append(outputs["RightJoinIndex"], rightOutput)
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameJoin, inputs, outputs,
		map[string]*Attribute{operator.InputPartyCodesAttr: partyAttr, operator.JoinTypeAttr: joinTypeAttr, operator.PsiAlgorithmAttr: psiAlgAttr}, joinArgs.PartyCodes); err != nil {
		return nil, nil, err
	}
	return leftOutput, rightOutput, nil
}

func (plan *GraphBuilder) AddSecretJoinNode(name string, leftKeys, rightKeys, leftPayloads, rightPayloads []*Tensor, partyCodes []string) ([]*Tensor, []*Tensor, error) {
	inputs := make(map[string][]*Tensor)
	inputs["LeftKey"] = leftKeys
	inputs["RightKey"] = rightKeys
	inputs["Left"] = leftPayloads
	inputs["Right"] = rightPayloads
	var leftOutput []*Tensor
	for _, t := range leftPayloads {
		leftOutput = append(leftOutput, plan.AddTensorAs(t))
	}
	var rightOutput []*Tensor
	for _, t := range rightPayloads {
		rightOutput = append(rightOutput, plan.AddTensorAs(t))
	}

	outputs := make(map[string][]*Tensor)
	outputs["LeftOutput"] = leftOutput
	outputs["RightOutput"] = rightOutput
	if _, err := plan.AddExecutionNode(name, operator.OpNameSecretJoin, inputs, outputs,
		map[string]*Attribute{}, partyCodes); err != nil {
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
		((left.DType != pb.PrimitiveDataType_INT64 && left.DType != pb.PrimitiveDataType_DATETIME && left.DType != pb.PrimitiveDataType_TIMESTAMP) ||
			(right.DType != pb.PrimitiveDataType_INT64)) {
		return status.Wrap(pb.Code_NOT_SUPPORTED, fmt.Errorf("op %v requires both left and right operands be int64", opType))
	}
	if _, ok := strTypeUnsupportedOpM[opType]; ok &&
		(left.DType == pb.PrimitiveDataType_STRING || right.DType == pb.PrimitiveDataType_STRING) {
		return status.Wrap(pb.Code_NOT_SUPPORTED, fmt.Errorf("op %v doesn't support input type %v", opType, pb.PrimitiveDataType_STRING))
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
func (plan *GraphBuilder) AddVariadicCompareNode(name string, opType string, inputs []*Tensor, parties []string) (*Tensor, error) {
	output := plan.AddTensorAs(inputs[0])
	output.SkipDTypeCheck = true
	isFirstTensorTimestamp := typeutil.IsTimeType(inputs[0].DType)

	for _, tensor := range inputs {
		if !typeutil.IsNumericDtype(tensor.DType) && !typeutil.IsTimeType(tensor.DType) {
			return nil, fmt.Errorf("AddVariadicCompareNode: unsupported dtype %v, only support numeric types or timestamp", tensor.DType)
		}

		if (isFirstTensorTimestamp && typeutil.IsNumericDtype(tensor.DType)) || (!isFirstTensorTimestamp && typeutil.IsTimeType(tensor.DType)) {
			return nil, fmt.Errorf("AddVariadicCompareNode: timestamp and numeric type cannot be mixed")
		}

		output.CC.UpdateMoreRestrictedCCLFrom(tensor.CC)

		if !isFirstTensorTimestamp {
			dType, err := typeutil.GetWiderType(tensor.DType, output.DType)
			if err != nil {
				return nil, fmt.Errorf("AddVariadicCompareNode: %v", err)
			}

			output.DType = dType
		}
	}

	_, err := plan.AddExecutionNode(name, opType, map[string][]*Tensor{In: inputs}, map[string][]*Tensor{Out: {output}}, map[string]*Attribute{}, parties)
	if err != nil {
		return nil, fmt.Errorf("AddVariadicCompareNode: %v", err)
	}
	return output, nil
}

func (plan *GraphBuilder) AddConstantNode(name string, value *types.Datum, partyCodes []string) (*Tensor, error) {
	var dType pb.PrimitiveDataType
	attr := &Attribute{}
	switch value.Kind() {
	case types.KindFloat32:
		dType = pb.PrimitiveDataType_FLOAT32
		attr.SetFloat(float32(value.GetFloat64()))
	case types.KindFloat64:
		dType = pb.PrimitiveDataType_FLOAT64
		attr.SetDouble(value.GetFloat64())
	case types.KindInt64:
		dType = pb.PrimitiveDataType_INT64
		attr.SetInt64(value.GetInt64())
	case types.KindMysqlDecimal:
		// NOTE(shunde.csd): SCQL Internal does not distinguish decimal and float,
		// It handles decimal as float.
		// If users have requirements for precision, they should use integers.
		dType = pb.PrimitiveDataType_FLOAT64
		v, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return nil, fmt.Errorf("addConstantNode: convert decimal to float error: %v", err)
		}
		attr.SetDouble(v)
	case types.KindString:
		dType = pb.PrimitiveDataType_STRING
		attr.SetString(value.GetString())
	case types.KindMysqlTime:
		dType = pb.PrimitiveDataType_DATETIME
		time := value.GetMysqlTime()

		timestamp := time.CoreTime().ToUnixTimestamp()

		attr.SetInt64(timestamp)
	default:
		return nil, fmt.Errorf("addConstantNode: unsupported data{%+v}", value)
	}
	// constant in query can be seen by all party
	cc := ccl.NewCCL()
	for _, p := range partyCodes {
		cc.SetLevelForParty(p, ccl.Plain)
	}

	output := plan.AddColumn(
		"constant_data", pb.TensorStatus_TENSORSTATUS_PUBLIC,
		pb.TensorOptions_REFERENCE, dType)
	output.CC = cc
	output.IsConstScalar = true

	if _, err := plan.AddExecutionNode(name, operator.OpNameConstant, map[string][]*Tensor{},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{operator.ScalarAttr: attr}, partyCodes); err != nil {
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
	output := plan.AddTensorAs(input)
	output.Name = "isnull_out"
	output.DType = pb.PrimitiveDataType_BOOL
	if _, err := plan.AddExecutionNode(name, operator.OpNameIsNull, map[string][]*Tensor{"In": {input}},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, []string{input.OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddArrowFuncNode(nodeName, funcName string, funcOpt compute.FunctionOptions, inputs []*Tensor, outputType pb.PrimitiveDataType) (*Tensor, error) {
	for i, input := range inputs {
		// TODO: support constant input
		if input.Status() != pb.TensorStatus_TENSORSTATUS_PRIVATE {
			return nil, fmt.Errorf("AddArrowFuncNode: only support private input now, illegal input {%v}", input)
		}
		if i > 0 && input.OwnerPartyCode != inputs[0].OwnerPartyCode {
			return nil, fmt.Errorf("AddArrowFuncNode: inputs must belong to the same party")
		}
	}
	output := plan.AddTensorAs(inputs[0])
	output.Name = nodeName + "_out"
	output.DType = outputType

	attrs := make(map[string]*Attribute)
	nameAttr := &Attribute{}
	nameAttr.SetString(funcName)
	attrs[operator.FuncNameAttr] = nameAttr
	if funcOpt != nil {
		optTypeAttr := &Attribute{}
		optTypeAttr.SetString(funcOpt.TypeName())
		attrs[operator.FuncOptTypeAttr] = optTypeAttr

		buf, err := compute.SerializeOptions(funcOpt, memory.DefaultAllocator)
		if err != nil {
			return nil, fmt.Errorf("AddArrowFuncNode: serialize func options failed: %v", err)
		}
		optAttr := &Attribute{}
		// encode to base64, attribute not support bytes yet.
		optAttr.SetString(base64.StdEncoding.EncodeToString(buf.Bytes()))
		attrs[operator.FuncOptAttr] = optAttr
	}
	if _, err := plan.AddExecutionNode(nodeName, operator.OpNameArrowFunc, map[string][]*Tensor{"In": inputs},
		map[string][]*Tensor{"Out": {output}}, attrs, []string{inputs[0].OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddIfNullNode(name string, expr *Tensor, altValue *Tensor) (output *Tensor, err error) {
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
		if in.Status() != pb.TensorStatus_TENSORSTATUS_PRIVATE {
			return nil, fmt.Errorf("AddCoalesceNode: only support private inputs now")
		}
		if i > 0 && in.OwnerPartyCode != inputs[0].OwnerPartyCode {
			return nil, fmt.Errorf("AddCoalesceNode: inputs must belong to the same party")
		}
	}

	output := plan.AddTensorAs(inputs[0])
	output.Name = "coalesce_out"
	if inputs[0].DType == pb.PrimitiveDataType_FLOAT32 {
		// Coalesce using DOUBLE as retType for FLOAT inputs, see coalesceFunctionClass.getFunction for details
		output.DType = pb.PrimitiveDataType_FLOAT64
	}
	if _, err := plan.AddExecutionNode(name, operator.OpNameCoalesce, map[string][]*Tensor{"Exprs": inputs},
		map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, []string{inputs[0].OwnerPartyCode}); err != nil {
		return nil, err
	}
	return output, nil
}

func (plan *GraphBuilder) AddTrigonometricFunction(opName string, opType string, input *Tensor, partyCodes []string) (*Tensor, error) {
	if !typeutil.IsNumericDtype(input.DType) {
		return nil, fmt.Errorf("AddTrigonometricFunction: only support numeric data type, but get %v", input.DType.String())
	}
	output := plan.AddTensorAs(input)
	output.DType = pb.PrimitiveDataType_FLOAT64
	if _, err := plan.AddExecutionNode(opName, opType, map[string][]*Tensor{"In": {input}}, map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}

	return output, nil
}

func (plan *GraphBuilder) AddUnaryNumericNode(name string, opType string, input *Tensor, outputType pb.PrimitiveDataType, partyCodes []string) (*Tensor, error) {
	if !typeutil.IsNumericDtype(input.DType) {
		return nil, fmt.Errorf("AddUnaryNumericNode: only support numeric data type, but get %v", input.DType.String())
	}

	output := plan.AddTensorAs(input)
	output.DType = outputType
	if _, err := plan.AddExecutionNode(name, opType, map[string][]*Tensor{"In": {input}}, map[string][]*Tensor{"Out": {output}}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, err
	}

	return output, nil
}

func (plan *GraphBuilder) AddCastNode(name string, outputDType pb.PrimitiveDataType, input *Tensor, partyCodes []string) (*Tensor, error) {
	if len(partyCodes) > 1 && (input.DType == pb.PrimitiveDataType_STRING || outputDType == pb.PrimitiveDataType_STRING) {
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
func (plan *GraphBuilder) AddReduceAggNode(aggName string, in *Tensor, attr map[string]*Attribute) (*Tensor, error) {
	opType, ok := operator.ReduceAggOp[aggName]
	if !ok {
		return nil, fmt.Errorf("AddReduceAggNode: unsupported aggregation fucntion %v", aggName)
	}
	partyCodes := plan.partyInfo.GetParties()
	if in.Status() == pb.TensorStatus_TENSORSTATUS_PRIVATE {
		partyCodes = []string{in.OwnerPartyCode}
	}
	out := plan.AddTensorAs(in)
	if aggName == ast.AggFuncAvg {
		out.DType = pb.PrimitiveDataType_FLOAT64
	} else if aggName == ast.AggFuncSum {
		if in.DType == pb.PrimitiveDataType_BOOL {
			out.DType = pb.PrimitiveDataType_INT64
		} else if IsFloatOrDoubleType(in.DType) {
			out.DType = pb.PrimitiveDataType_FLOAT64
		}
	} else if aggName == ast.AggFuncCount {
		out.DType = pb.PrimitiveDataType_INT64
	}

	if _, err := plan.AddExecutionNode("reduce_"+aggName, opType,
		map[string][]*Tensor{"In": {in}}, map[string][]*Tensor{"Out": {out}},
		attr, partyCodes); err != nil {
		return nil, fmt.Errorf("AddReduceAggNode: %v", err)
	}
	return out, nil
}

// AddShapeNode adds a Shape node
func (plan *GraphBuilder) AddShapeNode(name string, in *Tensor, axis int, partyCode string) (*Tensor, error) {
	out := plan.AddTensorAs(in)
	out.SetStatus(pb.TensorStatus_TENSORSTATUS_PRIVATE)
	out.DType = pb.PrimitiveDataType_INT64
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
		if shapeRefTensor.Status() == pb.TensorStatus_TENSORSTATUS_PRIVATE {
			partyCodes = []string{shapeRefTensor.OwnerPartyCode}
			out.SetStatus(pb.TensorStatus_TENSORSTATUS_PRIVATE)
			out.OwnerPartyCode = shapeRefTensor.OwnerPartyCode
		}
		outs = append(outs, out)
	}

	if _, err := plan.AddExecutionNode(name, operator.OpNameBroadcastTo, map[string][]*Tensor{"In": ins, "ShapeRefTensor": {shapeRefTensor}}, map[string][]*Tensor{"Out": outs}, map[string]*Attribute{}, partyCodes); err != nil {
		return nil, fmt.Errorf("AddBroadcastToNode: %v", err)
	}
	return outs, nil
}

func IsFloatOrDoubleType(tp pb.PrimitiveDataType) bool {
	return tp == pb.PrimitiveDataType_FLOAT32 || tp == pb.PrimitiveDataType_FLOAT64
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

// AddReplicateNode adds a Replicate node, used in cross join
func (plan *GraphBuilder) AddReplicateNode(name string, left []*Tensor,
	right []*Tensor, leftOutput []*Tensor, rightOutput []*Tensor,
	attr map[string]*Attribute, partyCodes []string) (*ExecutionNode, error) {
	if len(left) == 0 || len(right) == 0 {
		return nil, fmt.Errorf("AddReplicateNode: left and right should not be empty")
	}

	if len(left) != len(leftOutput) || len(right) != len(rightOutput) {
		return nil, fmt.Errorf("AddReplicateNode: input(%v/%v) should have same length with output(%v/%v)", len(left), len(right), len(leftOutput), len(rightOutput))
	}

	leftFirstTensor := left[0]
	if leftFirstTensor.status != pb.TensorStatus_TENSORSTATUS_PRIVATE {
		return nil, fmt.Errorf("AddReplicateNode: all input tensors should be private")
	}

	for _, tensor := range left[1:] {
		if tensor.OwnerPartyCode != leftFirstTensor.OwnerPartyCode {
			return nil, fmt.Errorf("AddReplicateNode: all input tensors should be from the same party")
		}
		if tensor.status != pb.TensorStatus_TENSORSTATUS_PRIVATE {
			return nil, fmt.Errorf("AddReplicateNode: all input tensors should be private")
		}
	}

	rightFirstTensor := right[0]

	for _, tensor := range right[1:] {
		if tensor.OwnerPartyCode != rightFirstTensor.OwnerPartyCode {
			return nil, fmt.Errorf("AddReplicateNode: all input tensors should be from the same party")
		}
		if tensor.status != pb.TensorStatus_TENSORSTATUS_PRIVATE {
			return nil, fmt.Errorf("AddReplicateNode: all input tensors should be private")
		}
	}
	return plan.AddExecutionNode(name, operator.OpNameReplicate, map[string][]*Tensor{"Left": left, "Right": right},
		map[string][]*Tensor{"LeftOut": leftOutput, "RightOut": rightOutput}, attr, partyCodes)
}
