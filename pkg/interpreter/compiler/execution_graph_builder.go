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
	"sort"
	"strings"

	"maps"
	"slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
)

type ExecutionGraphBuilder struct {
	pipelineEngineNodes []*PipelineExecNode
	tensorMetaManager   *TensorMetaManager
	srm                 *SecurityRelaxationManager
	vt                  *VisibilityTable
	tm                  *TensorManager
	info                *graph.EnginesInfo

	outputNames []string

	preOpStreamingType operator.StreamingOpType
	compileOpts        *v1.CompileOptions
}

type PipelineExecNode struct {
	Batched        bool
	ExecutionNodes []*graph.ExecutionNode
}

func NewExecutionGraphBuilder(tensorMetaManager *TensorMetaManager, srm *SecurityRelaxationManager, vt *VisibilityTable, info *graph.EnginesInfo, compileOpts *v1.CompileOptions) *ExecutionGraphBuilder {
	builder := &ExecutionGraphBuilder{
		tensorMetaManager: tensorMetaManager,
		srm:               srm,
		vt:                vt,
		// make nextTensorID == {the number of existing tensor metas} + 1,
		// then we can keep tensor id for tensors which are the first placed tensor for their corresponding tensor meta
		tm:          NewTensorManager(tensorMetaManager.tensorNum + 1),
		info:        info,
		compileOpts: compileOpts,
	}
	return builder
}

func (builder *ExecutionGraphBuilder) GetAllParties() []string {
	return builder.vt.allParties
}

func (builder *ExecutionGraphBuilder) SetBatched(batched bool) {
	if builder.compileOpts != nil {
		builder.compileOpts.Batched = batched
	}
}

func (builder *ExecutionGraphBuilder) IsBatched() bool {
	if builder.compileOpts != nil {
		return builder.compileOpts.Batched
	}
	return false
}

func (builder *ExecutionGraphBuilder) TensorManager() *TensorManager {
	return builder.tm
}

func (builder *ExecutionGraphBuilder) DumpPlan() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Execution Graph:\n")
	for _, pipeline := range builder.pipelineEngineNodes {
		for _, node := range pipeline.ExecutionNodes {
			fmt.Fprintf(&sb, "%s\n", node.ToString())
		}
	}
	return sb.String()
}

// Build converts the OperatorGraph into ExecutionGraph.
func (builder *ExecutionGraphBuilder) Build(operatorGraph *OperatorGraph) (*graph.Graph, error) {
	kr := NewKernelResolver(builder.srm, builder.vt, builder.tm, builder.compileOpts)
	for _, node := range operatorGraph.operators {
		kernel, err := kr.Resolve(node)
		if err != nil {
			return nil, fmt.Errorf("Build Execution Graph failed: %v", err)
		}
		logrus.Debugf("Processing node: %s, kernel: %s", node.String(), kernel.String())
		node.SetKernel(kernel)
		if err := builder.addKernel(kernel, node); err != nil {
			return nil, fmt.Errorf("Build Execution Graph failed: %v", err)
		}
	}

	return builder.buildGraph()
}

func (builder *ExecutionGraphBuilder) addKernel(kernel Kernel, node Operator) error {
	if err := kernel.ensureTensorPlace(builder, node); err != nil {
		return fmt.Errorf("addKernel: %v", err)
	}
	if err := kernel.toEngineNodes(builder, node); err != nil {
		return fmt.Errorf("addKernel: %v", err)
	}
	return nil
}

// addEngineNode creates an execution node with the specified operation and adds it to the current pipeline.
// It handles pipeline batching decisions based on operation streaming type and manages tensor flow between nodes.
func (builder *ExecutionGraphBuilder) addEngineNode(name string, opType string, inputs map[string][]*graph.Tensor, outputs map[string][]*graph.Tensor,
	attributes map[string]*graph.Attribute, partyCodes []string) error {
	opDef, err := operator.FindOpDef(opType)
	if err != nil {
		return fmt.Errorf("addEngineNode: %v", err)
	}

	if err := operator.CheckParamStatusConstraint(opDef, inputs, outputs); err != nil {
		return fmt.Errorf("addEngineNode: %v", err)
	}

	node := &graph.ExecutionNode{
		Name:       name,
		OpType:     opType,
		Inputs:     make(map[string][]*graph.Tensor),
		Outputs:    make(map[string][]*graph.Tensor),
		Attributes: make(map[string]*graph.Attribute),
		Parties:    partyCodes,
	}
	for k, is := range inputs {
		node.Inputs[k] = slices.Clone(is)
	}
	for k, os := range outputs {
		node.Outputs[k] = slices.Clone(os)
	}
	maps.Copy(node.Attributes, attributes)

	for k, defaultAttr := range opDef.GetDefaultAttribute() {
		if _, ok := node.Attributes[k]; !ok {
			node.Attributes[k] = &graph.Attribute{
				TensorValue: graph.NewTensorFromProto(defaultAttr.GetT()),
			}
		}
	}

	if len(builder.pipelineEngineNodes) == 0 ||
		(builder.IsBatched() && len(builder.getLastPipelineOperator().ExecutionNodes) > 0 && builder.preOpStreamingType != opDef.GetStreamingType()) {
		builder.pipelineEngineNodes = append(builder.pipelineEngineNodes, &PipelineExecNode{})
	}
	curPipelineNode := builder.getLastPipelineOperator()
	builder.preOpStreamingType = opDef.GetStreamingType()
	curPipelineNode.ExecutionNodes = append(curPipelineNode.ExecutionNodes, node)
	if builder.IsBatched() && len(curPipelineNode.ExecutionNodes) == 1 {
		curPipelineNode.Batched = (opDef.GetStreamingType() == operator.StreamingOp)
	}
	return nil
}

// prepareResultForParty creates tensors for party-specific result handling.
// It generates input tensors with private placement and output tensors for result names.
func (builder *ExecutionGraphBuilder) prepareResultForParty(originResults []*TensorMeta, resultNames []string, partyCode string) (inputs, outputs []*graph.Tensor, err error) {
	if len(originResults) != len(resultNames) {
		return nil, nil, fmt.Errorf("prepareResultForParty: originResults length:%d != resultNames length:%d", len(originResults), len(resultNames))
	}
	for idx, tensor := range originResults {
		input, err := builder.getOrCreatePlacedTensor(tensor, &privatePlacement{partyCode: partyCode})
		if err != nil {
			return nil, nil, fmt.Errorf("prepareResultForParty: %w", err)
		}
		inputs = append(inputs, input)

		output := builder.tm.CreateResultTensor(input, resultNames[idx])
		outputs = append(outputs, output)
	}
	return
}

// buildGraph constructs the ExecutionGraph represented as a graph structure.
// It organizes ExecutionNodes into pipelines and establishes tensor flow relationships between nodes.
func (builder *ExecutionGraphBuilder) buildGraph() (*graph.Graph, error) {
	plan := &graph.Graph{
		Pipelines: make([]*graph.Pipeline, 0),
		PartyInfo: builder.info.GetPartyInfo(),
	}

	tensorUsers := make(map[int][]*graph.ExecutionNode)
	tensorSource := make(map[int]*graph.ExecutionNode)
	tensorId2Tensor := make(map[int]*graph.Tensor)

	for _, pipelineNode := range builder.pipelineEngineNodes {
		graphPipeline := &graph.Pipeline{Nodes: make(map[*graph.ExecutionNode]bool), Batched: pipelineNode.Batched}
		// 1. create node
		for _, node := range pipelineNode.ExecutionNodes {
			node.ID = plan.NodeCnt
			node.Edges = make(map[*graph.Edge]bool)
			graphPipeline.Nodes[node] = true
			plan.NodeCnt++
		}
		// 2. create edge
		// 2.1 collect tensor source and users
		for node := range graphPipeline.Nodes {
			for _, ts := range node.Inputs {
				for _, t := range ts {
					tensorId2Tensor[t.ID] = t
					_, ok := tensorUsers[t.ID]
					if !ok {
						tensorUsers[t.ID] = make([]*graph.ExecutionNode, 0)
					}
					tensorUsers[t.ID] = append(tensorUsers[t.ID], node)
				}
			}
			for _, ts := range node.Outputs {
				for _, t := range ts {
					tensorId2Tensor[t.ID] = t
					if sourceNode, ok := tensorSource[t.ID]; ok {
						if sourceNode != node {
							return nil, fmt.Errorf("buildGraph: find more than one source for tensor %d, source node %s, current node %s", t.ID, sourceNode.Name, node.Name)
						}
					}
					tensorSource[t.ID] = node
				}
			}
		}
		plan.Pipelines = append(plan.Pipelines, graphPipeline)
	}

	// 2.2 create edge and set nodes' output edges
	for k, v := range tensorId2Tensor {
		for _, user := range tensorUsers[k] {
			edge := &graph.Edge{
				From:  tensorSource[k],
				To:    user,
				Value: v,
			}
			tensorSource[k].Edges[edge] = true
		}
	}

	plan.OutputNames = builder.outputNames

	// for batched pipeline
	if builder.IsBatched() {
		builder.fillPipeline(plan, tensorId2Tensor)
	}

	return plan, nil
}

// fillPipeline determines input/output tensors for each pipeline by analyzing tensor flow across pipelines.
// It identifies tensors created in one pipeline but consumed by downstream pipelines to set pipeline boundaries.
func (builder *ExecutionGraphBuilder) fillPipeline(plan *graph.Graph, idToTensor map[int]*graph.Tensor) {
	var pipelineCreatedTensors []map[int]bool
	for _, pipeline := range plan.Pipelines {
		curPipeOutputTensor := make(map[int]bool)
		curPipeInputTensor := make(map[int]bool)
		for node := range pipeline.Nodes {
			for _, ts := range node.Inputs {
				for _, t := range ts {
					// TODO: Support share tensors
					if t.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
						pipeline.Batched = false
					}
					curPipeInputTensor[t.ID] = true
				}
			}
			for _, ts := range node.Outputs {
				for _, t := range ts {
					// TODO: Support share tensors
					if t.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
						pipeline.Batched = false
					}
					curPipeOutputTensor[t.ID] = true
				}
			}
		}

		var pipelineInputTs []*graph.Tensor
		for tid := range curPipeInputTensor {
			if _, ok := curPipeOutputTensor[tid]; !ok {
				tmpT := idToTensor[tid]
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
		pipelineOutputTensors := make(map[int]*graph.Tensor, 0)
		for j := i + 1; j < len(plan.Pipelines); j++ {
			for _, t := range plan.Pipelines[j].InputTensors {
				if _, ok := outputTensors[t.ID]; ok {
					pipelineOutputTensors[t.ID] = t
				}
			}
		}
		for _, t := range pipelineOutputTensors {
			plan.Pipelines[i].OutputTensors = append(plan.Pipelines[i].OutputTensors, t)
		}
		// sort to be determinism
		sort.Slice(plan.Pipelines[i].OutputTensors, func(m, n int) bool {
			return plan.Pipelines[i].OutputTensors[m].ID < plan.Pipelines[i].OutputTensors[n].ID
		})
	}
}

func (builder *ExecutionGraphBuilder) getLastPipelineOperator() *PipelineExecNode {
	return builder.pipelineEngineNodes[len(builder.pipelineEngineNodes)-1]
}
