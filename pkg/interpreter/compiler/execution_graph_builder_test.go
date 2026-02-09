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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
)

func createTestHelp() (*ExecutionGraphBuilder, func(string, proto.PrimitiveDataType, tensorPlacement, []string) (*TensorMeta, *graph.Tensor)) {
	tmm := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob"})
	info := &graph.EnginesInfo{}

	builder := NewExecutionGraphBuilder(tmm, srm, vt, info, &v1.CompileOptions{Batched: false})

	createPlacedTensor := func(name string, dtype proto.PrimitiveDataType, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := builder.tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		builder.vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := builder.tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	return builder, createPlacedTensor
}

func TestExecutionGraphBuilderBasicMethods(t *testing.T) {
	r := require.New(t)

	tmm := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	info := &graph.EnginesInfo{}

	builder := NewExecutionGraphBuilder(tmm, srm, vt, info, &v1.CompileOptions{Batched: false})

	// Test GetAllParties
	parties := builder.GetAllParties()
	r.Equal([]string{"alice", "bob", "carol"}, parties)

	// Test SetBatched and IsBatched
	r.False(builder.IsBatched())
	builder.SetBatched(true)
	r.True(builder.IsBatched())
	builder.SetBatched(false)
	r.False(builder.IsBatched())

	// Test TensorManager
	r.Equal(builder.tm, builder.TensorManager())
}

func TestAddEngineNode(t *testing.T) {
	r := require.New(t)

	builder, createPlacedTensor := createTestHelp()

	// Create test tensors
	_, inputTensor1 := createPlacedTensor("test_input1", proto.PrimitiveDataType_INT64, &secretPlacement{}, []string{})
	_, inputTensor2 := createPlacedTensor("test_input2", proto.PrimitiveDataType_INT64, &secretPlacement{}, []string{})
	_, outputTensor := createPlacedTensor("test_output", proto.PrimitiveDataType_INT64, &secretPlacement{}, []string{})

	inputs := map[string][]*graph.Tensor{
		"Left":  {inputTensor1},
		"Right": {inputTensor2},
	}
	outputs := map[string][]*graph.Tensor{
		"Out": {outputTensor},
	}
	attributes := make(map[string]*graph.Attribute)
	partyCodes := builder.GetAllParties()

	// Test adding a valid operation
	err := builder.addEngineNode("node_name", operator.OpNameAdd, inputs, outputs, attributes, partyCodes)
	r.NoError(err)
	r.Len(builder.pipelineEngineNodes, 1)
	r.Len(builder.pipelineEngineNodes[0].ExecutionNodes, 1)
	r.Equal("node_name", builder.pipelineEngineNodes[0].ExecutionNodes[0].Name)
	r.Equal(operator.OpNameAdd, builder.pipelineEngineNodes[0].ExecutionNodes[0].OpType)

	// Test adding an invalid operation
	err = builder.addEngineNode("node_name2", "invalid_op", inputs, outputs, attributes, partyCodes)
	r.Error(err)
	r.Contains(err.Error(), "failed to find opType")
}

func TestBuildGraph(t *testing.T) {
	r := require.New(t)

	builder, createPlacedTensor := createTestHelp()

	// Create tensors
	_, tensor1 := createPlacedTensor("tensor1", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{})
	_, tensor2 := createPlacedTensor("tensor2", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{})
	_, tensor3 := createPlacedTensor("tensor3", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	// For test here, we only fill the input and output tensors, the attributes are empty
	// Add RunSQL node
	err := builder.addEngineNode("runsql", operator.OpNameRunSQL,
		map[string][]*graph.Tensor{},
		map[string][]*graph.Tensor{"Out": {tensor1}},
		make(map[string]*graph.Attribute),
		builder.GetAllParties())
	r.NoError(err)

	// Add Sort node
	err = builder.addEngineNode("sort", operator.OpNameSort,
		map[string][]*graph.Tensor{"In": {tensor1}, "Key": {tensor1}},
		map[string][]*graph.Tensor{"Out": {tensor2}},
		make(map[string]*graph.Attribute),
		builder.GetAllParties())
	r.NoError(err)

	// Add Limit node
	err = builder.addEngineNode("limit", operator.OpNameLimit,
		map[string][]*graph.Tensor{"In": {tensor2}},
		map[string][]*graph.Tensor{"Out": {tensor3}},
		make(map[string]*graph.Attribute),
		builder.GetAllParties())
	r.NoError(err)

	// Verify pipeline is properly populated
	r.Len(builder.pipelineEngineNodes, 1)
	r.Len(builder.pipelineEngineNodes[0].ExecutionNodes, 3)

	// Build the graph
	g, err := builder.buildGraph()
	r.NoError(err)
	r.NotNil(g)
	r.Len(g.Pipelines, 1)
	r.Equal(3, g.NodeCnt)
	r.Len(g.Pipelines[0].Nodes, 3)
}

func TestPrepareResultForParty(t *testing.T) {
	r := require.New(t)

	builder, createPlacedTensor := createTestHelp()

	// Create test tensors
	ut1, _ := createPlacedTensor("result1", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{"alice"})
	ut2, _ := createPlacedTensor("result2", proto.PrimitiveDataType_STRING, &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	originResults := []*TensorMeta{
		ut1,
		ut2,
	}
	resultNames := []string{"count", "id"}
	partyCode := "alice"

	// Test prepareResultForParty
	inputs, outputs, err := builder.prepareResultForParty(originResults, resultNames, partyCode)
	r.NoError(err)
	r.Len(inputs, 2)
	r.Len(outputs, 2)
	r.Equal("count", outputs[0].Name)
	r.Equal("id", outputs[1].Name)
	r.Equal(proto.PrimitiveDataType_STRING, outputs[0].DType.DType)
	r.Equal(proto.PrimitiveDataType_STRING, outputs[1].DType.DType)
}

func TestPrepareResultForPartyLengthMismatch(t *testing.T) {
	r := require.New(t)

	builder, createPlacedTensor := createTestHelp()

	meta, _ := createPlacedTensor("result1", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{"alice"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	originResults := []*TensorMeta{
		meta,
	}
	resultNames := []string{"count", "id"} // Length mismatch
	partyCode := "alice"

	// Test prepareResultForParty with length mismatch
	inputs, outputs, err := builder.prepareResultForParty(originResults, resultNames, partyCode)
	r.Error(err)
	r.Contains(err.Error(), "length")
	r.Nil(inputs)
	r.Nil(outputs)
}

func TestPrepareResultForPartyInvisible(t *testing.T) {
	r := require.New(t)

	builder, createPlacedTensor := createTestHelp()

	// Create tensors with visibility that doesn't include the target party
	ut1, _ := createPlacedTensor("result1", proto.PrimitiveDataType_INT64, &privatePlacement{partyCode: "alice"}, []string{"alice"})
	ut2, _ := createPlacedTensor("result2", proto.PrimitiveDataType_STRING, &privatePlacement{partyCode: "bob"}, []string{"bob"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	originResults := []*TensorMeta{
		ut1,
		ut2,
	}
	resultNames := []string{"count", "id"}
	partyCode := "alice" // alice is not in the visibility of bob or carol's tensors

	// Test prepareResultForParty with visibility mismatch
	inputs, outputs, err := builder.prepareResultForParty(originResults, resultNames, partyCode)
	r.Error(err)
	r.Contains(err.Error(), "prepareResultForParty")
	r.Nil(inputs)
	r.Nil(outputs)
}
