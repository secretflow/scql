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
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// Helper to create test tensors
func createTestTensor(id int, name string, refNum int32) *Tensor {
	return &Tensor{
		ID:     id,
		Name:   name,
		DType:  NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
		status: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		RefNum: refNum,
	}
}

// Helper to create test execution nodes
func createTestExecutionNode(id int, name string, inputs, outputs []*Tensor) *ExecutionNode {
	node := &ExecutionNode{
		ID:         id,
		Name:       name,
		OpType:     "TestOp",
		Inputs:     make(map[string][]*Tensor),
		Outputs:    make(map[string][]*Tensor),
		Attributes: make(map[string]*Attribute),
		Edges:      make(map[*Edge]bool),
		Parties:    []string{"alice", "bob"},
	}

	if len(inputs) > 0 {
		node.Inputs["input"] = inputs
	}
	if len(outputs) > 0 {
		node.Outputs["output"] = outputs
	}

	return node
}

// Helper to create test edges
func createTestEdge(from, to *ExecutionNode, value *Tensor) *Edge {
	edge := &Edge{
		From:  from,
		To:    to,
		Value: value,
	}
	from.Edges[edge] = true
	return edge
}

// Helper to create test pipeline
func createTestPipeline(nodes []*ExecutionNode, batched bool) *Pipeline {
	nodeMap := make(map[*ExecutionNode]bool)
	for _, node := range nodes {
		nodeMap[node] = true
	}

	return &Pipeline{
		Batched:       batched,
		InputTensors:  []*Tensor{},
		OutputTensors: []*Tensor{},
		Nodes:         nodeMap,
	}
}

// Helper to create test graph with party info
func createTestGraph(pipelines []*Pipeline) *Graph {
	partyInfo := NewPartyInfo([]*Participant{
		{PartyCode: "alice", Endpoints: []string{"alice.com"}, Token: "token"},
		{PartyCode: "bob", Endpoints: []string{"bob.com"}, Token: "token"},
	})

	return &Graph{
		Pipelines:   pipelines,
		NodeCnt:     0,
		OutputNames: []string{},
		PartyInfo:   partyInfo,
	}
}

func TestGraphUpdateTensorRefNum(t *testing.T) {
	r := require.New(t)

	// Test basic reference counting
	t1 := createTestTensor(1, "t1", 0)
	t2 := createTestTensor(2, "t2", 0)
	t3 := createTestTensor(3, "t3", 0)

	nodeA := createTestExecutionNode(1, "A", nil, []*Tensor{t1})
	nodeB := createTestExecutionNode(2, "B", []*Tensor{t1}, []*Tensor{t2})
	nodeC := createTestExecutionNode(3, "C", []*Tensor{t1, t2}, []*Tensor{t3})

	pipeline := createTestPipeline([]*ExecutionNode{nodeA, nodeB, nodeC}, false)
	graph := createTestGraph([]*Pipeline{pipeline})

	// Verify initial RefNum
	r.Equal(int32(0), t1.RefNum)
	r.Equal(int32(0), t2.RefNum)
	r.Equal(int32(0), t3.RefNum)

	// Update reference numbers
	graph.UpdateTensorRefNum()

	// t1 is input to B and C = 2 references
	// t2 is input to C = 1 reference
	// t3 has no consumers = 0 reference
	r.Equal(int32(2), t1.RefNum)
	r.Equal(int32(1), t2.RefNum)
	r.Equal(int32(0), t3.RefNum)
}

func TestGraphUpdateTensorRefNumMultiplePipelines(t *testing.T) {
	r := require.New(t)

	// Create two pipelines with shared tensors
	t1 := createTestTensor(1, "t1", 0)
	t2 := createTestTensor(2, "t2", 0)

	// Pipeline 1
	nodeA1 := createTestExecutionNode(1, "A1", nil, []*Tensor{t1})
	nodeB1 := createTestExecutionNode(2, "B1", []*Tensor{t1}, []*Tensor{t2})
	pipeline1 := createTestPipeline([]*ExecutionNode{nodeA1, nodeB1}, false)

	// Pipeline 2
	nodeA2 := createTestExecutionNode(3, "A2", []*Tensor{t1}, []*Tensor{t2})
	pipeline2 := createTestPipeline([]*ExecutionNode{nodeA2}, false)

	graph := createTestGraph([]*Pipeline{pipeline1, pipeline2})

	// Update reference numbers
	graph.UpdateTensorRefNum()

	// t1 is input to B1 and A2 = 2 references
	// t2 has no consumers = 0 reference
	r.Equal(int32(2), t1.RefNum)
	r.Equal(int32(0), t2.RefNum)
}

func TestGraphTopologicalSort(t *testing.T) {
	t.Run("Linear", func(t *testing.T) {
		r := require.New(t)

		// Create A -> B -> C
		t1 := createTestTensor(1, "t1", 0)
		t2 := createTestTensor(2, "t2", 0)
		t3 := createTestTensor(3, "t3", 0)

		nodeA := createTestExecutionNode(1, "A", nil, []*Tensor{t1})
		nodeB := createTestExecutionNode(2, "B", []*Tensor{t1}, []*Tensor{t2})
		nodeC := createTestExecutionNode(3, "C", []*Tensor{t2}, []*Tensor{t3})

		createTestEdge(nodeA, nodeB, t1)
		createTestEdge(nodeB, nodeC, t2)

		pipeline := createTestPipeline([]*ExecutionNode{nodeA, nodeB, nodeC}, false)
		graph := createTestGraph([]*Pipeline{pipeline})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 1)
		r.Equal([]*ExecutionNode{nodeA, nodeB, nodeC}, result[0])
	})

	t.Run("Diamond", func(t *testing.T) {
		r := require.New(t)

		// Create diamond shape: A -> [B, C] -> D
		t1 := createTestTensor(1, "t1", 0)
		t2 := createTestTensor(2, "t2", 0)
		t3 := createTestTensor(3, "t3", 0)
		t4 := createTestTensor(4, "t4", 0)

		nodeA := createTestExecutionNode(1, "A", nil, []*Tensor{t1})
		nodeB := createTestExecutionNode(2, "B", []*Tensor{t1}, []*Tensor{t2})
		nodeC := createTestExecutionNode(3, "C", []*Tensor{t1}, []*Tensor{t3})
		nodeD := createTestExecutionNode(4, "D", []*Tensor{t2, t3}, []*Tensor{t4})

		createTestEdge(nodeA, nodeB, t1)
		createTestEdge(nodeA, nodeC, t1)
		createTestEdge(nodeB, nodeD, t2)
		createTestEdge(nodeC, nodeD, t3)

		pipeline := createTestPipeline([]*ExecutionNode{nodeA, nodeB, nodeC, nodeD}, false)
		graph := createTestGraph([]*Pipeline{pipeline})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 1)

		// Verify A is first, D is last, B and C in between (sorted by ID)
		r.Equal(nodeA, result[0][0])
		r.Equal(nodeD, result[0][3])
		r.Equal(nodeB, result[0][1])
		r.Equal(nodeC, result[0][2])
	})

	t.Run("Cycle", func(t *testing.T) {
		r := require.New(t)

		// Create A -> B -> C -> A (cycle)
		t1 := createTestTensor(1, "t1", 0)
		t2 := createTestTensor(2, "t2", 0)
		t3 := createTestTensor(3, "t3", 0)

		nodeA := createTestExecutionNode(1, "A", []*Tensor{t3}, []*Tensor{t1})
		nodeB := createTestExecutionNode(2, "B", []*Tensor{t1}, []*Tensor{t2})
		nodeC := createTestExecutionNode(3, "C", []*Tensor{t2}, []*Tensor{t3})

		createTestEdge(nodeA, nodeB, t1)
		createTestEdge(nodeB, nodeC, t2)
		createTestEdge(nodeC, nodeA, t3)

		pipeline := createTestPipeline([]*ExecutionNode{nodeA, nodeB, nodeC}, false)
		graph := createTestGraph([]*Pipeline{pipeline})

		result, err := graph.TopologicalSort()
		r.Error(err)
		r.Contains(err.Error(), "topological sort fail: maybe circle in graph")
		r.Nil(result)
	})

	t.Run("SelfLoop", func(t *testing.T) {
		r := require.New(t)

		// Create node A with self-loop
		t1 := createTestTensor(1, "t1", 0)
		nodeA := createTestExecutionNode(1, "A", []*Tensor{t1}, []*Tensor{t1})
		createTestEdge(nodeA, nodeA, t1)

		pipeline := createTestPipeline([]*ExecutionNode{nodeA}, false)
		graph := createTestGraph([]*Pipeline{pipeline})

		result, err := graph.TopologicalSort()
		r.Error(err)
		r.Contains(err.Error(), "topological sort fail: maybe circle in graph")
		r.Nil(result)
	})

	t.Run("IndependentNodes", func(t *testing.T) {
		r := require.New(t)

		// Create nodes A, B, C with no dependencies
		t1 := createTestTensor(1, "t1", 0)
		t2 := createTestTensor(2, "t2", 0)
		t3 := createTestTensor(3, "t3", 0)

		nodeA := createTestExecutionNode(3, "A", nil, []*Tensor{t1}) // Higher ID
		nodeB := createTestExecutionNode(1, "B", nil, []*Tensor{t2}) // Lower ID
		nodeC := createTestExecutionNode(2, "C", nil, []*Tensor{t3}) // Middle ID

		pipeline := createTestPipeline([]*ExecutionNode{nodeA, nodeB, nodeC}, false)
		graph := createTestGraph([]*Pipeline{pipeline})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 1)

		// Should be sorted by ID for determinism: B(1), C(2), A(3)
		r.Equal([]*ExecutionNode{nodeB, nodeC, nodeA}, result[0])
	})

	t.Run("EmptyGraph", func(t *testing.T) {
		r := require.New(t)

		graph := createTestGraph([]*Pipeline{})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 0)
	})

	t.Run("EmptyPipeline", func(t *testing.T) {
		r := require.New(t)

		emptyPipeline := createTestPipeline([]*ExecutionNode{}, false)
		graph := createTestGraph([]*Pipeline{emptyPipeline})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 1)
		r.Len(result[0], 0)
	})

	t.Run("MultiplePipelines", func(t *testing.T) {
		r := require.New(t)

		// Pipeline 1: A -> B
		t1 := createTestTensor(1, "t1", 0)
		t2 := createTestTensor(2, "t2", 0)
		nodeA := createTestExecutionNode(1, "A", nil, []*Tensor{t1})
		nodeB := createTestExecutionNode(2, "B", []*Tensor{t1}, []*Tensor{t2})
		createTestEdge(nodeA, nodeB, t1)
		pipeline1 := createTestPipeline([]*ExecutionNode{nodeA, nodeB}, false)

		// Pipeline 2: C -> D
		t3 := createTestTensor(3, "t3", 0)
		t4 := createTestTensor(4, "t4", 0)
		nodeC := createTestExecutionNode(1, "C", nil, []*Tensor{t3})
		nodeD := createTestExecutionNode(2, "D", []*Tensor{t3}, []*Tensor{t4})
		createTestEdge(nodeC, nodeD, t3)
		pipeline2 := createTestPipeline([]*ExecutionNode{nodeC, nodeD}, false)

		graph := createTestGraph([]*Pipeline{pipeline1, pipeline2})

		result, err := graph.TopologicalSort()
		r.NoError(err)
		r.Len(result, 2)
		r.Equal([]*ExecutionNode{nodeA, nodeB}, result[0])
		r.Equal([]*ExecutionNode{nodeC, nodeD}, result[1])
	})
}

func TestGraphPartyInfo(t *testing.T) {
	r := require.New(t)

	graph := &Graph{
		PartyInfo: NewPartyInfo([]*Participant{
			{PartyCode: "alice", Endpoints: []string{"alice.com"}, Token: "token"},
			{PartyCode: "bob", Endpoints: []string{"bob.com"}, Token: "token"},
		}),
	}

	// Test GetParties
	parties := graph.GetParties()
	r.Equal([]string{"alice", "bob"}, parties)

	// Test GetUrlByParty
	url, err := graph.GetUrlByParty("alice")
	r.NoError(err)
	r.Equal("alice.com", url)

	// Test error case
	_, err = graph.GetUrlByParty("nonexistent")
	r.Error(err)
	r.Contains(err.Error(), "no party named nonexistent")
}
