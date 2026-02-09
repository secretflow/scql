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

// Helper to create test tensor with owner
func createTestTensorWithOwner(id int, name, owner string) *Tensor {
	tensor := &Tensor{
		ID:     id,
		Name:   name,
		DType:  NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
		RefNum: 0,
	}
	tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	tensor.OwnerPartyCode = owner
	return tensor
}

// Helper to create test execution node
func createTestExecutionNodeForSplitter(id int, name string, parties []string, inputs, outputs []*Tensor) *ExecutionNode {
	node := &ExecutionNode{
		ID:         id,
		Name:       name,
		OpType:     "TestOp",
		Inputs:     make(map[string][]*Tensor),
		Outputs:    make(map[string][]*Tensor),
		Attributes: make(map[string]*Attribute),
		Edges:      make(map[*Edge]bool),
		Parties:    parties,
	}

	if len(inputs) > 0 {
		node.Inputs["input"] = inputs
	}
	if len(outputs) > 0 {
		node.Outputs["output"] = outputs
	}

	return node
}

// Helper to create test SubDAG
func createTestSubDAG(nodes []*ExecutionNode) *WorkerSubDAG {
	nodeMap := make(map[*ExecutionNode]bool)
	for _, node := range nodes {
		nodeMap[node] = true
	}
	return &WorkerSubDAG{
		Nodes: nodeMap,
	}
}

func TestSplitSimple(t *testing.T) {
	r := require.New(t)

	// Create test tensors
	t0 := createTestTensorWithOwner(0, "t0", "alice")
	t1 := createTestTensorWithOwner(1, "t1", "bob")
	t2 := createTestTensorWithOwner(2, "t2", "carol")

	// Create execution nodes
	runSQLNode1 := createTestExecutionNodeForSplitter(0, "RunSQLOp1", []string{"alice"}, nil, []*Tensor{t0})
	runSQLNode2 := createTestExecutionNodeForSplitter(1, "RunSQLOp2", []string{"bob"}, nil, []*Tensor{t1})
	runSQLNode3 := createTestExecutionNodeForSplitter(2, "RunSQLOp3", []string{"carol"}, nil, []*Tensor{t2})

	joinNode1 := createTestExecutionNodeForSplitter(3, "join", []string{"alice", "bob"},
		[]*Tensor{t0, t1}, []*Tensor{t0})
	joinNode2 := createTestExecutionNodeForSplitter(4, "join", []string{"alice", "carol"},
		[]*Tensor{t0, t2}, []*Tensor{t0})

	filterNode1 := createTestExecutionNodeForSplitter(5, "filter", []string{"alice"},
		[]*Tensor{t0}, []*Tensor{t0})
	filterNode2 := createTestExecutionNodeForSplitter(6, "filter", []string{"bob"},
		[]*Tensor{t1}, []*Tensor{t1})
	filterNode3 := createTestExecutionNodeForSplitter(7, "filter", []string{"carol"},
		[]*Tensor{t2}, []*Tensor{t2})

	copyNode := createTestExecutionNodeForSplitter(8, "copy", []string{"alice", "bob"},
		[]*Tensor{t0}, []*Tensor{t0})

	// Create test graph with nodes
	nodes := []*ExecutionNode{
		runSQLNode1, runSQLNode2, runSQLNode3,
		joinNode1, joinNode2,
		filterNode1, filterNode2, filterNode3,
		copyNode,
	}

	// Create SubDAG
	subDag := createTestSubDAG(nodes)

	// Split the SubDAG
	partySubDAGs, err := Split(subDag)
	r.NoError(err)

	// Verify results
	// alice: should have 5 nodes (2 single-party nodes + 2 multi-party nodes + 1 copy)
	r.Equal(5, len(partySubDAGs["alice"].Nodes))

	// bob: should have 4 nodes (1 single-party node + 2 multi-party nodes + 1 copy)
	r.Equal(4, len(partySubDAGs["bob"].Nodes))

	// carol: should have 3 nodes (1 single-party node + 2 multi-party nodes)
	r.Equal(3, len(partySubDAGs["carol"].Nodes))

	// Verify node names in each party's subDAG
	aliceNames := make([]string, 0)
	for _, node := range partySubDAGs["alice"].Nodes {
		aliceNames = append(aliceNames, node.Name)
	}
	r.Contains(aliceNames, "RunSQLOp1")
	r.Contains(aliceNames, "join")
	r.Contains(aliceNames, "filter")
	r.Contains(aliceNames, "copy")

	bobNames := make([]string, 0)
	for _, node := range partySubDAGs["bob"].Nodes {
		bobNames = append(bobNames, node.Name)
	}
	r.Contains(bobNames, "RunSQLOp2")
	r.Contains(bobNames, "join")
	r.Contains(bobNames, "filter")
	r.Contains(bobNames, "copy")

	carolNames := make([]string, 0)
	for _, node := range partySubDAGs["carol"].Nodes {
		carolNames = append(carolNames, node.Name)
	}
	r.Contains(carolNames, "RunSQLOp3")
	r.Contains(carolNames, "join")
	r.Contains(carolNames, "filter")
}

func TestSplitEmptySubDAG(t *testing.T) {
	r := require.New(t)

	// Create empty SubDAG
	subDag := createTestSubDAG([]*ExecutionNode{})

	// Split the SubDAG
	partySubDAGs, err := Split(subDag)
	r.NoError(err)
	r.Equal(0, len(partySubDAGs))
}

func TestSplitSinglePartyNodes(t *testing.T) {
	r := require.New(t)

	// Create test tensors
	t0 := createTestTensorWithOwner(0, "t0", "alice")
	t1 := createTestTensorWithOwner(1, "t1", "bob")

	// Create single-party nodes only
	aliceNode := createTestExecutionNodeForSplitter(0, "AliceOp", []string{"alice"}, nil, []*Tensor{t0})
	bobNode := createTestExecutionNodeForSplitter(1, "BobOp", []string{"bob"}, nil, []*Tensor{t1})

	nodes := []*ExecutionNode{aliceNode, bobNode}
	subDag := createTestSubDAG(nodes)

	// Split the SubDAG
	partySubDAGs, err := Split(subDag)
	r.NoError(err)

	// Verify each party has only their own node
	r.Equal(1, len(partySubDAGs["alice"].Nodes))
	r.Equal("AliceOp", partySubDAGs["alice"].Nodes[0].Name)

	r.Equal(1, len(partySubDAGs["bob"].Nodes))
	r.Equal("BobOp", partySubDAGs["bob"].Nodes[0].Name)
}
