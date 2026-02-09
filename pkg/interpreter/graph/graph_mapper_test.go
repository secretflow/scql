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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/operator"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// Helper to create test party info
func createTestPartyInfoForMapper(parties ...string) *PartyInfo {
	participants := make([]*Participant, 0, len(parties))
	for _, party := range parties {
		participants = append(participants, &Participant{
			PartyCode: party,
			Endpoints: []string{party + ".net"},
			Token:     party + "_credential",
		})
	}
	return NewPartyInfo(participants)
}

// Helper to create test graph with nodes
func createTestGraphForMapper(parties []string) *Graph {
	// Create tensors
	tensors := make([]*Tensor, 0, len(parties)*3)
	for i, party := range parties {
		for j := 0; j < 3; j++ {
			tensor := &Tensor{
				ID:             i*3 + j,
				Name:           fmt.Sprintf("t%d%d", i, j),
				DType:          NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
				RefNum:         0,
				OwnerPartyCode: party,
			}
			tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
			tensors = append(tensors, tensor)
		}
	}

	// Create execution nodes
	nodes := make([]*ExecutionNode, 0)

	// RunSQL nodes for each party
	for i, party := range parties {
		node := &ExecutionNode{
			ID:         i,
			Name:       "RunSQLOp" + party,
			OpType:     operator.OpNameRunSQL,
			Inputs:     make(map[string][]*Tensor),
			Outputs:    make(map[string][]*Tensor),
			Attributes: make(map[string]*Attribute),
			Edges:      make(map[*Edge]bool),
			Parties:    []string{party},
		}
		node.Outputs["output"] = tensors[i*3 : i*3+3]
		nodes = append(nodes, node)
	}

	// MakeShare nodes
	makeShareNodes := make([]*ExecutionNode, 0)
	for i := range tensors {
		node := &ExecutionNode{
			ID:         len(parties) + i,
			Name:       fmt.Sprintf("make_share_%d_%d", i/3, i%3),
			OpType:     operator.OpNameMakeShare,
			Inputs:     make(map[string][]*Tensor),
			Outputs:    make(map[string][]*Tensor),
			Attributes: make(map[string]*Attribute),
			Edges:      make(map[*Edge]bool),
			Parties:    parties,
		}
		node.Inputs["input"] = []*Tensor{tensors[i]}
		makeShareNodes = append(makeShareNodes, node)
	}
	nodes = append(nodes, makeShareNodes...)

	// Concat nodes
	concatNodes := make([]*ExecutionNode, 0)
	for i := 0; i < len(parties); i++ {
		node := &ExecutionNode{
			ID:         len(parties) + len(tensors) + i,
			Name:       "concat_" + parties[i],
			OpType:     operator.OpNameConcat,
			Inputs:     make(map[string][]*Tensor),
			Outputs:    make(map[string][]*Tensor),
			Attributes: make(map[string]*Attribute),
			Edges:      make(map[*Edge]bool),
			Parties:    parties,
		}
		// Each concat node takes 3 make_share outputs
		startIdx := i * 3
		inputs := make([]*Tensor, 3)
		for j := 0; j < 3; j++ {
			// Create output tensors for make_share nodes
			outputTensor := &Tensor{
				ID:     len(tensors) + startIdx + j,
				Name:   fmt.Sprintf("shared_%d%d", i, j),
				DType:  NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
				RefNum: 0,
			}
			outputTensor.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
			inputs[j] = outputTensor
		}
		node.Inputs["In"] = inputs
		concatNodes = append(concatNodes, node)
	}
	nodes = append(nodes, concatNodes...)

	// Create graph
	partyInfo := createTestPartyInfoForMapper(parties...)
	pipeline := &Pipeline{
		Batched:       false,
		InputTensors:  []*Tensor{},
		OutputTensors: []*Tensor{},
		Nodes:         make(map[*ExecutionNode]bool),
	}
	for _, node := range nodes {
		pipeline.Nodes[node] = true
	}

	g := &Graph{
		Pipelines:   []*Pipeline{pipeline},
		NodeCnt:     len(nodes),
		OutputNames: []string{},
		PartyInfo:   partyInfo,
	}

	return g
}

func TestMapSimple(t *testing.T) {
	a := require.New(t)

	// Create test graph
	parties := []string{"alice", "bob", "carol"}
	graph := createTestGraphForMapper(parties)

	// Create partitioner and partition
	p := NewGraphPartitioner(graph)
	p.NaivePartition()

	// Create mapper and map
	m := NewGraphMapper(p.Graph, p.Pipelines)
	m.Map()

	// Verify all parties have execution plans
	a.Equal(len(parties), len(m.Codes))
	for _, party := range parties {
		a.Contains(m.Codes, party)
		a.NotNil(m.Codes[party])
		a.NotNil(m.Codes[party].Policy)
	}

	// NaivePartition should result in 1 worker
	for _, party := range parties {
		a.Equal(1, m.Codes[party].Policy.WorkerNumber)
	}

	// Verify make_share nodes exist in all parties
	// Find the first make_share node
	var makeShareNodeID int
	var found bool
	for id, node := range m.Codes["alice"].Nodes {
		if node.OpType == operator.OpNameMakeShare {
			makeShareNodeID = id
			found = true
			break
		}
	}
	a.True(found, "MakeShare node not found in alice's plan")

	// Verify the same make_share node exists in all parties
	for _, party := range parties {
		node, exists := m.Codes[party].Nodes[makeShareNodeID]
		a.True(exists, "MakeShare node not found in %s's plan", party)
		a.Equal(operator.OpNameMakeShare, node.OpType)
	}
}

func TestMapSimpleStreaming(t *testing.T) {
	a := require.New(t)

	// Create test graph
	parties := []string{"alice", "bob", "carol"}
	graph := createTestGraphForMapper(parties)

	// Mark the pipeline as batched
	for _, pipeline := range graph.Pipelines {
		pipeline.Batched = true
	}

	// Create partitioner and partition
	p := NewGraphPartitioner(graph)
	p.NaivePartition()

	// Create mapper and map
	m := NewGraphMapper(p.Graph, p.Pipelines)
	m.Map()

	// Verify all parties have execution plans
	a.Equal(len(parties), len(m.Codes))
	for _, party := range parties {
		a.Contains(m.Codes, party)
		a.NotNil(m.Codes[party])
		a.NotNil(m.Codes[party].Policy)
	}

	// NaivePartition should result in 1 worker
	for _, party := range parties {
		a.Equal(1, m.Codes[party].Policy.WorkerNumber)
	}

	// Verify we have both batched and non-batched pipeline jobs
	a.GreaterOrEqual(len(m.Codes["alice"].Policy.PipelineJobs), 1)

	// The first pipeline should be batched
	a.True(m.Codes["alice"].Policy.PipelineJobs[0].Batched)
}

func TestInferenceWorkerNumber(t *testing.T) {
	a := require.New(t)

	// Test empty graph
	emptyGraph := &Graph{
		Pipelines: []*Pipeline{},
		PartyInfo: createTestPartyInfoForMapper("alice", "bob"),
	}
	m := NewGraphMapper(emptyGraph, []*PartitionedPipeline{})
	a.Equal(1, m.InferenceWorkerNumber())

	// Test graph with single node per subdag
	singleNodeGraph := &Graph{
		Pipelines: []*Pipeline{
			{
				Nodes: map[*ExecutionNode]bool{
					{ID: 0}: true,
				},
			},
		},
		PartyInfo: createTestPartyInfoForMapper("alice", "bob"),
	}
	pipelines := []*PartitionedPipeline{
		{
			SubDAGs: []*WorkerSubDAG{
				{
					Nodes: map[*ExecutionNode]bool{
						{ID: 0}: true,
					},
				},
			},
		},
	}
	m = NewGraphMapper(singleNodeGraph, pipelines)
	a.Equal(1, m.InferenceWorkerNumber())

	// Test graph with many nodes (testing the level thresholds)
	manyNodesMap := make(map[*ExecutionNode]bool)
	manyNodes := make([]*ExecutionNode, 0, 20)
	for i := range 20 {
		node := &ExecutionNode{ID: i}
		manyNodes = append(manyNodes, node)
		manyNodesMap[node] = true
	}
	manyNodeGraph := &Graph{
		Pipelines: []*Pipeline{
			{
				Nodes: manyNodesMap,
			},
		},
		PartyInfo: createTestPartyInfoForMapper("alice", "bob"),
	}
	pipelines = []*PartitionedPipeline{
		{
			SubDAGs: []*WorkerSubDAG{
				{Nodes: manyNodesMap},
			},
		},
	}
	m = NewGraphMapper(manyNodeGraph, pipelines)
	// With 20 nodes, should return workerNumLevel1 = 8 (since it's > level2)
	a.Equal(8, m.InferenceWorkerNumber())

	// Test graph with nodes between level1 and level2
	betweenLevelNodesMap := make(map[*ExecutionNode]bool)
	betweenLevelNodes := make([]*ExecutionNode, 0, 10)
	for i := range 10 {
		node := &ExecutionNode{ID: i + 20}
		betweenLevelNodes = append(betweenLevelNodes, node)
		betweenLevelNodesMap[node] = true
	}
	betweenLevelNodeGraph := &Graph{
		Pipelines: []*Pipeline{
			{
				Nodes: betweenLevelNodesMap,
			},
		},
		PartyInfo: createTestPartyInfoForMapper("alice", "bob"),
	}
	betweenLevelPipelines := []*PartitionedPipeline{
		{
			SubDAGs: []*WorkerSubDAG{
				{Nodes: betweenLevelNodesMap},
			},
		},
	}
	m = NewGraphMapper(betweenLevelNodeGraph, betweenLevelPipelines)
	// With 10 nodes, should return workerNum/2 = 5 (since it's between level1 and level2)
	a.Equal(5, m.InferenceWorkerNumber())
}

func TestNeedSyncSymbol(t *testing.T) {
	tests := []struct {
		name     string
		opType   string
		expected bool
	}{
		{"Replicate", operator.OpNameReplicate, true},
		{"MakeShare", operator.OpNameMakeShare, true},
		{"Other", "OtherOperation", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExecutionNode{
				OpType: tt.opType,
			}
			result := needSyncSymbol(node)
			assert.Equal(t, tt.expected, result)
		})
	}
}
