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

package executor

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type mockGrpcEngineStub struct {
	sessionId string
	// url -> OutColumns
	responses map[string][]*proto.Tensor
}

func (client *mockGrpcEngineStub) RunExecutionPlan(url, credential string, executionPlanReq *proto.RunExecutionPlanRequest) (*proto.RunExecutionPlanResponse, error) {
	return &proto.RunExecutionPlanResponse{
		Status:     &proto.Status{Code: int32(proto.Code_OK)},
		JobId:      client.sessionId,
		OutColumns: client.responses[url],
	}, nil
}

func (client *mockGrpcEngineStub) Post(ctx context.Context, url, credential, content_type, body string) (string, error) {
	return "", errors.New("cannot use gRPC engine client to run HTTP method")
}

// Helper to create mock party info for tests
func createMockPartyInfo() *graph.PartyInfo {
	return graph.NewPartyInfo([]*graph.Participant{
		{
			PartyCode: "alice",
			Endpoints: []string{"alice.url"},
			Token:     "alice_credential",
		},
		{
			PartyCode: "bob",
			Endpoints: []string{"bob.url"},
			Token:     "bob_credential",
		},
	})
}

// Helper to create a simple test execution graph
func createTestExecutionGraph(partyInfo *graph.PartyInfo, outputName string) *graph.Graph {
	t1 := graph.NewTensor(0, "alice.t1")
	t1.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t1.OwnerPartyCode = "alice"
	t1.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)

	t2 := graph.NewTensor(1, "bob.t2")
	t2.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t2.OwnerPartyCode = "bob"
	t2.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)

	// Create execution nodes
	runSQLNode1 := &graph.ExecutionNode{
		ID:         0,
		Name:       "RunSQLOp1",
		OpType:     operator.OpNameRunSQL,
		Inputs:     make(map[string][]*graph.Tensor),
		Outputs:    map[string][]*graph.Tensor{"output": {t1}},
		Attributes: make(map[string]*graph.Attribute),
		Edges:      make(map[*graph.Edge]bool),
		Parties:    []string{"alice"},
	}

	runSQLNode2 := &graph.ExecutionNode{
		ID:         1,
		Name:       "RunSQLOp2",
		OpType:     operator.OpNameRunSQL,
		Inputs:     make(map[string][]*graph.Tensor),
		Outputs:    map[string][]*graph.Tensor{"output": {t2}},
		Attributes: make(map[string]*graph.Attribute),
		Edges:      make(map[*graph.Edge]bool),
		Parties:    []string{"bob"},
	}

	// Create pipeline with nodes
	pipeline := &graph.Pipeline{
		Batched:       false,
		InputTensors:  []*graph.Tensor{},
		OutputTensors: []*graph.Tensor{},
		Nodes:         map[*graph.ExecutionNode]bool{runSQLNode1: true, runSQLNode2: true},
	}

	// Create graph
	g := &graph.Graph{
		Pipelines:   []*graph.Pipeline{pipeline},
		NodeCnt:     2,
		OutputNames: []string{outputName},
		PartyInfo:   partyInfo,
	}

	return g
}

// Helper to create mock engine stub for testing
func createMockEngineStub(sessionID string, outputName string, mockValue []string) *EngineStub {
	stub := &EngineStub{}
	stub.webClient = &mockGrpcEngineStub{
		sessionId: sessionID,
		responses: map[string][]*proto.Tensor{
			"alice.url": {
				{
					Name:       outputName,
					ElemType:   proto.PrimitiveDataType_STRING,
					StringData: mockValue,
					Shape: &proto.TensorShape{
						Dim: []*proto.TensorShape_Dimension{{
							Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue))},
						}},
					},
				},
			},
		},
	}
	return stub
}

func TestSyncExecutor(t *testing.T) {
	a := require.New(t)

	// Use helper functions to create test components
	partyInfo := createMockPartyInfo()
	outputName := "alice.t1"
	executionGraph := createTestExecutionGraph(partyInfo, outputName)

	p := graph.NewGraphPartitioner(executionGraph)
	p.NaivePartition()

	m := graph.NewGraphMapper(p.Graph, p.Pipelines)
	m.Map()

	sessionId := "mock"
	startParams := &proto.JobStartParams{
		JobId: sessionId,
	}

	pbRequests := m.CodeGen(startParams)

	// only alice publish t1
	{
		mockValue := []string{"test"}
		stub := createMockEngineStub(sessionId, outputName, mockValue)

		executor, err := NewExecutor(pbRequests, p.Graph.OutputNames, stub, sessionId, p.Graph.PartyInfo)
		a.NoError(err)
		resp, err := executor.RunExecutionPlan(context.Background(), false)
		a.NoError(err)
		a.Equal(len(p.Graph.OutputNames), len(resp.Result.OutColumns))
		for i, name := range executor.OutputNames {
			out := resp.Result.OutColumns[i]
			a.Equal(name, out.Name)
			a.Equal(mockValue, out.GetStringData())
		}
	}
}

func TestFind(t *testing.T) {
	a := require.New(t)

	ss := []string{"0", "1", "2", "3"}

	{
		idx, err := find(ss, "1")
		a.Equal(1, idx)
		a.Nil(err)
	}

	{
		idx, err := find(ss, "4")
		a.Equal(-1, idx)
		a.NotNil(err)
	}
}
