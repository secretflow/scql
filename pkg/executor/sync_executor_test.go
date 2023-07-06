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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/interpreter/optimizer"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

type mockWebClient struct {
	sessionId string
	// url -> OutColumns
	responses map[string][]*proto.Tensor
}

func (client mockWebClient) Post(ctx context.Context, url string, credential string, content_type string, body string) (string, error) {
	resp := &proto.RunExecutionPlanResponse{
		Status:     &scql.Status{Code: int32(scql.Code_OK)},
		SessionId:  client.sessionId,
		OutColumns: client.responses[url],
	}
	return message.SerializeTo(resp, message.EncodingTypeJson)
}

func TestSyncExecutor(t *testing.T) {
	a := require.New(t)
	partyInfo, err := translator.NewPartyInfo([]string{"alice", "bob"}, []string{"alice.url", "bob.url"}, []string{"alice_credential", "bob_credential"})
	a.NoError(err)
	plan := translator.NewGraphBuilder(partyInfo)

	t1 := plan.AddTensor("alice.t1")
	t1.Status = scql.TensorStatus_TENSORSTATUS_PRIVATE
	err = plan.AddRunSQLNode("RunSQLOp1", []*translator.Tensor{t1},
		"select f1 from alice.t1", []string{"alice.t1"}, "alice")
	a.NoError(err)

	t2 := plan.AddTensor("bob.t2")
	t2.Status = scql.TensorStatus_TENSORSTATUS_PRIVATE
	err = plan.AddRunSQLNode("RunSQLOp2", []*translator.Tensor{t2},
		"select * from bob.t2", []string{"bob.t2"}, "bob")
	a.NoError(err)

	graph := plan.Build()
	graph.OutputNames = []string{t1.UniqueName(), t2.UniqueName()}

	p := optimizer.NewGraphPartitioner(graph)
	p.NaivePartition()

	m := optimizer.NewGraphMapper(p.Graph, p.SubDAGs)
	m.Map()
	a.NoError(err)

	sessionId := "mock"
	startParams := &scql.SessionStartParams{
		SessionId: sessionId,
	}

	pbRequests := m.CodeGen(startParams)

	// alice publish t1, bob publish t2
	{
		mockValue := []string{"test"}
		stub := &EngineStub{
			protocol: "http",
		}
		stub.webClient = mockWebClient{
			sessionId: sessionId,
			responses: map[string][]*proto.Tensor{
				fmt.Sprintf("http://alice.url%v", runExecutionPlanPath): {
					{
						Name:       t1.UniqueName(),
						ElemType:   proto.PrimitiveDataType_STRING,
						StringData: mockValue,
						Shape: &proto.TensorShape{
							Dim: []*proto.TensorShape_Dimension{{
								Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue))},
							}},
						},
					},
				},
				fmt.Sprintf("http://bob.url%v", runExecutionPlanPath): {
					{
						Name:       t2.UniqueName(),
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

		executor, err := NewSyncExecutor(pbRequests, p.Graph.OutputNames, stub, sessionId, p.Graph.PartyInfo)
		a.NoError(err)
		resp, err := executor.RunExecutionPlan(context.Background())
		a.NoError(err)
		a.Equal(sessionId, resp.ScdbSessionId)
		a.Equal(len(p.Graph.OutputNames), len(resp.OutColumns))
		for i, name := range executor.OutputNames {
			out := resp.OutColumns[i]
			a.Equal(translator.TensorNameFromUniqueName(name), out.Name)
			a.Equal(mockValue, out.GetStringData())
		}
	}

	// alice & bob joint publish t1, t2
	{
		mockValue := []string{"test", "test"}
		mockValue1 := []string{"test", constant.StringElementPlaceHolder}
		mockValue2 := []string{constant.StringElementPlaceHolder, "test"}
		stub := &EngineStub{
			protocol: "http",
		}
		stub.webClient = mockWebClient{
			sessionId: sessionId,
			responses: map[string][]*proto.Tensor{
				fmt.Sprintf("http://alice.url%v", runExecutionPlanPath): {
					{
						Name:       t1.UniqueName(),
						ElemType:   proto.PrimitiveDataType_STRING,
						StringData: mockValue1,
						Shape: &proto.TensorShape{
							Dim: []*proto.TensorShape_Dimension{{
								Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue1))},
							}},
						},
					},
					{
						Name:       t2.UniqueName(),
						ElemType:   proto.PrimitiveDataType_STRING,
						StringData: mockValue1,
						Shape: &proto.TensorShape{
							Dim: []*proto.TensorShape_Dimension{{
								Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue1))},
							}},
						},
					},
				},
				fmt.Sprintf("http://bob.url%v", runExecutionPlanPath): {
					{
						Name:       t1.UniqueName(),
						ElemType:   proto.PrimitiveDataType_STRING,
						StringData: mockValue2,
						Shape: &proto.TensorShape{
							Dim: []*proto.TensorShape_Dimension{{
								Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue2))},
							}},
						},
					},
					{
						Name:       t2.UniqueName(),
						ElemType:   proto.PrimitiveDataType_STRING,
						StringData: mockValue2,
						Shape: &proto.TensorShape{
							Dim: []*proto.TensorShape_Dimension{{
								Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue2))},
							}},
						},
					},
				},
			},
		}

		executor, err := NewSyncExecutor(pbRequests, p.Graph.OutputNames, stub, sessionId, p.Graph.PartyInfo)
		a.NoError(err)
		resp, err := executor.RunExecutionPlan(context.Background())
		a.NoError(err)
		a.Equal(sessionId, resp.ScdbSessionId)
		a.Equal(len(p.Graph.OutputNames), len(resp.OutColumns))
		for i, name := range executor.OutputNames {
			out := resp.OutColumns[i]
			a.Equal(translator.TensorNameFromUniqueName(name), out.Name)
			a.Equal(mockValue, out.GetStringData())
		}
	}
}
