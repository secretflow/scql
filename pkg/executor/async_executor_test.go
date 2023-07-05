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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/optimizer"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

type TestWebClient struct {
}

func (client TestWebClient) Post(ctx context.Context, url string, credential string, content_type string, body string) (string, error) {
	resp := &proto.Status{Code: int32(proto.Code_OK)}
	return message.SerializeTo(resp, message.EncodingTypeProtobuf)
}

func TestExecutor(t *testing.T) {
	partyInfo, err := translator.NewPartyInfo([]string{"alice", "bob"}, []string{"alice.url", "bob.url"}, []string{"alice_credential", "bob_credential"})
	assert.Nil(t, err)

	plan := translator.NewGraphBuilder(partyInfo)

	t1 := plan.AddTensor("alice.t1")
	t1.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	plan.AddRunSQLNode("RunSQLOp1", []*translator.Tensor{t1},
		"select f1 from alice.t1", []string{"alice.t1"}, "alice")

	t2 := plan.AddTensor("bob.t2")
	t2.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	plan.AddRunSQLNode("RunSQLOp2", []*translator.Tensor{t2},
		"select * from bob.t2", []string{"bob.t2"}, "bob")
	t3, err := plan.AddBinaryNode("less", "Less", t1, t2, ccl.CreateAllPlainCCL([]string{"alice", "bob"}), []string{"alice", "bob"})
	assert.NotNil(t, t3)
	assert.Nil(t, err)
	graph := plan.Build()

	stub := &EngineStub{
		contentType: "application/json",
	}
	stub.webClient = TestWebClient{}
	// runsql
	c0 := &proto.ReportRequest{
		DagId:     0,
		PartyCode: "alice",
		Status:    &proto.Status{Code: 0},
	}
	c1 := &proto.ReportRequest{
		DagId:     0,
		PartyCode: "bob",
		Status:    &proto.Status{Code: 0},
	}

	c2 := &proto.ReportRequest{
		DagId:     1,
		PartyCode: "alice",
		Status:    &proto.Status{Code: 0},
	}
	c3 := &proto.ReportRequest{
		DagId:     1,
		PartyCode: "bob",
		Status:    &proto.Status{Code: 0},
	}

	// less
	c4 := &proto.ReportRequest{
		DagId:     2,
		PartyCode: "alice",
		Status:    &proto.Status{Code: 0},
	}

	p := optimizer.NewGraphPartitioner(graph)
	p.NaivePartition()

	ae, err := NewAsyncExecutor(p.SubDAGs, graph.OutputNames, stub)
	assert.Nil(t, err)

	ae.RunFirst(context.Background())
	// 2 runsql

	next := func(ae *AsyncExecutor, ctx context.Context, req *proto.ReportRequest) (bool, error) {
		nextSubDagId, isEnd, err := ae.CanRunNext(ctx, req)
		if err != nil || isEnd || nextSubDagId < 0 {
			return isEnd, err
		}
		return false, ae.RunNext(ctx, req)
	}
	// run sql
	isEnd, err := next(ae, context.Background(), c0)
	assert.Equal(t, false, isEnd)
	assert.Nil(t, err)
	isEnd, err = next(ae, context.Background(), c1)
	assert.Equal(t, false, isEnd)
	assert.Nil(t, err)
	assert.Equal(t, 2, ae.successCount)
	// make_share
	isEnd, err = next(ae, context.Background(), c2)
	assert.Equal(t, false, isEnd)
	assert.Nil(t, err)
	isEnd, err = next(ae, context.Background(), c3)
	assert.Equal(t, false, isEnd)
	assert.Nil(t, err)
	assert.Equal(t, 3, ae.successCount)
	// less
	isEnd, err = next(ae, context.Background(), c4)
	assert.Equal(t, false, isEnd)
	assert.Nil(t, err)
	assert.Equal(t, 3, ae.successCount)
}

func TestExecutor_MergeQueryResults(t *testing.T) {
	a := require.New(t)

	{
		eid := "123"
		header := &proto.Status{Code: 0}
		mockValue := []string{"test"}
		e := AsyncExecutor{
			OutputNames: []string{"c1.0"},
			intermediateResults: []*proto.ReportRequest{
				{
					Status:    header,
					SessionId: eid,
					OutColumns: []*proto.Tensor{
						{
							Name:       "c1.0",
							ElemType:   proto.PrimitiveDataType_STRING,
							StringData: mockValue,
							Shape: &proto.TensorShape{
								Dim: []*proto.TensorShape_Dimension{{
									Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue))},
								}},
							},
						}},
				},
			},
		}
		result, err := e.MergeQueryResults()
		a.Nil(err)
		a.Equal(eid, result.ScdbSessionId)
		a.Equal(1, len(result.OutColumns))
		a.Equal(mockValue, result.OutColumns[0].GetStringData())
	}

	// merge partial published mockValue1 & mockValue2 -> mockValue
	{
		eid := "123"
		status := &proto.Status{Code: 0}
		mockValue1 := []string{constant.StringElementPlaceHolder, "test"}
		mockValue2 := []string{"test", constant.StringElementPlaceHolder}
		mockValue := []string{"test", "test"}
		e := AsyncExecutor{
			OutputNames: []string{"c1.0"},
			intermediateResults: []*proto.ReportRequest{
				{
					Status:    status,
					SessionId: eid,
					OutColumns: []*proto.Tensor{
						{
							Name:       "c1.0",
							ElemType:   proto.PrimitiveDataType_STRING,
							StringData: mockValue1,
							Shape: &proto.TensorShape{
								Dim: []*proto.TensorShape_Dimension{{
									Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue1))},
								}},
							},
						}},
				},
				{
					Status:    status,
					SessionId: eid,
					OutColumns: []*proto.Tensor{
						{
							Name:       "c1.0",
							ElemType:   proto.PrimitiveDataType_STRING,
							StringData: mockValue2,
							Shape: &proto.TensorShape{
								Dim: []*proto.TensorShape_Dimension{{
									Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(mockValue2))},
								}},
							},
						}},
				},
			},
		}
		result, err := e.MergeQueryResults()
		a.Nil(err)
		a.Equal(eid, result.ScdbSessionId)
		a.Equal(1, len(result.OutColumns))
		a.Equal(mockValue, result.OutColumns[0].GetStringData())
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

func TestSortTensorByOutputName(t *testing.T) {
	a := require.New(t)
	tensor := []*proto.Tensor{
		{Name: "0"},
		{Name: "1"},
		{Name: "2"},
		{Name: "3"},
	}

	{
		outputName := []string{"1", "2", "3", "0"}
		outputTensor, err := sortTensorByOutputName(tensor, outputName)
		a.Equal(4, len(outputTensor))
		a.Equal("1", outputTensor[0].Name)
		a.Equal("2", outputTensor[1].Name)
		a.Equal("3", outputTensor[2].Name)
		a.Equal("0", outputTensor[3].Name)
		a.Nil(err)
	}

	{
		outputName := []string{"1", "2", "3", "4"}
		outputTensor, err := sortTensorByOutputName(tensor, outputName)
		a.Nil(outputTensor)
		a.NotNil(err)
	}
}
