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

package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
)

func testCastConversion(t *testing.T, originValue interface{}, originType proto.PrimitiveDataType, castType proto.PrimitiveDataType, expectedValue interface{}) {
	r := require.New(t)

	participants := []*Participant{
		{PartyCode: "party1", Endpoints: []string{"party1.net"}, Token: "party1_credential"},
		{PartyCode: "party2", Endpoints: []string{"party2.net"}, Token: "party2_credential"},
	}
	partyInfo := NewPartyInfo(participants)
	e1 := NewGraphBuilder(partyInfo, false)

	t1 := e1.AddTensor("alice.date")
	t1.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t1.DType = castType
	e1.AddRunSQLNode("RunSQLOp1", []*Tensor{t1}, "select f1 from alice.t1", []string{"alice.t1"}, "party1")
	t1.CC = ccl.CreateAllPlainCCL([]string{"party1", "party2"})

	// create constant node according to originValue
	var constantTensor *Tensor
	switch originType {
	case proto.PrimitiveDataType_STRING:
		strDatum := types.NewStringDatum(originValue.(string))
		t2, err := e1.AddConstantNode("make_constant", &strDatum, []string{"party1"})
		r.NoError(err)
		r.NotNil(t2)
		constantTensor = t2
	case proto.PrimitiveDataType_INT64:
		intDatum := types.NewIntDatum(originValue.(int64))
		t2, err := e1.AddConstantNode("make_constant", &intDatum, []string{"party1"})
		r.NoError(err)
		r.NotNil(t2)
		constantTensor = t2
	case proto.PrimitiveDataType_FLOAT64:
		floatDatum := types.NewFloat64Datum(originValue.(float64))
		t2, err := e1.AddConstantNode("make_constant", &floatDatum, []string{"party1"})
		r.NoError(err)
		r.NotNil(t2)
		constantTensor = t2
	// TODO: add more test cases
	default:
		t.Fatalf("Unsupported origin type: %v", originType)
	}

	t3s, err := e1.AddBroadcastToNode("broadcast", []*Tensor{constantTensor}, t1)
	r.NoError(err)
	r.NotNil(t3s)
	t3 := t3s[0]
	r.NotNil(t3)

	t4, err := e1.AddCastNode("cast", t1.DType, t3, []string{"party1"})
	r.NoError(err)
	r.NotNil(t4)

	graph := e1.Build()
	pipelineNodes, err := graph.TopologicalSort()
	r.NoError(err)
	r.NotNil(pipelineNodes)

	r.Equal(4, len(pipelineNodes[0]))

	graphOptimizer := NewGraphOptimizer()
	err = graphOptimizer.Optimize(graph)
	r.NoError(err)
	pipelineNodes, err = graph.TopologicalSort()
	r.NoError(err)
	r.NotNil(pipelineNodes)

	// cast node should be removed
	r.Equal(3, len(pipelineNodes[0]))
	for _, node := range pipelineNodes[0] {
		if node.Name == "make_constant" {
			r.Equal(expectedValue, node.Attributes["scalar"].GetAttrValue())
		}
	}
}

func TestOptConstantCast(t *testing.T) {
	// Test for string to datetime conversion
	testCastConversion(t, "2025-05-08", proto.PrimitiveDataType_STRING, proto.PrimitiveDataType_DATETIME, int64(1746662400))

	// Test for int to float conversion
	testCastConversion(t, int64(12), proto.PrimitiveDataType_INT64, proto.PrimitiveDataType_FLOAT64, float64(12))

	// Test for float to int conversion
	testCastConversion(t, float64(12.2), proto.PrimitiveDataType_FLOAT64, proto.PrimitiveDataType_INT64, int64(12))
}
