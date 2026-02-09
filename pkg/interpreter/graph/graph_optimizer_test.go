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

	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// createConstantNode creates a constant node with the given value and type
func createConstantNode(id int, value any, dtype *DataType) *ExecutionNode {
	attr := &Attribute{}
	switch v := value.(type) {
	case string:
		attr.SetString(v)
	case int64:
		attr.SetInt64(v)
	case float64:
		attr.SetDouble(v)
	}

	tensor := createTestTensor(id, "constant_out", 0)
	tensor.DType = dtype

	node := createTestExecutionNode(id, "constant", nil, []*Tensor{tensor})
	node.OpType = operator.OpNameConstant
	node.Attributes["scalar"] = attr
	node.Outputs = make(map[string][]*Tensor)
	node.Outputs["Out"] = []*Tensor{tensor}
	return node
}

// createBroadcastNode creates a BroadcastTo node
func createBroadcastNode(id int, input *Tensor) *ExecutionNode {
	output := createTestTensor(id+100, "broadcast_out", 0) // Use different ID for output
	output.DType = input.DType

	node := createTestExecutionNode(id, "broadcast", []*Tensor{input}, []*Tensor{output})
	node.OpType = operator.OpNameBroadcastTo
	node.Outputs = make(map[string][]*Tensor)
	node.Outputs["Out"] = []*Tensor{output}
	return node
}

// createCastNode creates a Cast node
func createCastNode(id int, input *Tensor, targetType *DataType) *ExecutionNode {
	output := createTestTensor(id+200, "cast_out", 0) // Use different ID for output
	output.DType = targetType

	node := createTestExecutionNode(id, "cast", []*Tensor{input}, []*Tensor{output})
	node.OpType = operator.OpNameCast
	node.Outputs = make(map[string][]*Tensor)
	node.Outputs["Out"] = []*Tensor{output}
	return node
}

func testConstantCastOptimization(t *testing.T, originValue interface{},
	originType, castType *DataType, expectedValue interface{}) {
	r := require.New(t)

	// Create the graph: Constant -> Broadcast -> Cast
	constNode := createConstantNode(1, originValue, originType)
	broadcastNode := createBroadcastNode(2, constNode.Outputs["Out"][0])
	castNode := createCastNode(3, broadcastNode.Outputs["Out"][0], castType)

	// Add edges
	createTestEdge(constNode, broadcastNode, constNode.Outputs["Out"][0])
	createTestEdge(broadcastNode, castNode, broadcastNode.Outputs["Out"][0])

	// Create pipeline and graph
	pipeline := createTestPipeline([]*ExecutionNode{constNode, broadcastNode, castNode}, false)
	graph := createTestGraph([]*Pipeline{pipeline})

	// Verify initial state
	pipelineNodes, err := graph.TopologicalSort()
	r.NoError(err)
	r.Len(pipelineNodes, 1)
	r.Len(pipelineNodes[0], 3) // const, broadcast, cast

	// Apply optimization
	optimizer := NewGraphOptimizer()
	err = optimizer.Optimize(graph)
	r.NoError(err)

	// Verify optimization result
	pipelineNodes, err = graph.TopologicalSort()
	r.NoError(err)
	r.Len(pipelineNodes, 1)
	r.Len(pipelineNodes[0], 2) // cast node removed

	// Find constant node and verify cast was applied
	var optimizedConst *ExecutionNode
	for _, node := range pipelineNodes[0] {
		if node.Name == "constant" {
			optimizedConst = node
			break
		}
	}
	r.NotNil(optimizedConst)

	// Verify the value was cast correctly
	scalarAttr := optimizedConst.Attributes["scalar"]
	r.NotNil(scalarAttr)
	r.Equal(expectedValue, scalarAttr.GetAttrValue())

	// Verify tensor type was updated
	outputTensor := optimizedConst.Outputs["Out"][0]
	if castType.IsTimeType() {
		r.Equal(proto.PrimitiveDataType_INT64, outputTensor.DType.DType)
	} else {
		r.True(castType.Equal(outputTensor.DType))
	}
}

func TestOptConstantCast(t *testing.T) {
	t.Run("StringToDatetime", func(t *testing.T) {
		testConstantCastOptimization(t, "2025-05-08", NewPrimitiveDataType(proto.PrimitiveDataType_STRING),
			NewPrimitiveDataType(proto.PrimitiveDataType_DATETIME), int64(1746662400))
	})

	t.Run("Int64ToFloat64", func(t *testing.T) {
		testConstantCastOptimization(t, int64(12), NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
			NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64), float64(12))
	})

	t.Run("Float64ToInt64", func(t *testing.T) {
		testConstantCastOptimization(t, float64(12.2), NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64),
			NewPrimitiveDataType(proto.PrimitiveDataType_INT64), int64(12))
	})
}
