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

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
)

// createTestBuilder creates a properly initialized ExecutionGraphBuilder for testing
func createTestBuilder() *ExecutionGraphBuilder {
	tmm := NewTensorMetaManager()
	vt := NewVisibilityTable([]string{"alice", "bob"})
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	info := &graph.EnginesInfo{}

	return NewExecutionGraphBuilder(tmm, srm, vt, info, &v1.CompileOptions{Batched: false})
}

func TestConvertStatus(t *testing.T) {
	builder := createTestBuilder()

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) *graph.Tensor {
		meta := builder.tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		builder.vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := builder.tm.CreateAndSetFirstTensor(meta, place)
		builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + len(builder.tm.tensors) + 1
		return tensor
	}

	// test case 1: private -> same private (should fail)
	input := createPlacedTensor("test_tensor1", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
	output, err := builder.convertStatus(input, &privatePlacement{partyCode: "alice"}, true)
	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "has already met expect place")

	// test case 2: private -> different private (copy)
	input = createPlacedTensor("test_tensor2", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &privatePlacement{partyCode: "bob"}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "bob", output.OwnerPartyCode)

	// test case 3: private -> public
	input = createPlacedTensor("test_tensor3", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &publicPlacement{}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, output.Status())

	// test case 4: private -> secret
	input = createPlacedTensor("test_tensor4", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &secretPlacement{}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, output.Status())

	// test case 5: secret -> private
	input = createPlacedTensor("test_tensor5", &secretPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &privatePlacement{partyCode: "alice"}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "alice", output.OwnerPartyCode)

	// test case 6: secret -> public
	input = createPlacedTensor("test_tensor6", &secretPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &publicPlacement{}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, output.Status())

	// test case 7: secret -> same secret (should fail)
	input = createPlacedTensor("test_tensor7", &secretPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &secretPlacement{}, true)
	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "has already met expect place")

	// test case 8: public -> private
	input = createPlacedTensor("test_tensor8", &publicPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &privatePlacement{partyCode: "alice"}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "alice", output.OwnerPartyCode)

	// test case 9: public -> secret (should fail)
	input = createPlacedTensor("test_tensor9", &publicPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &secretPlacement{}, true)
	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "must be private")

	// test case 10: public -> same public (should fail)
	input = createPlacedTensor("test_tensor10", &publicPlacement{}, []string{"alice", "bob"})
	output, err = builder.convertStatus(input, &publicPlacement{}, true)
	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "has already met expect place")

	// test case 11: secret string -> private
	input = createPlacedTensor("test_tensor11", &secretPlacement{}, []string{"alice", "bob"})
	input.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	output, err = builder.convertStatus(input, &privatePlacement{partyCode: "alice"}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "alice", output.OwnerPartyCode)

	// test case 12: public string -> private
	input = createPlacedTensor("test_tensor12", &publicPlacement{}, []string{"alice", "bob"})
	input.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	output, err = builder.convertStatus(input, &privatePlacement{partyCode: "alice"}, true)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "alice", output.OwnerPartyCode)
}

func TestAddMakeShareNode(t *testing.T) {
	builder := createTestBuilder()
	meta := builder.tensorMetaManager.CreateTensorMeta("test_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	input, err := builder.tm.findTensorForConversion(meta, &secretPlacement{})
	assert.NoError(t, err)

	output, err := builder.addMakeShareNode(input)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, output.Status())
}

func TestAddMakePrivateNode(t *testing.T) {
	builder := createTestBuilder()
	meta := builder.tensorMetaManager.CreateTensorMeta("test_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	builder.tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	input, err := builder.tm.findTensorForConversion(meta, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	output, err := builder.addMakePrivateNode(input, "alice")
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "alice", output.OwnerPartyCode)
}

func TestAddMakePublicNode(t *testing.T) {
	builder := createTestBuilder()
	meta := builder.tensorMetaManager.CreateTensorMeta("test_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	input, err := builder.tm.findTensorForConversion(meta, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	output, err := builder.addMakePublicNode(input)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, output.Status())
}

func TestAddCopyNode(t *testing.T) {
	builder := createTestBuilder()
	meta := builder.tensorMetaManager.CreateTensorMeta("test_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	builder.tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	input, err := builder.tm.findTensorForConversion(meta, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	output, err := builder.addCopyNode(input, "alice", "bob")
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, output.Status())
	assert.Equal(t, "bob", output.OwnerPartyCode)

}

func TestGetOrCreatePlacedTensor(t *testing.T) {
	builder := createTestBuilder()
	meta := builder.tensorMetaManager.CreateTensorMeta("test_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	builder.vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice", "bob"}))
	builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

	// Test case 1: input tensor has no corresponding placed tensor
	output, err := builder.getOrCreatePlacedTensor(meta, &publicPlacement{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the input tensor does not have any corresponding placed tensor")
	assert.Nil(t, output)

	// Test case 2: input tensor has wanted placed tensor
	builder.tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	output, err = builder.getOrCreatePlacedTensor(meta, &publicPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, output)

	// Test case 3: input tensor has corresponding placed tensor with different placement
	output, err = builder.getOrCreatePlacedTensor(meta, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)
	assert.NotNil(t, output)
}
