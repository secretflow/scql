// Copyright 2025 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
)

func TestNewTensorManager(t *testing.T) {
	tm := NewTensorManager(1)
	assert.NotNil(t, tm)
	assert.NotNil(t, tm.metaId2Tpm)
	assert.NotNil(t, tm.tensorId2Meta)
	assert.Empty(t, tm.metaId2Tpm)
	assert.Empty(t, tm.tensorId2Meta)
}

func TestCorrespondingMeta(t *testing.T) {
	tm := NewTensorManager(1)

	// Create test tensor
	pt := &graph.Tensor{ID: 100, Name: "test_tensor"}

	// Test error when tensor not found
	_, err := tm.CorrespondingMeta(pt)
	assert.Error(t, err)

	// Setup mapping
	tm.tensorId2Meta[100] = &TensorMeta{
		ID:   100,
		Name: "test_tensor",
	}

	// Test successful lookup
	meta, err := tm.CorrespondingMeta(pt)
	assert.NoError(t, err)
	assert.NotNil(t, meta)
}

func TestCreateAndSetFirstPlacedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor", DType: graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)}

	// Test successful creation
	tensor, err := tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, tensor)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, tensor.Status())
	assert.Equal(t, 1, tensor.ID)

	// Test duplicate creation
	_, err = tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
	assert.Error(t, err)

	// Test private placement
	ut2 := &TensorMeta{ID: 2, Name: "test_tensor2", DType: graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)}
	tensor2, err := tm.CreateAndSetFirstTensor(ut2, &privatePlacement{partyCode: "bob"})
	assert.NoError(t, err)
	assert.Equal(t, "bob", tensor2.OwnerPartyCode)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, tensor2.Status())
}

func TestSetStatusedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	// Setup initial state
	meta := &TensorMeta{ID: 1, Name: "test_tensor"}
	_, err := tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Create placed tensor
	pt := &graph.Tensor{ID: 100, Name: "statused_tensor"}
	pt.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)

	// Test successful set
	err = tm.setPlacedTensor(meta, pt, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	// Test duplicate tensor ID
	pt2 := &graph.Tensor{ID: 100, Name: "duplicate_tensor"}
	pt2.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC)
	err = tm.setPlacedTensor(meta, pt2, &publicPlacement{})
	assert.Error(t, err)

	// Must use CreateAndSetFirstTensor to create the first placed tensor
	ut2 := &TensorMeta{ID: 999, Name: "nonexistent"}
	err = tm.setPlacedTensor(ut2, pt, &publicPlacement{})
	assert.Error(t, err)

	// Test state consistency validation
	pt3 := &graph.Tensor{ID: 200, Name: "mismatch_tensor", OwnerPartyCode: "charlie"}
	pt3.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC) // Public tensor with private placement
	err = tm.setPlacedTensor(meta, pt3, &privatePlacement{partyCode: "charlie"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tensor status")
}

func TestGetPlacedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}
	_, err := tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Test successful get
	tensor, err := tm.getPlacedTensor(meta, &publicPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, tensor)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, tensor.Status())

	// Test non-existent placement
	_, err = tm.getPlacedTensor(meta, &privatePlacement{partyCode: "alice"})
	assert.Error(t, err)

	// Test non-existent UT
	ut2 := &TensorMeta{ID: 999, Name: "nonexistent"}
	_, err = tm.getPlacedTensor(ut2, &publicPlacement{})
	assert.Error(t, err)
}

func TestGetOnePlacedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	// Test no placed tensors
	_, err := tm.getOnePlacedTensor(meta)
	assert.Error(t, err)

	// Add public tensor
	_, err = tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Should return public tensor
	tensor, err := tm.getOnePlacedTensor(meta)
	assert.NoError(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, tensor.Status())

	// Add private tensor
	ut2 := &TensorMeta{ID: 2, Name: "test_tensor2"}
	_, err = tm.CreateAndSetFirstTensor(ut2, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	// Should return private tensor when no public
	tensor2, err := tm.getOnePlacedTensor(ut2)
	assert.NoError(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, tensor2.Status())
}

func TestExistingPrivateParties(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	// Test empty result
	parties := tm.existingPrivateParties(meta)
	assert.Empty(t, parties)

	// Add private tensor
	_, err := tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	// Test single party
	parties = tm.existingPrivateParties(meta)
	assert.Equal(t, []string{"alice"}, parties)

	// Add another private tensor (simulated)
	tensor2 := &graph.Tensor{ID: 100, Name: "private_tensor", OwnerPartyCode: "bob"}
	tensor2.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	tm.setPlacedTensor(meta, tensor2, &privatePlacement{partyCode: "bob"})

	// Test multiple parties
	parties = tm.existingPrivateParties(meta)
	assert.Contains(t, parties, "alice")
	assert.Contains(t, parties, "bob")
	assert.Len(t, parties, 2)
}

func TestChoosePrivatePlacement(t *testing.T) {
	tm := NewTensorManager(1)

	ut1 := &TensorMeta{ID: 1, Name: "tensor1"}
	ut2 := &TensorMeta{ID: 2, Name: "tensor2"}

	// Setup: create tensors for alice and bob
	_, err := tm.CreateAndSetFirstTensor(ut1, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)
	_, err = tm.CreateAndSetFirstTensor(ut2, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	candidates := NewVisibleParties([]string{"alice", "bob", "charlie"})
	tensors := []*TensorMeta{ut1, ut2}

	// Should choose alice due to more existing tensors
	placement := tm.choosePrivatePlacement(candidates, tensors)
	assert.Equal(t, "alice", placement.partyCode)

	// Test fallback to any candidate when no existing tensors
	ut3 := &TensorMeta{ID: 3, Name: "tensor3"}
	placement = tm.choosePrivatePlacement(candidates, []*TensorMeta{ut3})
	assert.Contains(t, []string{"alice", "bob", "charlie"}, placement.partyCode)
}

func TestTensors(t *testing.T) {
	tm := NewTensorManager(1)
	assert.Empty(t, tm.Tensors())

	// Create some tensors
	tm.newTensor("tensor1")
	tm.newTensor("tensor2")

	tensors := tm.Tensors()
	assert.Len(t, tensors, 2)
	assert.Equal(t, "tensor1", tensors[0].Name)
	assert.Equal(t, "tensor2", tensors[1].Name)
}

func TestCreateTensorAs(t *testing.T) {
	tm := NewTensorManager(1)

	source := &graph.Tensor{
		Name:               "source_tensor",
		DType:              graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
		IsConstScalar:      true,
		OwnerPartyCode:     "alice",
		SecretStringOwners: []string{"alice", "bob"},
	}
	source.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)

	newTensor := tm.CreateTensorAs(source)
	assert.NotNil(t, newTensor)
	assert.Equal(t, source.Name, newTensor.Name)
	assert.True(t, source.DType.Equal(newTensor.DType))
	assert.Equal(t, source.IsConstScalar, newTensor.IsConstScalar)
	assert.Equal(t, source.OwnerPartyCode, newTensor.OwnerPartyCode)
	assert.Equal(t, source.SecretStringOwners, newTensor.SecretStringOwners)
	assert.Equal(t, source.Status(), newTensor.Status())
}

func TestCreateResultTensor(t *testing.T) {
	tm := NewTensorManager(1)

	source := &graph.Tensor{
		Name:          "source_tensor",
		DType:         graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64),
		IsConstScalar: false,
	}

	outputName := "result_output"
	resultTensor := tm.CreateResultTensor(source, outputName)
	assert.NotNil(t, resultTensor)
	assert.Equal(t, outputName, resultTensor.Name)
	assert.Equal(t, proto.TensorOptions_VALUE, resultTensor.Option)
	assert.Equal(t, proto.PrimitiveDataType_STRING, resultTensor.DType.DType)
	assert.Equal(t, []string{outputName}, resultTensor.StringS)
	assert.Equal(t, source.IsConstScalar, resultTensor.IsConstScalar)
}

func TestCreateUnbondedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	// Test public placement
	publicTensor := tm.CreateUnbondedTensor("public_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32), &publicPlacement{})
	assert.NotNil(t, publicTensor)
	assert.Equal(t, "public_tensor", publicTensor.Name)
	assert.Equal(t, proto.PrimitiveDataType_FLOAT32, publicTensor.DType.DType)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, publicTensor.Status())

	// Test private placement
	privateTensor := tm.CreateUnbondedTensor("private_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64), &privatePlacement{partyCode: "alice"})
	assert.NotNil(t, privateTensor)
	assert.Equal(t, "private_tensor", privateTensor.Name)
	assert.Equal(t, proto.PrimitiveDataType_INT64, privateTensor.DType.DType)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, privateTensor.Status())
	assert.Equal(t, "alice", privateTensor.OwnerPartyCode)

	// Test secret placement
	secretTensor := tm.CreateUnbondedTensor("secret_tensor", graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING), &secretPlacement{})
	assert.NotNil(t, secretTensor)
	assert.Equal(t, "secret_tensor", secretTensor.Name)
	assert.Equal(t, proto.PrimitiveDataType_STRING, secretTensor.DType.DType)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, secretTensor.Status())
}

func TestCreateConvertedTensor(t *testing.T) {
	tm := NewTensorManager(2)

	// Setup: create a tensor meta and its first placed tensor
	meta := &TensorMeta{ID: 1, Name: "test_tensor", DType: graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)}
	source, err := tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Test successful conversion to private
	privateTensor, err := tm.CreateConvertedTensor(source, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)
	assert.NotNil(t, privateTensor)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, privateTensor.Status())
	assert.Equal(t, "alice", privateTensor.OwnerPartyCode)

	// Test successful conversion to secret
	secretTensor, err := tm.CreateConvertedTensor(source, &secretPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, secretTensor)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, secretTensor.Status())

	// Test unsupported placement type
	// graph.Tensor also implements TensorPlacement interface, we do not need to mock new placement type here
	_, err = tm.CreateConvertedTensor(source, secretTensor)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported placement type")

	// Test without corresponding meta
	orphanTensor := &graph.Tensor{ID: 999, Name: "orphan"}
	convertedOrphan, err := tm.CreateConvertedTensor(orphanTensor, &publicPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, convertedOrphan)
}

func TestFindTensorForConversion(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	// Test no placed tensors
	_, err := tm.findTensorForConversion(meta, &secretPlacement{})
	assert.Error(t, err)

	// Setup: create public tensor
	_, err = tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Test conversion from public to secret (should fail - need private)
	_, err = tm.findTensorForConversion(meta, &secretPlacement{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no private tensor found")

	// Setup: create private tensor
	meta2 := &TensorMeta{ID: 2, Name: "private_tensor"}
	_, err = tm.CreateAndSetFirstTensor(meta2, &privatePlacement{partyCode: "alice"})
	assert.NoError(t, err)

	// Test conversion from private to secret
	source, err := tm.findTensorForConversion(meta2, &secretPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, source)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, source.Status())

	// Test conversion from any to public
	source, err = tm.findTensorForConversion(meta, &publicPlacement{})
	assert.NoError(t, err)
	assert.NotNil(t, source)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, source.Status())
}

func TestHasPlacedTensor(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	// Test no placed tensors
	assert.False(t, tm.hasPlacedTensor(meta, &publicPlacement{}))

	// Setup: create placed tensor
	_, err := tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
	assert.NoError(t, err)

	// Test existing placement
	assert.True(t, tm.hasPlacedTensor(meta, &publicPlacement{}))

	// Test non-existing placement
	assert.False(t, tm.hasPlacedTensor(meta, &privatePlacement{partyCode: "alice"}))
}

func TestNewTensorID(t *testing.T) {
	tm := NewTensorManager(100)

	id1 := tm.newTensorID()
	assert.Equal(t, 100, id1)

	id2 := tm.newTensorID()
	assert.Equal(t, 101, id2)

	// Verify the counter incremented
	assert.Equal(t, 102, tm.nextTensorID)
}

func TestTensorFromMeta(t *testing.T) {
	tm := NewTensorManager(1)

	meta := &TensorMeta{
		ID:            42,
		Name:          "test_tensor",
		DType:         graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32),
		IsConstScalar: true,
	}

	// Test public placement
	publicTensor := tm.tensorFromMeta(meta, &publicPlacement{})
	assert.NotNil(t, publicTensor)
	assert.Equal(t, meta.ID, publicTensor.ID)
	assert.Equal(t, meta.Name, publicTensor.Name)
	assert.True(t, meta.DType.Equal(publicTensor.DType))
	assert.Equal(t, meta.IsConstScalar, publicTensor.IsConstScalar)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, publicTensor.Status())

	// Test private placement
	privateTensor := tm.tensorFromMeta(meta, &privatePlacement{partyCode: "alice"})
	assert.NotNil(t, privateTensor)
	assert.Equal(t, "alice", privateTensor.OwnerPartyCode)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, privateTensor.Status())

	// Test secret placement
	secretTensor := tm.tensorFromMeta(meta, &secretPlacement{})
	assert.NotNil(t, secretTensor)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, secretTensor.Status())
}
