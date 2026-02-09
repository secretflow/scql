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
)

// MockTensorVisibilityChecker is a mock implementation of TensorVisibilityChecker for testing
type MockTensorVisibilityChecker struct {
	publicTensors map[int]bool
}

func NewMockTensorVisibilityChecker() *MockTensorVisibilityChecker {
	return &MockTensorVisibilityChecker{
		publicTensors: make(map[int]bool),
	}
}

func (m *MockTensorVisibilityChecker) IsTensorPublic(t *TensorMeta) bool {
	return m.publicTensors[t.ID]
}

func (m *MockTensorVisibilityChecker) SetTensorPublic(id int, isPublic bool) {
	m.publicTensors[id] = isPublic
}

func TestApplicableToAlwaysApply(t *testing.T) {
	csr := NewColumnSecurityRelaxationBase(true)
	tensor := &TensorMeta{ID: 1}
	mockTVC := NewMockTensorVisibilityChecker()

	// Should always return true regardless of tensor visibility
	result := csr.ApplicableTo(tensor, mockTVC)
	assert.True(t, result)

	// Even with public tensor
	mockTVC.SetTensorPublic(1, true)
	result = csr.ApplicableTo(tensor, mockTVC)
	assert.True(t, result)

	// Even with non-public tensor
	mockTVC.SetTensorPublic(1, false)
	result = csr.ApplicableTo(tensor, mockTVC)
	assert.True(t, result)
}

func TestApplicableToNotAlwaysApply(t *testing.T) {
	csr := NewColumnSecurityRelaxationBase(false)
	tensor := &TensorMeta{ID: 1}
	mockTVC := NewMockTensorVisibilityChecker()

	// Public tensor should be applicable
	mockTVC.SetTensorPublic(1, true)
	result := csr.ApplicableTo(tensor, mockTVC)
	assert.True(t, result)

	// Non-public tensor without MakeApplicable should not be applicable
	mockTVC.SetTensorPublic(1, false)
	result = csr.ApplicableTo(tensor, mockTVC)
	assert.False(t, result)

	// Non-public tensor with MakeApplicable should be applicable
	csr.MakeApplicable(tensor)
	result = csr.ApplicableTo(tensor, mockTVC)
	assert.True(t, result)
}

func TestMakeApplicable(t *testing.T) {
	csr := NewColumnSecurityRelaxationBase(false)
	tensor1 := &TensorMeta{ID: 1}
	tensor2 := &TensorMeta{ID: 2}

	// Initially empty
	assert.Empty(t, csr.withTensor)

	// Add first tensor
	csr.MakeApplicable(tensor1)
	assert.Contains(t, csr.withTensor, 1)
	assert.Len(t, csr.withTensor, 1)

	// Add second tensor
	csr.MakeApplicable(tensor2)
	assert.Contains(t, csr.withTensor, 1)
	assert.Contains(t, csr.withTensor, 2)
	assert.Len(t, csr.withTensor, 2)

	// Add same tensor again (should be idempotent)
	csr.MakeApplicable(tensor1)
	assert.Len(t, csr.withTensor, 2)
}

func TestAllApplicable(t *testing.T) {
	csr := NewColumnSecurityRelaxationBase(false)
	mockTVC := NewMockTensorVisibilityChecker()

	tensor1 := &TensorMeta{ID: 1}
	tensor2 := &TensorMeta{ID: 2}
	tensor3 := &TensorMeta{ID: 3}

	tensors := []*TensorMeta{tensor1, tensor2, tensor3}

	// Set tensor1 as public, others as non-public
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)
	mockTVC.SetTensorPublic(3, false)

	// AllApplicable should return false when not all are applicable
	result := csr.AllApplicable(tensors, mockTVC)
	assert.False(t, result)

	// Make tensor2 applicable
	csr.MakeApplicable(tensor2)
	result = csr.AllApplicable(tensors, mockTVC)
	assert.False(t, result)

	// Make tensor3 applicable
	csr.MakeApplicable(tensor3)
	result = csr.AllApplicable(tensors, mockTVC)
	assert.True(t, result)
}

func TestAllApplicableEmptyList(t *testing.T) {
	csr := NewColumnSecurityRelaxationBase(false)
	mockTVC := NewMockTensorVisibilityChecker()

	// Empty list should return true
	result := csr.AllApplicable([]*TensorMeta{}, mockTVC)
	assert.True(t, result)
}
