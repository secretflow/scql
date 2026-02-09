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

func TestInferCSRDataSource(t *testing.T) {
	// Setup test data
	node := &OperatorDataSource{
		originNames: []string{"col1", "col2", "col3"},
		outputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
	}

	// Test case 1: No applicable columns
	csr := NewColumnSecurityRelaxationBase(false)
	applicableColNames := []string{"col4", "col5"}
	err := node.InitCSR(csr, applicableColNames)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.outputs[0], nil))
	assert.False(t, csr.ApplicableTo(node.outputs[1], nil))
	assert.False(t, csr.ApplicableTo(node.outputs[2], nil))

	// Test case 2: Some applicable columns
	csr = NewColumnSecurityRelaxationBase(false)
	applicableColNames = []string{"col1", "col3"}
	err = node.InitCSR(csr, applicableColNames)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], nil))  // col1
	assert.False(t, csr.ApplicableTo(node.outputs[1], nil)) // col2
	assert.True(t, csr.ApplicableTo(node.outputs[2], nil))  // col3

	// Test case 3: Case insensitive matching
	csr = NewColumnSecurityRelaxationBase(false)
	applicableColNames = []string{"COL1", "Col2"}
	err = node.InitCSR(csr, applicableColNames)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], nil))  // col1
	assert.True(t, csr.ApplicableTo(node.outputs[1], nil))  // col2
	assert.False(t, csr.ApplicableTo(node.outputs[2], nil)) // col3
}

func TestInferCSREQJoin(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorEQJoin{
		// join keys' CSR attributes do not affect the output tensors' CSR attributes
		leftPayloads: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		rightPayloads: []*TensorMeta{
			{ID: 3},
			{ID: 4},
		},
		leftOutputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
		rightOutputs: []*TensorMeta{
			{ID: 12},
			{ID: 13},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)
	mockTVC.SetTensorPublic(3, true)
	mockTVC.SetTensorPublic(4, false)

	// Test case 1: CSR always apply
	csr := NewColumnSecurityRelaxationBase(true)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.leftOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.leftOutputs[1], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[1], mockTVC))

	// Test case 2: Selective relaxation based on tensor visibility
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.leftOutputs[0], mockTVC))   // leftPayloads[0] is public
	assert.False(t, csr.ApplicableTo(node.leftOutputs[1], mockTVC))  // leftPayloads[1] is not public
	assert.True(t, csr.ApplicableTo(node.rightOutputs[0], mockTVC))  // rightPayloads[0] is public
	assert.False(t, csr.ApplicableTo(node.rightOutputs[1], mockTVC)) // rightPayloads[1] is not public

	// Test case 3: Make applicable tensors
	csr.MakeApplicable(node.leftPayloads[1])
	csr.MakeApplicable(node.rightPayloads[1])
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.leftOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.leftOutputs[1], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[1], mockTVC))
}

func TestInferCSRCrossJoin(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorCrossJoin{
		leftInputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		rightInputs: []*TensorMeta{
			{ID: 3},
			{ID: 4},
		},
		leftOutputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
		rightOutputs: []*TensorMeta{
			{ID: 12},
			{ID: 13},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)
	mockTVC.SetTensorPublic(3, true)
	mockTVC.SetTensorPublic(4, false)

	// Test case 1: CSR always apply
	csr := NewColumnSecurityRelaxationBase(true)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.leftOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.leftOutputs[1], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[1], mockTVC))

	// Test case 2: Selective relaxation
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.leftOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.leftOutputs[1], mockTVC))
	assert.True(t, csr.ApplicableTo(node.rightOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.rightOutputs[1], mockTVC))
}

func TestInferCSRLimit(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorLimit{
		inputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		outputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)

	// Test case 1: CSR always apply
	csr := NewColumnSecurityRelaxationBase(true)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 2: Selective relaxation
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))
}

func TestInferCSRFilter(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorFilter{
		mask: &TensorMeta{ID: 0},
		inputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		outputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(0, true)  // mask is public
	mockTVC.SetTensorPublic(1, true)  // input 1 is public
	mockTVC.SetTensorPublic(2, false) // input 2 is not public

	// Test case 1: CSR always apply
	csr := NewColumnSecurityRelaxationBase(true)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 2: Selective relaxation
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 3: Mask not applicable
	mockTVC.SetTensorPublic(0, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))
}

func TestInferCSRBroadcastTo(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorBroadcastTo{
		shapeRef: &TensorMeta{ID: 0},
		scalars: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		outputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(0, true)  // shapeRef is public
	mockTVC.SetTensorPublic(1, true)  // scalar 1 is public
	mockTVC.SetTensorPublic(2, false) // scalar 2 is not public

	// Test case 1: CSR always apply
	csr := NewColumnSecurityRelaxationBase(true)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 2: Selective relaxation
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 3: ShapeRef not applicable
	mockTVC.SetTensorPublic(0, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))
}

func TestInferCSRConcat(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorConcat{
		inputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
		output: &TensorMeta{ID: 10},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)
	mockTVC.SetTensorPublic(3, true)

	// Test case 1: All inputs are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 2: Not all inputs are public
	mockTVC.SetTensorPublic(3, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 3: Make applicable
	csr.MakeApplicable(node.inputs[2])
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 4: Empty inputs
	emptyNode := &OperatorConcat{
		inputs: []*TensorMeta{},
		output: &TensorMeta{ID: 20},
	}
	csr = NewColumnSecurityRelaxationBase(false)
	err = emptyNode.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(emptyNode.output, mockTVC))
}

func TestInferCSRSort(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorSort{
		sortKeys: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		payloads: []*TensorMeta{
			{ID: 3},
			{ID: 4},
		},
		outputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)
	mockTVC.SetTensorPublic(3, true)
	mockTVC.SetTensorPublic(4, false)

	// Test case 1: All sort keys are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))

	// Test case 2: Not all sort keys are public
	mockTVC.SetTensorPublic(2, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.outputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.outputs[1], mockTVC))
}

func TestInferCSRReduce(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorReduce{
		input:  &TensorMeta{ID: 1},
		output: &TensorMeta{ID: 10},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)

	// Test case 1: Input is public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 2: Input is not public
	mockTVC.SetTensorPublic(1, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 3: Make applicable
	csr.MakeApplicable(node.input)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))
}

func TestInferCSRGroupAgg(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorGroupAgg{
		groupKeys: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
		aggArgs: []*TensorMeta{
			{ID: 3},
			{ID: 4},
		},
		simpleCountOutputs: []*TensorMeta{
			{ID: 20},
			{ID: 21},
		},
		argFuncOutputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)
	mockTVC.SetTensorPublic(3, true)
	mockTVC.SetTensorPublic(4, false)

	// Test case 1: All group keys are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.argFuncOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.argFuncOutputs[1], mockTVC))
	assert.True(t, csr.ApplicableTo(node.simpleCountOutputs[0], mockTVC))
	assert.True(t, csr.ApplicableTo(node.simpleCountOutputs[1], mockTVC))

	// Test case 2: Not all group keys are public
	mockTVC.SetTensorPublic(2, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.argFuncOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.argFuncOutputs[1], mockTVC))
	assert.False(t, csr.ApplicableTo(node.simpleCountOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.simpleCountOutputs[1], mockTVC))
}

func TestInferCSRRankWindow(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorWindow{
		partitionKeys: []*TensorMeta{
			{ID: 1},
		},
		orderKeys: []*TensorMeta{
			{ID: 2},
		},
		payloads: []*TensorMeta{
			{ID: 3},
			{ID: 4},
		},
		funcOutput: &TensorMeta{ID: 0},
		payloadOutputs: []*TensorMeta{
			{ID: 10},
			{ID: 11},
		},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)
	mockTVC.SetTensorPublic(3, true)
	mockTVC.SetTensorPublic(4, false)

	// Test case 1: All partition and order keys are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.funcOutput, mockTVC))
	assert.True(t, csr.ApplicableTo(node.payloadOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.payloadOutputs[1], mockTVC))

	// Test case 2: Not all partition keys are public
	mockTVC.SetTensorPublic(1, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.funcOutput, mockTVC))
	assert.False(t, csr.ApplicableTo(node.payloadOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.payloadOutputs[1], mockTVC))

	// Test case 3: Not all order keys are public
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.funcOutput, mockTVC))
	assert.False(t, csr.ApplicableTo(node.payloadOutputs[0], mockTVC))
	assert.False(t, csr.ApplicableTo(node.payloadOutputs[1], mockTVC))
}

func TestInferCSRIn(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorIn{
		left:   &TensorMeta{ID: 1},
		right:  &TensorMeta{ID: 2},
		output: &TensorMeta{ID: 10},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)

	// Test case 1: Both inputs are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 2: Left input not public
	mockTVC.SetTensorPublic(1, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 3: Right input not public
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 4: Make applicable
	csr.MakeApplicable(node.left)
	csr.MakeApplicable(node.right)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))
}

func TestInferCSRFunction(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Setup test data
	node := &OperatorFunction{
		inputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
		output: &TensorMeta{ID: 10},
	}

	// Set tensor visibility
	mockTVC.SetTensorPublic(1, true)
	mockTVC.SetTensorPublic(2, true)
	mockTVC.SetTensorPublic(3, true)

	// Test case 1: All inputs are public
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 2: Not all inputs are public
	mockTVC.SetTensorPublic(3, false)
	csr = NewColumnSecurityRelaxationBase(false)
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 3: Make applicable
	csr.MakeApplicable(node.inputs[2])
	err = node.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(node.output, mockTVC))

	// Test case 4: Empty inputs
	emptyNode := &OperatorFunction{
		inputs: []*TensorMeta{},
		output: &TensorMeta{ID: 20},
	}
	csr = NewColumnSecurityRelaxationBase(false)
	err = emptyNode.InferCSR(csr, mockTVC)
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(emptyNode.output, mockTVC))
}

func TestInferCSRTrivial(t *testing.T) {
	opDataSource := &OperatorDataSource{}
	err := opDataSource.InferCSR(nil, nil)
	assert.NoError(t, err)

	opRunSQL := &OperatorRunSQL{}
	err = opRunSQL.InferCSR(nil, nil)
	assert.NoError(t, err)

	opConstant := &OperatorConstant{}
	err = opConstant.InferCSR(nil, nil)
	assert.NoError(t, err)

	opResult := &OperatorResult{}
	err = opResult.InferCSR(nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OperatorResult's InferCSR should not be called")
}

func TestInferCSRRegressionEdgeCases(t *testing.T) {
	mockTVC := NewMockTensorVisibilityChecker()

	// Test case 1: Empty applicableColNames for DataSource
	node := &OperatorDataSource{
		originNames: []string{"col1", "col2"},
		outputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
		},
	}
	csr := NewColumnSecurityRelaxationBase(false)
	err := node.InitCSR(csr, []string{})
	assert.NoError(t, err)
	assert.False(t, csr.ApplicableTo(node.outputs[0], nil))
	assert.False(t, csr.ApplicableTo(node.outputs[1], nil))

	// Test case 2: Empty tensors for other nodes
	emptyNode := &OperatorLimit{
		inputs:  []*TensorMeta{},
		outputs: []*TensorMeta{},
	}
	err = emptyNode.InferCSR(csr, mockTVC)
	assert.NoError(t, err)

	// Test case 3: Nil tensors handling
	nilNode := &OperatorReduce{
		input:  nil,
		output: nil,
	}
	err = nilNode.InferCSR(csr, mockTVC)
	assert.NoError(t, err)

	// Test case 4: Case sensitivity edge case
	caseNode := &OperatorDataSource{
		originNames: []string{"Col1", "COL2", "col3"},
		outputs: []*TensorMeta{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
	}
	csr = NewColumnSecurityRelaxationBase(false)
	err = caseNode.InitCSR(csr, []string{"COL1", "col2", "COL3"})
	assert.NoError(t, err)
	assert.True(t, csr.ApplicableTo(caseNode.outputs[0], nil)) // Col1 matches COL1
	assert.True(t, csr.ApplicableTo(caseNode.outputs[1], nil)) // COL2 matches col2
	assert.True(t, csr.ApplicableTo(caseNode.outputs[2], nil)) // col3 matches COL3
}
