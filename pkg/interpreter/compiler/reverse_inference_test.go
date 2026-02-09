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

	"github.com/secretflow/scql/pkg/parser/ast"
)

func TestReverseInferenceOperatorResult(t *testing.T) {
	assert := assert.New(t)

	node := &OperatorResult{}
	vs := &VisibilitySolver{vt: NewVisibilityTable([]string{})}

	tensors, err := node.ReverseInfer(vs)
	assert.Error(err)
	assert.Contains(err.Error(), "OperatorResult is not any tensor's source node")
	assert.Nil(tensors)
}

func TestReverseInferenceOperatorDataSource(t *testing.T) {
	assert := assert.New(t)

	node := &OperatorDataSource{}
	vs := &VisibilitySolver{vt: NewVisibilityTable([]string{})}

	tensors, err := node.ReverseInfer(vs)
	assert.Error(err)
	assert.Contains(err.Error(), "OperatorDataSource should only exist in OperatorRunSQL's subPlanNodes")
	assert.Nil(tensors)
}

func TestReverseInferenceSimpleNodes(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name string
		node interface {
			ReverseInfer(*VisibilitySolver) ([]*TensorMeta, error)
		}
	}{
		{name: "OperatorRunSQL", node: &OperatorRunSQL{}},
		{name: "OperatorEQJoin", node: &OperatorEQJoin{}},
		{name: "OperatorLimit", node: &OperatorLimit{}},
		{name: "OperatorFilter", node: &OperatorFilter{}},
		{name: "OperatorBroadcastTo", node: &OperatorBroadcastTo{}},
		{name: "OperatorReduce", node: &OperatorReduce{}},
		{name: "OperatorGroupAgg", node: &OperatorGroupAgg{}},
		{name: "OperatorIn", node: &OperatorIn{}},
		{name: "OperatorConstant", node: &OperatorConstant{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vs := &VisibilitySolver{vt: NewVisibilityTable([]string{})}
			tensors, err := tc.node.ReverseInfer(vs)
			assert.NoError(err)
			assert.Nil(tensors)
		})
	}
}

func TestReverseInferenceCrossJoin(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2", "party3"})

	// Create tensors with proper IDs
	leftInput1 := &TensorMeta{ID: 1}
	leftOutput1 := &TensorMeta{ID: 2}
	leftInput2 := &TensorMeta{ID: 3}
	leftOutput2 := &TensorMeta{ID: 4}

	rightInput1 := &TensorMeta{ID: 5}
	rightOutput1 := &TensorMeta{ID: 6}
	rightInput2 := &TensorMeta{ID: 7}
	rightOutput2 := &TensorMeta{ID: 8}

	// Set initial visibilities
	vt.UpdateVisibility(leftOutput1, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(leftOutput2, NewVisibleParties([]string{"party2"}))
	vt.UpdateVisibility(rightOutput1, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(rightOutput2, NewVisibleParties([]string{"party3"}))

	node := &OperatorCrossJoin{
		leftInputs:   []*TensorMeta{leftInput1, leftInput2},
		leftOutputs:  []*TensorMeta{leftOutput1, leftOutput2},
		rightInputs:  []*TensorMeta{rightInput1, rightInput2},
		rightOutputs: []*TensorMeta{rightOutput1, rightOutput2},
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	assert.Len(tensors, 4)

	assert.Equal([]string{"party1"}, vt.TensorVisibleParties(leftInput1).GetParties())
	assert.Equal([]string{"party2"}, vt.TensorVisibleParties(leftInput2).GetParties())
	assert.Equal([]string{"party1"}, vt.TensorVisibleParties(rightInput1).GetParties())
	assert.Equal([]string{"party3"}, vt.TensorVisibleParties(rightInput2).GetParties())
}

func TestReverseInferenceConcat(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2"})

	output := &TensorMeta{ID: 1}
	input1 := &TensorMeta{ID: 2}
	input2 := &TensorMeta{ID: 3}
	input3 := &TensorMeta{ID: 4}

	vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))

	node := &OperatorConcat{
		output: output,
		inputs: []*TensorMeta{input1, input2, input3},
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	assert.Len(tensors, 3)

	expectedParties := []string{"party1"}
	assert.Equal(expectedParties, vt.TensorVisibleParties(input1).GetParties())
	assert.Equal(expectedParties, vt.TensorVisibleParties(input2).GetParties())
	assert.Equal(expectedParties, vt.TensorVisibleParties(input3).GetParties())
}

func TestReverseInferenceSort(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2"})

	output1 := &TensorMeta{ID: 1}
	payload1 := &TensorMeta{ID: 2}
	output2 := &TensorMeta{ID: 3}
	payload2 := &TensorMeta{ID: 4}

	vt.UpdateVisibility(output1, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(output2, NewVisibleParties([]string{"party2"}))

	node := &OperatorSort{
		outputs:  []*TensorMeta{output1, output2},
		payloads: []*TensorMeta{payload1, payload2},
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	assert.Len(tensors, 2)

	assert.Equal([]string{"party1"}, vt.TensorVisibleParties(payload1).GetParties())
	assert.Equal([]string{"party2"}, vt.TensorVisibleParties(payload2).GetParties())
}

func TestReverseInferenceRankWindow(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2"})

	output1 := &TensorMeta{ID: 1}
	payload1 := &TensorMeta{ID: 2}
	output2 := &TensorMeta{ID: 3}
	payload2 := &TensorMeta{ID: 4}

	vt.UpdateVisibility(output1, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(output2, NewVisibleParties([]string{"party2"}))

	node := &OperatorWindow{
		payloadOutputs: []*TensorMeta{output1, output2},
		payloads:       []*TensorMeta{payload1, payload2},
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	assert.Len(tensors, 2)

	assert.Equal([]string{"party1"}, vt.TensorVisibleParties(payload1).GetParties())
	assert.Equal([]string{"party2"}, vt.TensorVisibleParties(payload2).GetParties())
}

func TestReverseInferenceFunctionUnaryNot(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2"})

	output := &TensorMeta{ID: 100}
	input := &TensorMeta{ID: 101}

	vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(input, NewVisibleParties([]string{"party2"}))

	node := &OperatorFunction{
		funcName: ast.UnaryNot,
		inputs:   []*TensorMeta{input},
		output:   output,
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	// One tensor's visiblity was updated
	assert.Len(tensors, 1)
	// Party1 has been added to input's visible parties
	assert.Equal([]string{"party1", "party2"}, vt.TensorVisibleParties(input).GetParties())
}

func TestReverseInferenceFunctionStringOps(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name     string
		funcName string
	}{
		{name: "Lower", funcName: ast.Lower},
		{name: "Upper", funcName: ast.Upper},
		{name: "Trim", funcName: ast.Trim},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vt := NewVisibilityTable([]string{"party1", "party2"})

			output := &TensorMeta{ID: 200}
			input := &TensorMeta{ID: 201}

			vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))
			vt.UpdateVisibility(input, NewVisibleParties([]string{"party2"}))

			node := &OperatorFunction{
				funcName: tc.funcName,
				inputs:   []*TensorMeta{input},
				output:   output,
			}

			vs := &VisibilitySolver{vt: vt}
			tensors, err := node.ReverseInfer(vs)

			assert.NoError(err)
			// One tensor's visiblity was updated
			assert.Len(tensors, 1)
			// Party1 has been added to input's visible parties
			assert.Equal([]string{"party1", "party2"}, vt.TensorVisibleParties(input).GetParties())
		})
	}
}

func TestReverseInferenceFunctionUnaryMathOps(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name     string
		funcName string
	}{
		{name: "Ln", funcName: ast.Ln},
		{name: "Log10", funcName: ast.Log10},
		{name: "Log2", funcName: ast.Log2},
		{name: "Exp", funcName: ast.Exp},
		{name: "Sqrt", funcName: ast.Sqrt},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vt := NewVisibilityTable([]string{"party1", "party2"})

			output := &TensorMeta{ID: 300}
			input := &TensorMeta{ID: 301}

			vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))
			vt.UpdateVisibility(input, NewVisibleParties([]string{"party2"}))

			node := &OperatorFunction{
				funcName: tc.funcName,
				inputs:   []*TensorMeta{input},
				output:   output,
			}

			vs := &VisibilitySolver{vt: vt}
			tensors, err := node.ReverseInfer(vs)

			assert.NoError(err)
			// One tensor's visiblity was updated
			assert.Len(tensors, 1)
			// Party1 has been added to input's visible parties
			assert.Equal([]string{"party1", "party2"}, vt.TensorVisibleParties(input).GetParties())
		})
	}
}

func TestReverseInferenceFunctionBinaryOps(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name     string
		funcName string
	}{
		{name: "Plus", funcName: ast.Plus},
		{name: "Minus", funcName: ast.Minus},
		{name: "Mul", funcName: ast.Mul},
		{name: "Div", funcName: ast.Div},
		{name: "AddDate", funcName: ast.AddDate},
		{name: "SubDate", funcName: ast.SubDate},
		{name: "DateDiff", funcName: ast.DateDiff},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vt := NewVisibilityTable([]string{"party1", "party2", "party3"})

			output := &TensorMeta{ID: 400}
			input1 := &TensorMeta{ID: 401}
			input2 := &TensorMeta{ID: 402}

			// Set up scenario where intersection will produce updates
			vt.UpdateVisibility(output, NewVisibleParties([]string{"party1", "party2"}))
			vt.UpdateVisibility(input1, NewVisibleParties([]string{"party2", "party3"}))
			vt.UpdateVisibility(input2, NewVisibleParties([]string{"party1", "party3"}))

			node := &OperatorFunction{
				funcName: tc.funcName,
				inputs:   []*TensorMeta{input1, input2},
				output:   output,
			}

			intersection1 := VPIntersection(vt.TensorVisibleParties(input2), vt.TensorVisibleParties(output))
			assert.Len(intersection1.GetParties(), 1)
			intersection2 := VPIntersection(vt.TensorVisibleParties(input1), vt.TensorVisibleParties(output))
			assert.Len(intersection2.GetParties(), 1)

			vs := &VisibilitySolver{vt: vt}
			tensors, err := node.ReverseInfer(vs)

			assert.NoError(err)
			// Two inputs' visiblity was updated
			assert.Len(tensors, 2)

			// Intersection has been added to input's visible parties
			assert.Contains(vt.TensorVisibleParties(input1).GetParties(), intersection1.GetParties()[0])
			assert.Contains(vt.TensorVisibleParties(input2).GetParties(), intersection2.GetParties()[0])
		})
	}
}

func TestReverseInferenceFunctionBinaryOpsNoUpdate(t *testing.T) {
	assert := assert.New(t)

	vt := NewVisibilityTable([]string{"party1", "party2", "party3"})

	output := &TensorMeta{ID: 500}
	input1 := &TensorMeta{ID: 501}
	input2 := &TensorMeta{ID: 502}

	// Set up scenario where intersection will be empty (no updates)
	vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"party2"}))
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"party3"}))

	node := &OperatorFunction{
		funcName: ast.Plus,
		inputs:   []*TensorMeta{input1, input2},
		output:   output,
	}

	vs := &VisibilitySolver{vt: vt}
	tensors, err := node.ReverseInfer(vs)

	assert.NoError(err)
	assert.Len(tensors, 0) // No updates since intersection is empty

	// Verify visibilities remain unchanged
	assert.Equal([]string{"party2"}, vt.TensorVisibleParties(input1).GetParties())
	assert.Equal([]string{"party3"}, vt.TensorVisibleParties(input2).GetParties())
}

func TestReverseInferenceFunctionCommentedCases(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name     string
		funcName string
	}{
		{name: "Abs", funcName: ast.Abs},
		{name: "Floor", funcName: ast.Floor},
		{name: "Ceil", funcName: ast.Ceil},
		{name: "Round", funcName: ast.Round},
		{name: "Cos", funcName: ast.Cos},
		{name: "Sin", funcName: ast.Sin},
		{name: "Tan", funcName: ast.Tan},
		{name: "Acos", funcName: ast.Acos},
		{name: "Asin", funcName: ast.Asin},
		{name: "Atan", funcName: ast.Atan},
		{name: "Pow", funcName: ast.Pow},
		{name: "IntDiv", funcName: ast.IntDiv},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vt := NewVisibilityTable([]string{"party1", "party2"})

			output := &TensorMeta{ID: 600}
			input := &TensorMeta{ID: 601}

			vt.UpdateVisibility(output, NewVisibleParties([]string{"party1"}))
			vt.UpdateVisibility(input, NewVisibleParties([]string{"party2"}))

			node := &OperatorFunction{
				funcName: tc.funcName,
				inputs:   []*TensorMeta{input},
				output:   output,
			}

			vs := &VisibilitySolver{vt: vt}
			tensors, err := node.ReverseInfer(vs)

			assert.NoError(err)

			// These cases have no implementation (commented out)
			// So they should return empty tensors slice
			assert.Len(tensors, 0)

			// Verify input visibility remains unchanged
			assert.Equal([]string{"party2"}, vt.TensorVisibleParties(input).GetParties())
		})
	}
}
