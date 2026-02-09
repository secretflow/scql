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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
)

func createTestTensor(id int) *TensorMeta {
	return &TensorMeta{ID: id}
}

func TestInitDataourceVisibility(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Test case 1: Basic initialization
	t1 := createTestTensor(1)
	t2 := createTestTensor(2)
	node := &OperatorDataSource{
		outputs:     []*TensorMeta{t1, t2},
		sourceParty: "alice",
		originNames: []string{"col1", "col2"},
	}

	originalVisibility := map[string]*VisibleParties{
		"col1": NewVisibleParties([]string{"alice", "bob"}),
		"col2": NewVisibleParties([]string{"carol"}),
	}

	err := node.InitVis(vt, originalVisibility)
	assert.NoError(t, err)

	// Check visibility for first output
	vp1 := vt.TensorVisibleParties(t1)
	// vp1 contains "alice" and "bob" because they'are provided by the original visibility
	assert.True(t, vp1.Contains("alice"))
	assert.True(t, vp1.Contains("bob"))
	assert.False(t, vp1.Contains("carol"))

	// Check visibility for second output
	vp2 := vt.TensorVisibleParties(t2)
	// vp2 contains "carol" because it's provided by the original visibility
	assert.True(t, vp2.Contains("carol"))
	// vp2 contains "alice" because it's the source party
	assert.True(t, vp2.Contains("alice"))
	assert.False(t, vp2.Contains("bob"))

	// Test case 2: No original visibility
	vt2 := NewVisibilityTable(parties)
	t3 := createTestTensor(3)
	node2 := &OperatorDataSource{
		outputs:     []*TensorMeta{t3},
		sourceParty: "bob",
		originNames: []string{"col3"},
	}

	err = node2.InitVis(vt2, map[string]*VisibleParties{})
	assert.NoError(t, err)

	vp3 := vt2.TensorVisibleParties(t3)
	assert.True(t, vp3.Contains("bob"))
	assert.False(t, vp3.Contains("alice"))
	assert.False(t, vp3.Contains("carol"))

	// Test case 3: Case insensitive original visibility
	vt3 := NewVisibilityTable(parties)
	t4 := createTestTensor(4)
	node3 := &OperatorDataSource{
		outputs:     []*TensorMeta{t4},
		sourceParty: "carol",
		originNames: []string{"COL1"},
	}

	originalVisibility3 := map[string]*VisibleParties{
		"col1": NewVisibleParties([]string{"alice", "carol"}),
	}

	err = node3.InitVis(vt3, originalVisibility3)
	assert.NoError(t, err)

	vp4 := vt3.TensorVisibleParties(t4)
	assert.True(t, vp4.Contains("alice"))
	assert.True(t, vp4.Contains("carol"))
	assert.False(t, vp4.Contains("bob"))
}

func TestOperatorEQJoinInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	leftKey1 := createTestTensor(1)
	leftKey2 := createTestTensor(2)
	rightKey1 := createTestTensor(3)
	rightKey2 := createTestTensor(4)
	leftPayload1 := createTestTensor(5)
	leftPayload2 := createTestTensor(6)
	rightPayload1 := createTestTensor(7)
	rightPayload2 := createTestTensor(8)
	leftOutput1 := createTestTensor(9)
	leftOutput2 := createTestTensor(10)
	rightOutput1 := createTestTensor(11)
	rightOutput2 := createTestTensor(12)

	// Set initial visibility
	vt.UpdateVisibility(leftKey1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(leftKey2, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(rightKey1, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(rightKey2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(leftPayload1, NewVisibleParties([]string{"alice"}))
	vt.UpdateVisibility(leftPayload2, NewVisibleParties([]string{"bob"}))
	vt.UpdateVisibility(rightPayload1, NewVisibleParties([]string{"carol"}))
	vt.UpdateVisibility(rightPayload2, NewVisibleParties([]string{"bob", "carol"}))

	node := &OperatorEQJoin{
		leftKeys:      []*TensorMeta{leftKey1, leftKey2},
		rightKeys:     []*TensorMeta{rightKey1, rightKey2},
		leftPayloads:  []*TensorMeta{leftPayload1, leftPayload2},
		rightPayloads: []*TensorMeta{rightPayload1, rightPayload2},
		leftOutputs:   []*TensorMeta{leftOutput1, leftOutput2},
		rightOutputs:  []*TensorMeta{rightOutput1, rightOutput2},
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check left outputs visibility (intersection of keys and payloads)
	// keys visibility: {"bob"} (intersection of {"alice", "bob"} and {"bob", "carol"})
	// leftOutput1: intersection of {"bob"} and {"alice"} = empty
	assert.True(t, vt.TensorVisibleParties(leftOutput1).IsEmpty())

	// leftOutput2: intersection of {"bob"} and {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(leftOutput2).GetParties())

	// Check right outputs visibility
	// rightOutput1: intersection of {"bob"} and {"carol"} = empty
	assert.True(t, vt.TensorVisibleParties(rightOutput1).IsEmpty())

	// rightOutput2: intersection of {"bob"} and {"bob", "carol"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(rightOutput2).GetParties())
}

func TestOperatorCrossJoinInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	leftInput1 := createTestTensor(1)
	leftInput2 := createTestTensor(2)
	rightInput1 := createTestTensor(3)
	rightInput2 := createTestTensor(4)
	leftOutput1 := createTestTensor(5)
	leftOutput2 := createTestTensor(6)
	rightOutput1 := createTestTensor(7)
	rightOutput2 := createTestTensor(8)

	// Set initial visibility
	vt.UpdateVisibility(leftInput1, NewVisibleParties([]string{"alice"}))
	vt.UpdateVisibility(leftInput2, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(rightInput1, NewVisibleParties([]string{"bob"}))
	vt.UpdateVisibility(rightInput2, NewVisibleParties([]string{"carol"}))

	node := &OperatorCrossJoin{
		leftInputs:   []*TensorMeta{leftInput1, leftInput2},
		rightInputs:  []*TensorMeta{rightInput1, rightInput2},
		leftOutputs:  []*TensorMeta{leftOutput1, leftOutput2},
		rightOutputs: []*TensorMeta{rightOutput1, rightOutput2},
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check left outputs: should have same visibility as left inputs
	assert.Equal(t, []string{"alice"}, vt.TensorVisibleParties(leftOutput1).GetParties())
	assert.Equal(t, []string{"alice", "bob"}, vt.TensorVisibleParties(leftOutput2).GetParties())

	// Check right outputs: should have same visibility as right inputs
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(rightOutput1).GetParties())
	assert.Equal(t, []string{"carol"}, vt.TensorVisibleParties(rightOutput2).GetParties())
}

func TestOperatorLimitInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	input1 := createTestTensor(1)
	input2 := createTestTensor(2)
	output1 := createTestTensor(3)
	output2 := createTestTensor(4)

	// Set initial visibility
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"carol"}))

	node := &OperatorLimit{
		inputs:  []*TensorMeta{input1, input2},
		outputs: []*TensorMeta{output1, output2},
		offset:  10,
		count:   100,
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check outputs: should have same visibility as inputs
	assert.Equal(t, []string{"alice", "bob"}, vt.TensorVisibleParties(output1).GetParties())
	assert.Equal(t, []string{"carol"}, vt.TensorVisibleParties(output2).GetParties())
}

func TestOperatorFilterInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	mask := createTestTensor(1)
	input1 := createTestTensor(2)
	input2 := createTestTensor(3)
	output1 := createTestTensor(4)
	output2 := createTestTensor(5)

	// Set initial visibility
	vt.UpdateVisibility(mask, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "carol"}))
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"bob"}))

	node := &OperatorFilter{
		mask:    mask,
		inputs:  []*TensorMeta{input1, input2},
		outputs: []*TensorMeta{output1, output2},
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check outputs: should be intersection of mask visibility and input visibility
	// output1: {"alice", "bob"} ∩ {"alice", "carol"} = {"alice"}
	assert.Equal(t, []string{"alice"}, vt.TensorVisibleParties(output1).GetParties())

	// output2: {"alice", "bob"} ∩ {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output2).GetParties())
}

func TestOperatorBroadcastToInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	shapeRef := createTestTensor(1)
	scalar1 := createTestTensor(2)
	scalar2 := createTestTensor(3)
	output1 := createTestTensor(4)
	output2 := createTestTensor(5)

	// Set initial visibility
	vt.UpdateVisibility(shapeRef, NewVisibleParties([]string{"alice"})) // shapeRef visibility not used
	vt.UpdateVisibility(scalar1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(scalar2, NewVisibleParties([]string{"carol"}))

	node := &OperatorBroadcastTo{
		shapeRef: shapeRef,
		scalars:  []*TensorMeta{scalar1, scalar2},
		outputs:  []*TensorMeta{output1, output2},
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check outputs: should have same visibility as scalars
	assert.Equal(t, []string{"alice", "bob"}, vt.TensorVisibleParties(output1).GetParties())
	assert.Equal(t, []string{"carol"}, vt.TensorVisibleParties(output2).GetParties())
}

func TestOperatorConcatInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	input1 := createTestTensor(1)
	input2 := createTestTensor(2)
	input3 := createTestTensor(3)
	output := createTestTensor(4)

	// Set initial visibility
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(input3, NewVisibleParties([]string{"bob"}))

	node := &OperatorConcat{
		inputs: []*TensorMeta{input1, input2, input3},
		output: output,
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check output: should be intersection of all inputs
	// {"alice", "bob"} ∩ {"bob", "carol"} ∩ {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output).GetParties())
}

func TestOperatorSortInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	sortKey1 := createTestTensor(1)
	sortKey2 := createTestTensor(2)
	payload1 := createTestTensor(3)
	payload2 := createTestTensor(4)
	output1 := createTestTensor(5)
	output2 := createTestTensor(6)

	// Set initial visibility
	vt.UpdateVisibility(sortKey1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(sortKey2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(payload1, NewVisibleParties([]string{"alice", "carol"}))
	vt.UpdateVisibility(payload2, NewVisibleParties([]string{"bob"}))

	node := &OperatorSort{
		sortKeys:   []*TensorMeta{sortKey1, sortKey2},
		payloads:   []*TensorMeta{payload1, payload2},
		outputs:    []*TensorMeta{output1, output2},
		descending: slices.Repeat([]bool{false}, 2),
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check outputs: should be intersection of rank visibility and payload visibility
	// rank visibility: {"bob"} (intersection of {"alice", "bob"} and {"bob", "carol"})
	// output1: {"bob"} ∩ {"alice", "carol"} = empty
	assert.True(t, vt.TensorVisibleParties(output1).IsEmpty())

	// output2: {"bob"} ∩ {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output2).GetParties())
}

func TestOperatorReduceInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Test case 1: COUNT function should be public
	input1 := createTestTensor(1)
	output1 := createTestTensor(2)
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "bob"}))

	countFunc := &aggregation.AggFuncDesc{}
	countFunc.Name = ast.AggFuncCount
	node1 := &OperatorReduce{
		input:   input1,
		output:  output1,
		aggFunc: countFunc,
	}

	err := node1.InferVis(vt)
	assert.NoError(t, err)
	assert.True(t, vt.VisibilityPublic(vt.TensorVisibleParties(output1)))

	// Test case 2: SUM function should have same visibility as input
	input2 := createTestTensor(3)
	output2 := createTestTensor(4)
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"alice", "bob"}))

	sumFunc := &aggregation.AggFuncDesc{}
	sumFunc.Name = ast.AggFuncSum
	node2 := &OperatorReduce{
		input:   input2,
		output:  output2,
		aggFunc: sumFunc,
	}

	err = node2.InferVis(vt)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alice", "bob"}, vt.TensorVisibleParties(output2).GetParties())

	// Test case 3: AVG function should have same visibility as input
	input3 := createTestTensor(5)
	output3 := createTestTensor(6)
	vt.UpdateVisibility(input3, NewVisibleParties([]string{"carol"}))

	avgFunc := &aggregation.AggFuncDesc{}
	avgFunc.Name = ast.AggFuncAvg
	node3 := &OperatorReduce{
		input:   input3,
		output:  output3,
		aggFunc: avgFunc,
	}

	err = node3.InferVis(vt)
	assert.NoError(t, err)
	assert.Equal(t, []string{"carol"}, vt.TensorVisibleParties(output3).GetParties())

	// Test case 4: Unsupported function should return error
	input4 := createTestTensor(7)
	output4 := createTestTensor(8)
	unsupportedFunc := &aggregation.AggFuncDesc{}
	unsupportedFunc.Name = "unsupported"
	node4 := &OperatorReduce{
		input:   input4,
		output:  output4,
		aggFunc: unsupportedFunc,
	}

	err = node4.InferVis(vt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported agg func")
}

func TestOperatorGroupAggInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	groupKey1 := createTestTensor(1)
	groupKey2 := createTestTensor(2)
	payload1 := createTestTensor(3)
	payload2 := createTestTensor(4)
	payload3 := createTestTensor(5)
	output1 := createTestTensor(6)
	output2 := createTestTensor(7)
	output3 := createTestTensor(8)
	simpleCountOutput := createTestTensor(9)

	// Set initial visibility
	vt.UpdateVisibility(groupKey1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(groupKey2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(payload1, NewVisibleParties([]string{"alice", "carol"}))
	vt.UpdateVisibility(payload2, NewVisibleParties([]string{"bob"}))
	vt.UpdateVisibility(payload3, NewVisibleParties([]string{"carol"}))

	// Test case 1: COUNT function should have keys visibility
	countFunc := &aggregation.AggFuncDesc{}
	countFunc.Name = ast.AggFuncCount

	// Test case 2: SUM function should have intersection of keys and payload
	sumFunc := &aggregation.AggFuncDesc{}
	sumFunc.Name = ast.AggFuncSum

	node := &OperatorGroupAgg{
		groupKeys:          []*TensorMeta{groupKey1, groupKey2},
		aggArgs:            []*TensorMeta{payload1, payload2, payload3},
		argFuncOutputs:     []*TensorMeta{output1, output2, output3},
		simpleCountOutputs: []*TensorMeta{simpleCountOutput},
		aggFuncsWithArg: []*aggregation.AggFuncDesc{
			countFunc,
			sumFunc,
			sumFunc,
		},
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check keys visibility: {"bob"} (intersection of {"alice", "bob"} and {"bob", "carol"})
	keysVisibility := NewVisibleParties([]string{"bob"})

	// output1 (COUNT): should have keys visibility
	assert.Equal(t, keysVisibility.GetParties(), vt.TensorVisibleParties(output1).GetParties())

	// output2 (SUM): intersection of keys and payload2 = {"bob"} ∩ {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output2).GetParties())

	// output3 (SUM): intersection of keys and payload3 = {"bob"} ∩ {"carol"} = empty
	assert.True(t, vt.TensorVisibleParties(output3).IsEmpty())

	// simpleCountOutput: should have keys visibility
	assert.Equal(t, keysVisibility.GetParties(), vt.TensorVisibleParties(simpleCountOutput).GetParties())

	// Test case 4: Unsupported function should return error
	node2 := &OperatorGroupAgg{
		groupKeys:          []*TensorMeta{groupKey1},
		aggArgs:            []*TensorMeta{payload1},
		argFuncOutputs:     []*TensorMeta{createTestTensor(10)},
		simpleCountOutputs: []*TensorMeta{},
		aggFuncsWithArg: []*aggregation.AggFuncDesc{
			func() *aggregation.AggFuncDesc {
				agg := &aggregation.AggFuncDesc{}
				agg.Name = "unsupported"
				return agg
			}(),
		},
	}

	err = node2.InferVis(vt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported agg func")
}

func TestOperatorRankWindowInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	orderKey1 := createTestTensor(1)
	orderKey2 := createTestTensor(2)
	payload1 := createTestTensor(3)
	payload2 := createTestTensor(4)
	output1 := createTestTensor(5)
	output2 := createTestTensor(6)
	rank := createTestTensor(7)

	// Set initial visibility
	vt.UpdateVisibility(orderKey1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(orderKey2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(payload1, NewVisibleParties([]string{"alice", "carol"}))
	vt.UpdateVisibility(payload2, NewVisibleParties([]string{"bob"}))

	node := &OperatorWindow{
		orderKeys:      []*TensorMeta{orderKey1, orderKey2},
		payloads:       []*TensorMeta{payload1, payload2},
		payloadOutputs: []*TensorMeta{output1, output2},
		funcOutput:     rank,
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check rank visibility: should be intersection of order keys
	// {"alice", "bob"} ∩ {"bob", "carol"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(rank).GetParties())

	// Check outputs: should have same visibility as payloads (FIXME: should this consider keys?)
	assert.Equal(t, []string{"alice", "carol"}, vt.TensorVisibleParties(output1).GetParties())
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output2).GetParties())
}

func TestOperatorInInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Setup tensors
	left := createTestTensor(1)
	right := createTestTensor(2)
	output := createTestTensor(3)

	// Set initial visibility
	vt.UpdateVisibility(left, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(right, NewVisibleParties([]string{"bob", "carol"}))

	node := &OperatorIn{
		left:   left,
		right:  right,
		output: output,
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check output: should be intersection of left and right
	// {"alice", "bob"} ∩ {"bob", "carol"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output).GetParties())
}

func TestOperatorFunctionInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Test case 1: Multiple inputs
	input1 := createTestTensor(1)
	input2 := createTestTensor(2)
	input3 := createTestTensor(3)
	output := createTestTensor(4)

	// Set initial visibility
	vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "bob"}))
	vt.UpdateVisibility(input2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(input3, NewVisibleParties([]string{"bob"}))

	node := &OperatorFunction{
		inputs: []*TensorMeta{input1, input2, input3},
		output: output,
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check output: should be intersection of all inputs
	// {"alice", "bob"} ∩ {"bob", "carol"} ∩ {"bob"} = {"bob"}
	assert.Equal(t, []string{"bob"}, vt.TensorVisibleParties(output).GetParties())

	// Test case 2: Single input
	input4 := createTestTensor(5)
	output2 := createTestTensor(6)
	vt.UpdateVisibility(input4, NewVisibleParties([]string{"alice"}))

	node2 := &OperatorFunction{
		inputs: []*TensorMeta{input4},
		output: output2,
	}

	err = node2.InferVis(vt)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alice"}, vt.TensorVisibleParties(output2).GetParties())

	// Test case 3: No inputs (edge case)
	output3 := createTestTensor(7)
	node3 := &OperatorFunction{
		inputs: []*TensorMeta{},
		output: output3,
	}

	err = node3.InferVis(vt)
	assert.NoError(t, err)
	assert.True(t, vt.VisibilityPublic(vt.TensorVisibleParties(output3))) // Should be public when no inputs
}

func TestOperatorConstantInferVis(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	output := createTestTensor(1)

	node := &OperatorConstant{
		output: output,
		value:  nil, // value doesn't affect visibility
	}

	err := node.InferVis(vt)
	assert.NoError(t, err)

	// Check output: should always be public
	assert.True(t, vt.VisibilityPublic(vt.TensorVisibleParties(output)))
}

func TestTrivialInferVis(t *testing.T) {
	opDatasource := &OperatorDataSource{}
	err := opDatasource.InferVis(nil)
	assert.NoError(t, err)

	opRunSQL := &OperatorRunSQL{}
	err = opRunSQL.InferVis(nil)
	assert.NoError(t, err)

	opResult := &OperatorResult{}
	err = opResult.InferVis(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OperatorResult's InferVis should not be called")
}
