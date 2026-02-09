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
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
)

func TestOperatorResultInfer(t *testing.T) {
	assert := assert.New(t)

	node := &OperatorResult{}
	vs := &VisibilitySolver{vt: NewVisibilityTable([]string{"alice", "bob"})}

	tensors, err := node.Infer(vs, false)
	assert.NoError(err)
	assert.Nil(tensors)

	// Test with applySecurityRelaxation=true (should behave the same)
	tensors, err = node.Infer(vs, true)
	assert.NoError(err)
	assert.Nil(tensors)
}

func TestOperatorDataSourceInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{
		"col2": NewVisibleParties([]string{"bob"}),
	}, nil, false)

	// Test basic data source inference
	t1 := createTestTensor(1)
	t2 := createTestTensor(2)
	node := &OperatorDataSource{
		outputs:     []*TensorMeta{t1, t2},
		sourceParty: "alice",
		originNames: []string{"col1", "col2"},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)

	// Two tensors updated
	assert.Len(tensors, 2)
	assert.Contains(tensors, t1)
	assert.Contains(tensors, t2)

	// Verify visibility was set correctly
	vp1 := vs.vt.TensorVisibleParties(t1)
	assert.True(vp1.Contains("alice"))
	assert.False(vp1.Contains("bob"))

	vp2 := vs.vt.TensorVisibleParties(t2)
	assert.True(vp2.Contains("alice")) // source party
	assert.True(vp2.Contains("bob"))   // from original visibility
}

func TestOperatorRunSQLInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	// Set up sub plan with OperatorDataSource and OperatorFunction
	t1 := createTestTensor(1)
	t2 := createTestTensor(2)
	dataSource := &OperatorDataSource{
		outputs:     []*TensorMeta{t1, t2},
		sourceParty: "alice",
		originNames: []string{"col1", "col2"},
	}

	t3 := createTestTensor(3)
	add := &OperatorFunction{
		inputs: []*TensorMeta{t1, t2},
		output: t3,
	}

	node := &OperatorRunSQL{
		outputs:       []*TensorMeta{t3},
		sql:           "SELECT col1 + col2 FROM table1",
		sourceParty:   "alice",
		tableRefs:     []string{"table1"},
		subGraphNodes: []Operator{dataSource, add},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, t3)
}

func TestOperatorEQJoinInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	global := &GlobalSecurityRelaxation{RevealKeyAfterJoin: true}
	srm := NewSecurityRelaxationManager(global, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	// Setup tensors for join
	leftKey := createTestTensor(1)
	rightKey := createTestTensor(2)
	leftPayload := createTestTensor(3)
	rightPayload := createTestTensor(4)
	leftOutput := createTestTensor(5)
	rightOutput := createTestTensor(6)

	// Set initial visibility
	vs.vt.UpdateVisibility(leftKey, NewVisibleParties([]string{"alice", "bob"}))
	vs.vt.UpdateVisibility(rightKey, NewVisibleParties([]string{"bob", "carol"}))
	vs.vt.UpdateVisibility(leftPayload, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(rightPayload, NewVisibleParties([]string{"carol"}))

	node := &OperatorEQJoin{
		leftKeys:      []*TensorMeta{leftKey},
		rightKeys:     []*TensorMeta{rightKey},
		leftPayloads:  []*TensorMeta{leftPayload},
		rightPayloads: []*TensorMeta{rightPayload},
		leftOutputs:   []*TensorMeta{leftOutput},
		rightOutputs:  []*TensorMeta{rightOutput},
	}

	// Test without security relaxation
	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	// output tensors' visibility unchanged
	assert.Len(tensors, 0)

	// Test with security relaxation
	tensors, err = node.Infer(vs, true)
	require.NoError(err)
	assert.Len(tensors, 2)
	assert.Equal(vs.vt.TensorVisibleParties(leftOutput), NewVisibleParties([]string{"alice"}))
	assert.Equal(vs.vt.TensorVisibleParties(rightOutput), NewVisibleParties([]string{"carol"}))
}

func TestOperatorConstantInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vs := &VisibilitySolver{vt: NewVisibilityTable(parties)}

	output := createTestTensor(1)
	node := &OperatorConstant{
		output: output,
		value:  nil,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)

	// Verify constant is public
	assert.True(vs.vt.VisibilityPublic(vs.vt.TensorVisibleParties(output)))
}

func TestOperatorFilterInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	mask := createTestTensor(1)
	input := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(mask, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(input, NewVisibleParties([]string{"alice", "bob"}))

	node := &OperatorFilter{
		mask:    mask,
		inputs:  []*TensorMeta{input},
		outputs: []*TensorMeta{output},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice"})
}

func TestOperatorConcatInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	input1 := createTestTensor(1)
	input2 := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice", "bob"}))
	vs.vt.UpdateVisibility(input2, NewVisibleParties([]string{"bob", "carol"}))

	node := &OperatorConcat{
		inputs: []*TensorMeta{input1, input2},
		output: output,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"bob"})
}

func TestOperatorSortInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	sortKey := createTestTensor(1)
	payload := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(sortKey, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(payload, NewVisibleParties([]string{"alice", "bob"}))

	node := &OperatorSort{
		sortKeys: []*TensorMeta{sortKey},
		payloads: []*TensorMeta{payload},
		outputs:  []*TensorMeta{output},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice"})
}

func TestOperatorReduceInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	input := createTestTensor(1)
	output := createTestTensor(2)
	vs.vt.UpdateVisibility(input, NewVisibleParties([]string{"bob"}))

	sumFunc := &aggregation.AggFuncDesc{}
	sumFunc.Name = ast.AggFuncSum
	node := &OperatorReduce{
		input:   input,
		output:  output,
		aggFunc: sumFunc,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"bob"})
}

func TestOperatorGroupAggInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	groupKey := createTestTensor(1)
	payload := createTestTensor(2)
	countOutput := createTestTensor(3)

	vs.vt.UpdateVisibility(groupKey, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(payload, NewVisibleParties([]string{"bob"}))

	node := &OperatorGroupAgg{
		groupKeys:          []*TensorMeta{groupKey},
		aggArgs:            []*TensorMeta{payload},
		argFuncOutputs:     []*TensorMeta{},
		simpleCountOutputs: []*TensorMeta{countOutput},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, countOutput)
	assert.Equal(vt.TensorVisibleParties(countOutput).GetParties(), []string{"alice"})
}

func TestOperatorRankWindowInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	orderKey := createTestTensor(1)
	payload := createTestTensor(2)
	output := createTestTensor(3)
	rank := createTestTensor(4)

	vs.vt.UpdateVisibility(orderKey, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(payload, NewVisibleParties([]string{"alice", "bob"}))

	node := &OperatorWindow{
		orderKeys:      []*TensorMeta{orderKey},
		payloads:       []*TensorMeta{payload},
		payloadOutputs: []*TensorMeta{output},
		funcOutput:     rank,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 2)
	assert.Contains(tensors, output)
	assert.Contains(tensors, rank)
	assert.Equal(vt.TensorVisibleParties(rank).GetParties(), []string{"alice"})
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice", "bob"})

}

func TestOperatorInInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	left := createTestTensor(1)
	right := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(left, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(right, NewVisibleParties([]string{"alice", "bob"}))

	node := &OperatorIn{
		left:   left,
		right:  right,
		output: output,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice"})
}

func TestOperatorFunctionInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{RevealFilterMask: true}, map[string][]string{})

	// Test case 1: Basic function without ApplyRevealFilterMask
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)
	consumerTracker := make(TensorConsumerTracker)
	vs.consumerTracker = &consumerTracker

	input1 := createTestTensor(1)
	input2 := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(input1, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(input2, NewVisibleParties([]string{"alice", "bob"}))

	node := &OperatorFunction{
		inputs:   []*TensorMeta{input1, input2},
		output:   output,
		funcName: ast.GT,
	}

	// Test security relaxation ApplyRevealFilterMask not applied
	tensors, err := node.Infer(vs, true)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice"})

	// ApplyRevealFilterMask applied - output used as filter mask
	filterNode := &OperatorFilter{
		mask:    output,
		inputs:  []*TensorMeta{createTestTensor(4)},
		outputs: []*TensorMeta{createTestTensor(5)},
	}
	consumerTracker.AddConsumer(output, filterNode)

	// ApplyRevealFilterMask should make the output public
	tensors, err = node.Infer(vs, true)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	// The output should be public due to ApplyRevealFilterMask
	assert.True(vt.VisibilityPublic(vt.TensorVisibleParties(output)))
}

func TestOperatorBroadcastToInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	shapeRef := createTestTensor(1)
	scalar := createTestTensor(2)
	output := createTestTensor(3)

	vs.vt.UpdateVisibility(scalar, NewVisibleParties([]string{"alice", "bob"}))
	vs.vt.UpdateVisibility(shapeRef, NewVisibleParties([]string{"alice"}))

	node := &OperatorBroadcastTo{
		shapeRef: shapeRef,
		scalars:  []*TensorMeta{scalar},
		outputs:  []*TensorMeta{output},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"alice", "bob"})
}

func TestOperatorLimitInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	input := createTestTensor(1)
	output := createTestTensor(2)
	vs.vt.UpdateVisibility(input, NewVisibleParties([]string{"bob"}))

	node := &OperatorLimit{
		inputs:  []*TensorMeta{input},
		outputs: []*TensorMeta{output},
		offset:  10,
		count:   100,
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 1)
	assert.Contains(tensors, output)
	assert.Equal(vt.TensorVisibleParties(output).GetParties(), []string{"bob"})
}

func TestOperatorCrossJoinInfer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
	vs := NewVisibilitySolver(vt, srm, map[string]*VisibleParties{}, nil, false)

	leftInput := createTestTensor(1)
	rightInput := createTestTensor(2)
	leftOutput := createTestTensor(3)
	rightOutput := createTestTensor(4)

	vs.vt.UpdateVisibility(leftInput, NewVisibleParties([]string{"alice"}))
	vs.vt.UpdateVisibility(rightInput, NewVisibleParties([]string{"bob"}))

	node := &OperatorCrossJoin{
		leftInputs:   []*TensorMeta{leftInput},
		rightInputs:  []*TensorMeta{rightInput},
		leftOutputs:  []*TensorMeta{leftOutput},
		rightOutputs: []*TensorMeta{rightOutput},
	}

	tensors, err := node.Infer(vs, false)
	require.NoError(err)
	assert.Len(tensors, 2)
	assert.Contains(tensors, leftOutput)
	assert.Contains(tensors, rightOutput)
	assert.Equal(vt.TensorVisibleParties(leftOutput).GetParties(), []string{"alice"})
	assert.Equal(vt.TensorVisibleParties(rightOutput).GetParties(), []string{"bob"})
}
