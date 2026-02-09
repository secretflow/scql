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

func TestTensorProducerTracker(t *testing.T) {
	tensor1 := &TensorMeta{ID: 1}
	tensor2 := &TensorMeta{ID: 2}

	// Create a node using existing OperatorConstant
	node := &OperatorConstant{output: tensor1}

	tracker := make(TensorProducerTracker)

	// Test SetProducer
	tracker.SetProducer(tensor1, node)

	// Test GetProducer - success
	retrievedNode, err := tracker.GetProducer(tensor1)
	assert.NoError(t, err)
	assert.Equal(t, node, retrievedNode)

	// Test GetProducer - not found
	retrievedNode, err = tracker.GetProducer(tensor2)
	assert.Error(t, err)
	assert.Nil(t, retrievedNode)

	// Test RemoveRecord
	tracker.RemoveRecord(tensor1)
	retrievedNode, err = tracker.GetProducer(tensor1)
	assert.Error(t, err)
	assert.Nil(t, retrievedNode)
}

func TestTensorConsumerTracker(t *testing.T) {
	tensor1 := &TensorMeta{ID: 1}
	tensor2 := &TensorMeta{ID: 2}

	// Create mock nodes
	mockNode1 := &OperatorConstant{output: tensor1}
	mockNode2 := &OperatorConstant{output: tensor2}
	mockFilterNode := &OperatorFilter{mask: tensor1}

	tracker := make(TensorConsumerTracker)

	// Test AddConsumer and GetConsumers - single node
	tracker.AddConsumer(tensor1, mockNode1)
	nodes := tracker.GetConsumers(tensor1)
	assert.Len(t, nodes, 1)
	assert.Equal(t, mockNode1, nodes[0])

	// Test AddConsumer - duplicate (should be ignored)
	tracker.AddConsumer(tensor1, mockNode1)
	nodes = tracker.GetConsumers(tensor1)
	assert.Len(t, nodes, 1)

	// Test AddConsumer - multiple nodes
	tracker.AddConsumer(tensor1, mockNode2)
	nodes = tracker.GetConsumers(tensor1)
	assert.Len(t, nodes, 2)

	// Test GetConsumers - not found
	nodes = tracker.GetConsumers(tensor2)
	assert.Nil(t, nodes)

	// Test UsedAsFilterMask - true
	tracker.AddConsumer(tensor1, mockFilterNode)
	assert.True(t, tracker.UsedAsFilterMask(tensor1))

	// Test UsedAsFilterMask - false
	assert.False(t, tracker.UsedAsFilterMask(tensor2))

	// Test RemoveRecord
	tracker.RemoveRecord(tensor1)
	nodes = tracker.GetConsumers(tensor1)
	assert.Nil(t, nodes)
	assert.False(t, tracker.UsedAsFilterMask(tensor1))
}

func TestUsedAsFilterMask(t *testing.T) {
	tensor := &TensorMeta{ID: 1}
	otherTensor := &TensorMeta{ID: 2}

	// Create nodes
	constantNode := &OperatorConstant{output: tensor}
	filterNode := &OperatorFilter{mask: tensor}

	tracker := make(TensorConsumerTracker)

	// Test with constant node only
	tracker.AddConsumer(tensor, constantNode)
	assert.False(t, tracker.UsedAsFilterMask(tensor))

	// Test with filter node
	tracker.AddConsumer(tensor, filterNode)
	assert.True(t, tracker.UsedAsFilterMask(tensor))

	// Test with non-filter node type
	assert.False(t, tracker.UsedAsFilterMask(otherTensor))
}
