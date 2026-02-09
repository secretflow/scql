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
	"fmt"

	"slices"
)

// TensorProducerTracker is a map from tensor ID to the source operator that produces the tensor.
// If a Operator's output tensors contains tensor t1, then t1's producer is this Operator.
type TensorProducerTracker map[int]Operator

// GetProducer returns the source node that produces the given tensor.
// Returns an error if the tensor has no recorded source node.
func (t TensorProducerTracker) GetProducer(tensor *TensorMeta) (Operator, error) {
	node, ok := t[tensor.ID]
	if !ok {
		return nil, fmt.Errorf("GetProducer: unable to find tensor %v", tensor)
	}
	return node, nil
}

// SetProducer records that the given node produces the specified tensor.
func (t TensorProducerTracker) SetProducer(tensor *TensorMeta, node Operator) {
	t[tensor.ID] = node
}

// RemoveRecord removes the producer operator record for the given tensor.
func (t TensorProducerTracker) RemoveRecord(tensor *TensorMeta) {
	delete(t, tensor.ID)
}

// TensorConsumerTracker is a map from tensor ID to the user nodes that use the tensor.
// If a Operator's input tensors contains tensor t1, then this Operator is one of t1's consumers.
type TensorConsumerTracker map[int][]Operator

// GetConsumers returns all nodes that use the given tensor as input.
// Returns nil if the tensor has no recorded user nodes.
func (t TensorConsumerTracker) GetConsumers(tensor *TensorMeta) []Operator {
	nodes, ok := t[tensor.ID]
	if !ok {
		return nil
	}
	return nodes
}

// AddConsumer records that the given node uses the specified tensor as input.
func (t TensorConsumerTracker) AddConsumer(tensor *TensorMeta, node Operator) {
	nodes := t[tensor.ID]
	if slices.Contains(nodes, node) {
		return
	}
	t[tensor.ID] = append(nodes, node)
}

// RemoveRecord removes all consumer operator records for the given tensor.
func (t TensorConsumerTracker) RemoveRecord(tensor *TensorMeta) {
	delete(t, tensor.ID)
}

// UsedAsFilterMask returns true if the tensor is used as a filter mask
// in any OperatorFilter.
// This is useful for applying the security relaxation 'RevealFilterMask'
func (t TensorConsumerTracker) UsedAsFilterMask(tensor *TensorMeta) bool {
	nodes := t.GetConsumers(tensor)
	for _, node := range nodes {
		if filter, ok := node.(*OperatorFilter); ok {
			if filter.mask.ID == tensor.ID {
				return true
			}
		}
	}
	return false
}
