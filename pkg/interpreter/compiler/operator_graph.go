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
	"strings"
)

type OperatorGraph struct {
	operators       []Operator
	tensors         []*TensorMeta
	producerTracker *TensorProducerTracker
	consumerTracker *TensorConsumerTracker
}

// Nodes returns all execution nodes.
func (og *OperatorGraph) Nodes() []Operator {
	return og.operators
}

// Tensors returns the global tensor catalogue.
func (og *OperatorGraph) Tensors() []*TensorMeta {
	return og.tensors
}

func (og *OperatorGraph) String() string {
	var sb strings.Builder
	fmt.Fprintln(&sb, "Operators: ")
	for _, node := range og.operators {
		fmt.Fprintf(&sb, "%s\n", node)
		// fmt.Fprintf(&sb, "Kernel: %s\n", node.Kernel())
	}
	fmt.Fprintln(&sb, "Tensors: ")
	for _, tensor := range og.tensors {
		fmt.Fprintf(&sb, "Tensor[%d]: %s, Type: %v\n", tensor.ID, tensor.Name, tensor.DType)
	}
	return sb.String()
}

// DetailedString returns a detailed string representation of the OperatorGraph including visibility information
func (og *OperatorGraph) DetailedString(visibilityRegistry VisibilityRegistry) string {
	var sb strings.Builder
	// Operators section
	fmt.Fprintln(&sb, "Operators: ")
	for _, node := range og.operators {
		fmt.Fprintf(&sb, "%s\n", node)
	}

	// Tensors with visibility section
	fmt.Fprintln(&sb, "Tensors: ")
	for _, tensor := range og.tensors {
		fmt.Fprintf(&sb, "Tensor[%d]: %s, Type: %v", tensor.ID, tensor.Name, tensor.DType)
		if tensor.IsConstScalar {
			fmt.Fprintf(&sb, ", ConstScalar: true")
		}

		if visibilityRegistry != nil {
			visibleParties := visibilityRegistry.TensorVisibleParties(tensor)
			if visibleParties != nil && !visibleParties.IsEmpty() {
				fmt.Fprintf(&sb, ", VisibleParties: [%s]", visibleParties.String())
			} else {
				fmt.Fprintf(&sb, ", VisibleParties: []")
			}
		}
		fmt.Fprintln(&sb)
	}
	return sb.String()
}
