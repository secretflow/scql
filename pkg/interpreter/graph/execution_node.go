// Copyright 2023 Ant Group Co., Ltd.
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

package graph

import (
	"fmt"
	"sort"
	"strings"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// ExecutionNode struct
type ExecutionNode struct {
	ID         int
	Name       string
	OpType     string
	Inputs     map[string][]*Tensor
	Outputs    map[string][]*Tensor
	Attributes map[string]*Attribute

	Edges map[*Edge]bool // Out Edges
	// party codes of participants
	Parties []string
}

// ToString dumps a debug string of the execution node
func (node *ExecutionNode) ToString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s:{", node.Name)

	fmt.Fprint(&builder, "in:[")
	var inputs []string
	for input := range node.Inputs {
		inputs = append(inputs, input)
	}
	sort.Strings(inputs)
	for _, input := range inputs {
		fmt.Fprintf(&builder, "%s:{", input)
		for _, argument := range node.Inputs[input] {
			fmt.Fprintf(&builder, "t_%d,", argument.ID)
		}
		fmt.Fprint(&builder, "},")
	}
	fmt.Fprint(&builder, "],")

	fmt.Fprint(&builder, "out:[")
	var outputs []string
	for output := range node.Outputs {
		outputs = append(outputs, output)
	}
	sort.Strings(outputs)
	for _, output := range outputs {
		fmt.Fprintf(&builder, "%s:{", output)
		for _, argument := range node.Outputs[output] {
			fmt.Fprintf(&builder, "t_%d,", argument.ID)
		}
		fmt.Fprint(&builder, "},")
	}
	fmt.Fprint(&builder, "],")

	fmt.Fprint(&builder, "attr:[")
	var keys []string
	for k := range node.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&builder, "%s:%s,", k, node.Attributes[k].ToString())
	}
	fmt.Fprint(&builder, "],")

	fmt.Fprint(&builder, "party:[")
	for _, p := range node.Parties {
		fmt.Fprintf(&builder, "%s,", p)
	}
	fmt.Fprint(&builder, "]")
	fmt.Fprint(&builder, "}")
	return builder.String()
}

// ToBriefString dumps a brief string of the execution node
func (node *ExecutionNode) ToBriefString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s", node.Name)
	fmt.Fprint(&builder, "\\l[")
	for _, p := range node.Parties {
		fmt.Fprintf(&builder, "%s,", p)
	}
	fmt.Fprint(&builder, "]")
	return builder.String()
}

func (node *ExecutionNode) GetAttrStrings(name string) ([]string, error) {
	if attr, ok := node.Attributes[name]; ok {
		return attr.GetStrings()
	}
	return nil, fmt.Errorf("getAttrStrings: attribute %s doesn't exists", name)
}

// ToProto serializes in-memory execution node to proto
func (node *ExecutionNode) ToProto() *proto.ExecNode {
	pb := &proto.ExecNode{
		NodeName: fmt.Sprintf("%s.%v", node.Name, node.ID),
		OpType:   node.OpType,
	}

	if node.Inputs != nil && len(node.Inputs) > 0 {
		pb.Inputs = make(map[string]*proto.TensorList)
		for k, inputs := range node.Inputs {

			pb.Inputs[k] = &proto.TensorList{
				Tensors: make([]*proto.Tensor, 0),
			}
			for _, i := range inputs {
				pb.Inputs[k].Tensors = append(pb.Inputs[k].Tensors, i.ToProto())
			}
		}
	}

	if node.Outputs != nil && len(node.Outputs) > 0 {
		pb.Outputs = make(map[string]*proto.TensorList)
		for k, outputs := range node.Outputs {
			pb.Outputs[k] = &proto.TensorList{
				Tensors: make([]*proto.Tensor, 0),
			}
			for _, o := range outputs {
				pb.Outputs[k].Tensors = append(pb.Outputs[k].Tensors, o.ToProto())
			}
		}
	}
	if node.Attributes != nil && len(node.Attributes) > 0 {
		pb.Attributes = make(map[string]*proto.AttributeValue)
		for k, attr := range node.Attributes {
			pb.Attributes[k] = attr.ToProto()
		}
	}
	return pb
}

func (node *ExecutionNode) UpdateInput(id int, newTensor *Tensor) {
	for k, inputs := range node.Inputs {
		var ids []int
		for i, input := range inputs {
			if input.ID == id {
				ids = append(ids, i)
			}
		}
		for _, i := range ids {
			node.Inputs[k][i] = newTensor
		}
	}
}

func (node *ExecutionNode) UpdateOutput(id int, newTensor *Tensor) {
	for k, outputs := range node.Outputs {
		var ids []int
		for i, input := range outputs {
			if input.ID == id {
				ids = append(ids, i)
			}
		}
		for _, i := range ids {
			node.Outputs[k][i] = newTensor
		}
	}
}
