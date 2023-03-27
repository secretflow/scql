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

package optimizer

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// isBinaryOpWithShareInputs checks whether the binary operation node with SecretShare inputs
func isBinaryOpWithShareInputs(node *translator.ExecutionNode) bool {
	if exists := slices.Contains(operator.BinaryOps, node.OpType); !exists {
		return false
	}

	for _, ts := range node.Inputs {
		for _, tensor := range ts {
			if tensor.Status != proto.TensorStatus(translator.SecretStatus) {
				return false
			}
		}
	}

	return true
}

func mergeExecutionNodes(nodes []*translator.ExecutionNode) (*translator.ExecutionNode, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("must contains at least one ExecutionNode")
	}
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	inputs := make(map[string][]*translator.Tensor)
	outputs := make(map[string][]*translator.Tensor)
	for _, node := range nodes {
		for k, ts := range node.Inputs {
			_, ok := inputs[k]
			if !ok {
				inputs[k] = make([]*translator.Tensor, 0)
			}
			inputs[k] = append(inputs[k], ts...)
		}
		for k, ts := range node.Outputs {
			_, ok := outputs[k]
			if !ok {
				outputs[k] = make([]*translator.Tensor, 0)
			}
			outputs[k] = append(outputs[k], ts...)
		}
	}

	return &translator.ExecutionNode{
		Name:           nodes[0].Name,
		OpType:         nodes[0].OpType,
		Inputs:         inputs,
		Outputs:        outputs,
		Attributes:     nodes[0].Attributes,
		PartyCodeInfos: nodes[0].PartyCodeInfos,
	}, nil
}

// genBinaryNodeDigest generates digest for ExecutionNode
// It contains:
//   - OpType
//   - Input Data Type
//   - Party Codes
func genBinaryNodeDigest(node *translator.ExecutionNode) string {
	partyCodes := make([]string, len(node.PartyCodeInfos.GetParties()))
	copy(partyCodes, node.PartyCodeInfos.GetParties())
	sort.Strings(partyCodes)

	var dTypeStrings []string
	for _, ts := range node.Inputs {
		dTypeStrings = append(dTypeStrings, ts[0].DType.String())
	}

	return fmt.Sprintf("{OpType:%s,InputDataType:%s,PartyCodes:[%s]}", node.OpType, strings.Join(dTypeStrings, ","), strings.Join(partyCodes, ","))
}

// mergeMakeShare merges MakeShare nodes in a subDAG
func mergeMakeShare(inputDAG *SubDAG) bool {
	// 1. find candidate
	candidate := make([]*translator.ExecutionNode, 0)
	for node := range inputDAG.Nodes {
		if node.OpType != operator.OpNameMakeShare {
			continue
		}
		candidate = append(candidate, node)
	}

	if len(candidate) <= 1 {
		return false
	}

	sort.Slice(candidate, func(i, j int) bool { return candidate[i].ID < candidate[j].ID })

	// 2. create new node
	nodes := make([]*translator.ExecutionNode, 0)
	nodes = append(nodes, candidate...)

	newNode, err := mergeExecutionNodes(nodes)
	if err != nil {
		return false
	}
	newNode.ID = candidate[0].ID
	inputDAG.Nodes[newNode] = true

	// 3. delete old MakeShare nodes
	for _, node := range candidate {
		delete(inputDAG.Nodes, node)
	}
	return true
}

// mergeMakePrivate merges MakePrivate nodes in a subDAG
func mergeMakePrivate(inputDAG *SubDAG) bool {
	// 1. find candidate
	candidates := make(map[string][]*translator.ExecutionNode)
	for node := range inputDAG.Nodes {
		if node.OpType != operator.OpNameMakePrivate {
			continue
		}
		partyCode, ok := node.Attributes[operator.RevealToAttr].GetAttrValue().(string)
		if !ok {
			return false
		}
		_, ok = candidates[partyCode]
		if !ok {
			candidates[partyCode] = make([]*translator.ExecutionNode, 0)
		}
		candidates[partyCode] = append(candidates[partyCode], node)
	}

	for _, candidate := range candidates {
		if len(candidate) <= 1 {
			continue
		}

		sort.Slice(candidate, func(i, j int) bool { return candidate[i].ID < candidate[j].ID })

		// 2. create new node
		nodes := make([]*translator.ExecutionNode, 0)
		nodes = append(nodes, candidate...)

		newNode, err := mergeExecutionNodes(nodes)
		if err != nil {
			return false
		}
		newNode.ID = candidate[0].ID
		inputDAG.Nodes[newNode] = true

		// 3. delete old MakePrivate nodes
		for _, node := range candidate {
			delete(inputDAG.Nodes, node)
		}
	}
	return true
}

// mergePublish merges Publish nodes in a subDAG
func mergePublish(inputDAG *SubDAG) bool {
	// 1. find candidate
	candidates := make(map[string][]*translator.ExecutionNode)
	for node := range inputDAG.Nodes {
		if node.OpType != operator.OpNamePublish {
			continue
		}
		partyCode := node.PartyCodeInfos.GetParties()[0]
		_, ok := candidates[partyCode]
		if !ok {
			candidates[partyCode] = make([]*translator.ExecutionNode, 0)
		}
		candidates[partyCode] = append(candidates[partyCode], node)
	}

	for _, candidate := range candidates {
		if len(candidate) <= 1 {
			continue
		}

		sort.Slice(candidate, func(i, j int) bool { return candidate[i].ID < candidate[j].ID })

		// 2. create new node
		nodes := make([]*translator.ExecutionNode, 0)
		nodes = append(nodes, candidate...)

		newNode, err := mergeExecutionNodes(nodes)
		if err != nil {
			return false
		}
		newNode.ID = candidate[0].ID
		inputDAG.Nodes[newNode] = true

		// 3. delete old MakePrivate nodes
		for _, node := range candidate {
			delete(inputDAG.Nodes, node)
		}
	}
	return true
}

// mergeBinaryNode merges multiply binary operation nodes into one in a subDAG
func mergeBinaryNode(inputDAG *SubDAG) bool {
	// 1. find candidate
	candidates := make(map[string][]*translator.ExecutionNode)
	for node := range inputDAG.Nodes {
		if !isBinaryOpWithShareInputs(node) {
			continue
		}
		// NOTE(shunde.csd): Only binary nodes these share the same {node domain, node op type, input data type and party codes} can be merged together.
		nodeDigest := genBinaryNodeDigest(node)
		_, ok := candidates[nodeDigest]
		if !ok {
			candidates[nodeDigest] = make([]*translator.ExecutionNode, 0)
		}
		candidates[nodeDigest] = append(candidates[nodeDigest], node)
	}

	for _, candidate := range candidates {
		if len(candidate) <= 1 {
			continue
		}

		sort.Slice(candidate, func(i, j int) bool { return candidate[i].ID < candidate[j].ID })

		// 2. create new node
		nodes := make([]*translator.ExecutionNode, 0)
		nodes = append(nodes, candidate...)

		newNode, err := mergeExecutionNodes(nodes)
		if err != nil {
			return false
		}
		newNode.ID = candidate[0].ID
		inputDAG.Nodes[newNode] = true

		// 3. delete old MakePrivate nodes
		for _, node := range candidate {
			delete(inputDAG.Nodes, node)
		}
	}
	return true
}
