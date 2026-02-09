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
	"sort"
)

// PartySubDAG struct
type PartySubDAG struct {
	Nodes     []*ExecutionNode
	PartyCode string
}

func splitExecutionNode(node *ExecutionNode) map[string]*ExecutionNode {
	result := make(map[string]*ExecutionNode)
	for _, party := range node.Parties {
		result[party] = &ExecutionNode{
			ID:         node.ID,
			Name:       node.Name,
			OpType:     node.OpType,
			Inputs:     node.Inputs,
			Outputs:    node.Outputs,
			Attributes: node.Attributes,
		}
	}
	return result
}

// Split a subDAG into several party subDAGs
func Split(subDag *WorkerSubDAG) (map[string]*PartySubDAG, error) {
	partyNodes := make([]*ExecutionNode, 0)
	singlePartyNodes := make([]*ExecutionNode, 0)
	for node := range subDag.Nodes {
		if len(node.Parties) == 1 {
			singlePartyNodes = append(singlePartyNodes, node)
		} else {
			partyNodes = append(partyNodes, node)
		}
	}
	sort.Slice(partyNodes, func(i, j int) bool { return partyNodes[i].Name < partyNodes[j].Name })
	sort.Slice(singlePartyNodes, func(i, j int) bool { return singlePartyNodes[i].Name < singlePartyNodes[j].Name })
	partyNodes = append(partyNodes, singlePartyNodes...)

	result := make(map[string]*PartySubDAG)
	for _, node := range partyNodes {
		nodes := splitExecutionNode(node)
		for party, n := range nodes {
			_, ok := result[party]
			if !ok {
				result[party] = &PartySubDAG{
					Nodes:     make([]*ExecutionNode, 0),
					PartyCode: party,
				}
			}
			result[party].Nodes = append(result[party].Nodes, n)
		}
	}
	return result, nil
}
