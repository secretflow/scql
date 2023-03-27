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
	"sort"

	"github.com/secretflow/scql/pkg/interpreter/translator"
)

// PartySubDAG struct
type PartySubDAG struct {
	Nodes      []*translator.ExecutionNode
	PartyCode  string
	PartyURL   string
	Credential string
}

func splitExecutionNode(node *translator.ExecutionNode) map[string]*translator.ExecutionNode {
	result := make(map[string]*translator.ExecutionNode)
	for _, party := range node.PartyCodeInfos.GetParties() {
		result[party] = &translator.ExecutionNode{
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
func Split(subDag *SubDAG) (map[string]*PartySubDAG, error) {
	partyNodes := make([]*translator.ExecutionNode, 0)
	singlePartyNodes := make([]*translator.ExecutionNode, 0)
	for node := range subDag.Nodes {
		if len(node.PartyCodeInfos.GetParties()) == 1 {
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
				url, err := node.PartyCodeInfos.GetUrlByParty(party)
				if err != nil {
					return nil, err
				}
				credential, err := node.PartyCodeInfos.GetCredentialByParty(party)
				if err != nil {
					return nil, err
				}
				result[party] = &PartySubDAG{
					Nodes:      make([]*translator.ExecutionNode, 0),
					PartyCode:  party,
					PartyURL:   url,
					Credential: credential,
				}
			}
			result[party].Nodes = append(result[party].Nodes, n)
		}
	}
	return result, nil
}
