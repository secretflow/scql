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
	"github.com/secretflow/scql/pkg/interpreter/graph"
)

type SubDAG struct {
	Nodes map[*graph.ExecutionNode]bool
}

type GraphPartitioner struct {
	Graph   *graph.Graph
	SubDAGs []*SubDAG
}

func NewGraphPartitioner(input *graph.Graph) *GraphPartitioner {
	return &GraphPartitioner{
		Graph:   input,
		SubDAGs: make([]*SubDAG, 0),
	}
}

// NaivePartition a graph by topological sort order
func (p *GraphPartitioner) NaivePartition() error {
	p.Graph.UpdateTensorRefNum()
	nodes, err := p.Graph.TopologicalSort()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		subDAG := &SubDAG{
			Nodes: make(map[*graph.ExecutionNode]bool),
		}
		subDAG.Nodes[node] = true
		p.SubDAGs = append(p.SubDAGs, subDAG)
	}
	return nil
}
