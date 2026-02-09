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
	"strings"
)

type WorkerSubDAG struct {
	Nodes map[*ExecutionNode]bool
}

// String returns a string representation of the WorkerSubDAG
func (s *WorkerSubDAG) String() string {
	var sb strings.Builder
	sb.WriteString("WorkerSubDAG{\n")
	sb.WriteString(fmt.Sprintf("  Nodes: %d,\n", len(s.Nodes)))
	sb.WriteString("  ExecutionNodes: [\n")

	for node := range s.Nodes {
		sb.WriteString("    ")
		sb.WriteString(node.ToString())
		sb.WriteString(",\n")
	}

	sb.WriteString("  ]\n}")
	return sb.String()
}

type PartitionedPipeline struct {
	Batched       bool
	InputTensors  []*Tensor
	OutputTensors []*Tensor
	SubDAGs       []*WorkerSubDAG
}

// String returns a string representation of the PartitionedPipeline
func (p *PartitionedPipeline) String() string {
	var sb strings.Builder
	sb.WriteString("PartitionedPipeline{\n")
	sb.WriteString(fmt.Sprintf("  Batched: %t,\n", p.Batched))

	if p.Batched {
		sb.WriteString(fmt.Sprintf("  InputTensors: %d,\n", len(p.InputTensors)))
		sb.WriteString(fmt.Sprintf("  OutputTensors: %d,\n", len(p.OutputTensors)))
	}

	// Add SubDAGs details
	sb.WriteString("  SubDAGs: [\n")
	for _, subDAG := range p.SubDAGs {
		// Indent SubDAG content
		subDAGStr := subDAG.String()
		lines := strings.Split(subDAGStr, "\n")
		for _, line := range lines {
			if line != "" {
				sb.WriteString("    ")
				sb.WriteString(line)
				sb.WriteString("\n")
			}
		}
		sb.WriteString(",\n")
	}
	sb.WriteString("  ]\n}")
	return sb.String()
}

type GraphPartitioner struct {
	Graph     *Graph
	Pipelines []*PartitionedPipeline
}

func NewGraphPartitioner(input *Graph) *GraphPartitioner {
	return &GraphPartitioner{
		Graph: input,
	}
}

// NaivePartition a graph by topological sort order
func (p *GraphPartitioner) NaivePartition() error {
	p.Graph.UpdateTensorRefNum()
	pipelineNodes, err := p.Graph.TopologicalSort()
	if err != nil {
		return err
	}
	for i, nodes := range pipelineNodes {
		pipeline := &PartitionedPipeline{Batched: p.Graph.Pipelines[i].Batched}
		if pipeline.Batched {
			pipeline.InputTensors = p.Graph.Pipelines[i].InputTensors
			pipeline.OutputTensors = p.Graph.Pipelines[i].OutputTensors
		}
		for _, node := range nodes {
			subDAG := &WorkerSubDAG{
				Nodes: make(map[*ExecutionNode]bool),
			}
			subDAG.Nodes[node] = true
			pipeline.SubDAGs = append(pipeline.SubDAGs, subDAG)
		}
		p.Pipelines = append(p.Pipelines, pipeline)
	}
	return nil
}
