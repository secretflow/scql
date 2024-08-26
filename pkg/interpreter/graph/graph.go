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
	"slices"
	"sort"
	"strings"
)

// Edge struct of a dag
type Edge struct {
	From  *ExecutionNode
	To    *ExecutionNode
	Value *Tensor
}

type Pipeline struct {
	Batched bool
	// tensors created by upstream pipeline but used in current pipeline
	InputTensors []*Tensor
	// tensors created by current pipeline but used in downstream pipeline
	OutputTensors []*Tensor
	Nodes         map[*ExecutionNode]bool
}

// Graph struct
type Graph struct {
	Pipelines   []*Pipeline
	NodeCnt     int
	OutputNames []string
	PartyInfo   *PartyInfo
}

func (graph *Graph) GetParties() []string {
	return graph.PartyInfo.GetParties()
}

func (graph *Graph) GetUrlByParty(party string) (string, error) {
	return graph.PartyInfo.GetUrlByParty(party)
}

func (graph *Graph) UpdateTensorRefNum() {
	for _, pipeline := range graph.Pipelines {
		for node := range pipeline.Nodes {
			for _, ts := range node.Inputs {
				for _, t := range ts {
					t.RefNum += 1
				}
			}
		}
	}
}

// TopologicalSort of the dag
func (graph *Graph) TopologicalSort() ([][]*ExecutionNode, error) {
	var outputs [][]*ExecutionNode
	for _, pipeline := range graph.Pipelines {
		var nodes []*ExecutionNode
		var queue []*ExecutionNode

		indegrees := make(map[*ExecutionNode]int)
		for node := range pipeline.Nodes {
			indegrees[node] = 0
		}

		for node := range pipeline.Nodes {
			for edge := range node.Edges {
				if _, ok := indegrees[edge.To]; ok {
					indegrees[edge.To]++
				}
			}
		}

		for k, v := range indegrees {
			if v == 0 {
				queue = append(queue, k)
			}
		}

		// NOTE(yang.y): sort nodes in the first queue to enforce determinism
		sort.Slice(queue, func(i, j int) bool { return queue[i].ID < queue[j].ID })

		count := 0
		for count = 0; len(queue) != 0; count++ {
			cur := queue[0]
			nodes = append(nodes, cur)
			queue = queue[1:]

			toAppend := []*ExecutionNode{}
			for v := range cur.Edges {
				indegrees[v.To] = indegrees[v.To] - 1
				if indegrees[v.To] == 0 {
					toAppend = append(toAppend, v.To)
				}
			}
			// NOTE(yang.y): sort nodes to be appended to enforce determinism
			sort.Slice(toAppend, func(i, j int) bool { return toAppend[i].ID < toAppend[j].ID })
			queue = append(queue, toAppend...)
		}
		if count != len(pipeline.Nodes) {
			// circle in DAG!
			return nil, fmt.Errorf("topological sort fail: maybe circle in graph")
		}
		outputs = append(outputs, nodes)
	}

	return outputs, nil
}

// DumpGraphviz dumps a graph viz for visualization
func (graph *Graph) DumpGraphviz() string {
	var builder strings.Builder
	fmt.Fprintln(&builder, "digraph G {")
	convertToSingleQuote := func(s string) string {
		return strings.ReplaceAll(s, "\"", "'")
	}

	nodes := []*ExecutionNode{}
	for _, pipeline := range graph.Pipelines {
		for n := range pipeline.Nodes {
			nodes = append(nodes, n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	for _, node := range nodes {
		fmt.Fprintf(&builder, "%d [label=\"%s\"]\n", node.ID, convertToSingleQuote(node.ToString()))
	}

	var all []string
	for _, node := range nodes {
		for edge := range node.Edges {
			all = append(all, fmt.Sprintf("%d -> %d [label = \"%s\"]\n", edge.From.ID,
				edge.To.ID, convertToSingleQuote(edge.Value.ToString())))
		}
	}
	sort.Strings(all)
	fmt.Fprint(&builder, strings.Join(all, ""))
	fmt.Fprint(&builder, "}")
	return builder.String()
}

// DumpBriefPipeline dumps pipeline to string
func (graph *Graph) DumpBriefPipeline() string {
	var builder strings.Builder
	fmt.Fprintln(&builder)
	for i, pipeline := range graph.Pipelines {
		fmt.Fprintln(&builder, fmt.Sprintf("pipeline %d {", i))
		fmt.Fprintln(&builder, fmt.Sprintf("Batched: %v", pipeline.Batched))
		nodes := []*ExecutionNode{}
		for n := range pipeline.Nodes {
			nodes = append(nodes, n)
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
		var nodeNames []string
		for _, node := range nodes {
			nodeNames = append(nodeNames, fmt.Sprintf("%s_%d", node.Name, node.ID))
		}
		fmt.Fprintf(&builder, "node: %+v\n", nodeNames)
		var inputTensorIDs []string
		var outputTensorIDs []string
		for _, t := range pipeline.InputTensors {
			inputTensorIDs = append(inputTensorIDs, fmt.Sprintf("t_%d", t.ID))
		}
		for _, t := range pipeline.OutputTensors {
			outputTensorIDs = append(outputTensorIDs, fmt.Sprintf("t_%d", t.ID))
		}
		slices.Sort(inputTensorIDs)
		slices.Sort(outputTensorIDs)
		fmt.Fprint(&builder, fmt.Sprintf("Inputs: %+v\n", inputTensorIDs))
		fmt.Fprint(&builder, fmt.Sprintf("Outputs: %+v\n", outputTensorIDs))
		fmt.Fprint(&builder, "}\n")
	}
	return builder.String()
}

// DumpBriefGraphviz dumps a brief graph viz for visualization
func (graph *Graph) DumpBriefGraphviz() string {
	var builder strings.Builder
	fmt.Fprintln(&builder, "digraph G {")
	convertToSingleQuote := func(s string) string {
		return strings.ReplaceAll(s, "\"", "'")
	}
	nodes := []*ExecutionNode{}
	for _, pipeline := range graph.Pipelines {
		for n := range pipeline.Nodes {
			nodes = append(nodes, n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	for _, node := range nodes {
		fmt.Fprintf(&builder, "%d [label=\"%s\"]\n", node.ID, convertToSingleQuote(node.ToBriefString()))
	}

	var all []string
	for _, node := range nodes {
		for edge := range node.Edges {
			all = append(all, fmt.Sprintf("%d -> %d [label = \"%s\"]\n", edge.From.ID,
				edge.To.ID, convertToSingleQuote(edge.Value.ToBriefString())))
		}
	}
	sort.Strings(all)
	fmt.Fprint(&builder, strings.Join(all, ""))
	fmt.Fprint(&builder, "}")
	return builder.String()
}

func (graph *Graph) EliminateIsolatedNodes() {
	indegrees := make(map[*ExecutionNode]int)
	outdegrees := make(map[*ExecutionNode]int)
	for _, pipeline := range graph.Pipelines {
		for node := range pipeline.Nodes {
			indegrees[node] = 0
			outdegrees[node] = 0
		}
		for node := range pipeline.Nodes {
			for edge := range node.Edges {
				indegrees[edge.To]++
				outdegrees[edge.From]++
			}
		}
		candidates := make([]*ExecutionNode, 0)

		for node, indegree := range indegrees {
			if indegree == 0 {
				outdegree := outdegrees[node]
				if outdegree == 0 {
					candidates = append(candidates, node)
				}
			}
		}

		for _, candidate := range candidates {
			delete(pipeline.Nodes, candidate)
		}
	}
}

func (graph *Graph) EliminateIsolatedEdges() {
	for _, pipeline := range graph.Pipelines {
		for node := range pipeline.Nodes {
			isolatedEdges := make([]*Edge, 0)
			for edge := range node.Edges {
				_, ok := pipeline.Nodes[edge.To]
				if !ok {
					isolatedEdges = append(isolatedEdges, edge)
				}
			}

			for _, e := range isolatedEdges {
				delete(node.Edges, e)
			}
		}
	}
}
