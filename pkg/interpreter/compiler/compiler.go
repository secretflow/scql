// Copyright 2026 Ant Group Co., Ltd.
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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/sessionctx"
)

type CompileTarget int

const (
	TargetAST CompileTarget = iota
	TargetLogicalPlan
	TargetOperatorGraph
	TargetExecutionGraph
	TargetExecutionPlan
)

// Pass represents a single compilation stage in the pipeline
type Pass interface {
	// Name returns the pass name for logging and debugging
	Name() string
	// Run executes the pass logic, returns error if failed
	Run(c *CompileContext) error
}

// CompileContext contains all state and intermediate representation
// throughout the compilation pipeline lifecycle
type CompileContext struct {
	// --- 1. Input ---
	Ctx     context.Context
	Request *v1.CompileSQLRequest

	// --- 2. Environment & Meta ---
	InfoSchema        infoschema.InfoSchema
	SessionCtx        sessionctx.Context
	EnginesInfo       *graph.EnginesInfo
	TensorMetaManager *TensorMetaManager

	// --- 3. Security & Visibility ---
	InvolvedParties           []string
	VisibilityTable           *VisibilityTable
	SecurityRelaxationManager *SecurityRelaxationManager

	// --- 4. Compile Result and Intermediate Representation ---
	AST            ast.StmtNode
	LogicalPlan    core.LogicalPlan
	OperatorGraph  *OperatorGraph
	ExecutionGraph *graph.Graph
	ExecutionPlan  *pb.CompiledPlan
}

// NewCompileContext initializes a new compilation context
func NewCompileContext(ctx context.Context, req *v1.CompileSQLRequest) *CompileContext {
	return &CompileContext{
		Ctx:     ctx,
		Request: req,
	}
}

type CompilationDetails struct {
	AST            ast.StmtNode
	LogicalPlan    core.LogicalPlan
	OperatorGraph  *OperatorGraph
	ExecutionGraph *graph.Graph
	ExecutionPlan  *pb.CompiledPlan

	// for visibility analysis
	VisibilityTable *VisibilityTable
}

// String implements fmt.Stringer interface for CompilationDetails
func (cd *CompilationDetails) String() string {
	var builder strings.Builder

	// AST section
	builder.WriteString("=== AST ===\n")
	if cd.AST != nil {
		builder.WriteString(cd.AST.Text())
	} else {
		builder.WriteString("nil")
	}
	builder.WriteString("\n\n")

	// LogicalPlan section
	builder.WriteString("=== Logical Plan ===\n")
	if cd.LogicalPlan != nil {
		builder.WriteString(core.ToString(cd.LogicalPlan))
	} else {
		builder.WriteString("nil")
	}
	builder.WriteString("\n\n")

	// OperatorGraph section
	builder.WriteString("=== Operator Graph ===\n")
	if cd.OperatorGraph != nil {
		// Use DetailedString if VisibilityTable is available
		if cd.VisibilityTable != nil {
			builder.WriteString(cd.OperatorGraph.DetailedString(cd.VisibilityTable))
		} else {
			builder.WriteString(cd.OperatorGraph.String())
		}
	} else {
		builder.WriteString("nil")
	}
	builder.WriteString("\n\n")

	// ExecutionGraph section
	builder.WriteString("=== Execution Graph ===\n")
	if cd.ExecutionGraph != nil {
		// Basic info
		fmt.Fprintf(&builder, "Node Count: %d\n", cd.ExecutionGraph.NodeCnt)
		fmt.Fprintf(&builder, "Pipeline Count: %d\n", len(cd.ExecutionGraph.Pipelines))
		fmt.Fprintf(&builder, "Output Names: %v\n", cd.ExecutionGraph.OutputNames)
		if cd.ExecutionGraph.PartyInfo != nil {
			fmt.Fprintf(&builder, "Parties: %v\n", cd.ExecutionGraph.PartyInfo.GetParties())
		}
		// Graph visualization (use brief for better readability)
		builder.WriteString("\nGraph Visualization (Graphviz):\n")
		builder.WriteString(cd.ExecutionGraph.DumpBriefGraphviz())
	} else {
		builder.WriteString("nil")
	}
	builder.WriteString("\n\n")

	// ExecutionPlan section
	builder.WriteString("=== Execution Plan ===\n")
	if cd.ExecutionPlan != nil {
		// Use custom formatting for ExecutionPlan
		builder.WriteString(formatExecutionPlan(cd.ExecutionPlan))
	} else {
		builder.WriteString("nil")
	}

	return builder.String()
}

// Compiler is the pipeline-based compiler implementation
type Compiler struct {
	passes []Pass
}

// NewCompiler creates a new compiler with passes based on the target
func NewCompiler(target CompileTarget) *Compiler {
	var passes []Pass

	// Always include ParserPass as it's the foundation
	passes = append(passes, NewParserPass())

	// Add passes based on target
	if target >= TargetLogicalPlan {
		passes = append(passes, NewLogicalPlanPass())
	}
	if target >= TargetOperatorGraph {
		passes = append(passes, NewOperatorGraphPass())
		passes = append(passes, NewVisibilityAnalysisPass())
	}
	if target >= TargetExecutionGraph {
		passes = append(passes, NewExecutionGraphPass())
	}
	if target >= TargetExecutionPlan {
		passes = append(passes, NewCodeGenPass())
	}

	return &Compiler{
		passes: passes,
	}
}

// Compile executes the compilation pipeline
func (c *Compiler) compileInternal(ctx context.Context, req *v1.CompileSQLRequest) (*CompileContext, error) {
	// Initialize compilation context
	compCtx := NewCompileContext(ctx, req)

	// Execute pipeline
	for _, pass := range c.passes {
		logrus.Debugf("Compiling Stage: %s", pass.Name())
		if err := pass.Run(compCtx); err != nil {
			return nil, fmt.Errorf("[%s] failed: %w", pass.Name(), err)
		}
	}

	// Return result
	return compCtx, nil
}

// CompileTo parses SQL and returns AST
func CompileToAST(ctx context.Context, req *v1.CompileSQLRequest) (ast.StmtNode, error) {
	c := NewCompiler(TargetAST)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}
	if compCtx.AST == nil {
		return nil, fmt.Errorf("AST is nil")
	}
	return compCtx.AST, nil
}

// CompileTo parses SQL and builds logical plan
func CompileToLogicalPlan(ctx context.Context, req *v1.CompileSQLRequest) (core.LogicalPlan, error) {
	c := NewCompiler(TargetLogicalPlan)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}
	if compCtx.LogicalPlan == nil {
		return nil, fmt.Errorf("LogicalPlan is nil")
	}
	return compCtx.LogicalPlan, nil
}

// CompileTo parses SQL and builds operator graph
func CompileToOperatorGraph(ctx context.Context, req *v1.CompileSQLRequest) (*OperatorGraph, error) {
	c := NewCompiler(TargetOperatorGraph)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}
	if compCtx.OperatorGraph == nil {
		return nil, fmt.Errorf("OperatorGraph is nil")
	}
	return compCtx.OperatorGraph, nil
}

// CompileTo parses SQL and builds execution graph
func CompileToExecutionGraph(ctx context.Context, req *v1.CompileSQLRequest) (*graph.Graph, error) {
	c := NewCompiler(TargetExecutionGraph)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}
	if compCtx.ExecutionGraph == nil {
		return nil, fmt.Errorf("ExecutionGraph is nil")
	}
	return compCtx.ExecutionGraph, nil
}

// Compile parses SQL and returns complete execution plan (default behavior)
func Compile(ctx context.Context, req *v1.CompileSQLRequest) (*pb.CompiledPlan, error) {
	c := NewCompiler(TargetExecutionPlan)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}
	if compCtx.ExecutionPlan == nil {
		return nil, fmt.Errorf("ExecutionPlan is nil")
	}
	return compCtx.ExecutionPlan, nil
}

// DetailedCompile parses SQL and returns all compilation details including all intermediate representations
func DetailedCompile(ctx context.Context, req *v1.CompileSQLRequest) (*CompilationDetails, error) {
	c := NewCompiler(TargetExecutionPlan)
	compCtx, err := c.compileInternal(ctx, req)
	if err != nil {
		return nil, err
	}

	details := &CompilationDetails{
		AST:             compCtx.AST,
		LogicalPlan:     compCtx.LogicalPlan,
		OperatorGraph:   compCtx.OperatorGraph,
		ExecutionGraph:  compCtx.ExecutionGraph,
		ExecutionPlan:   compCtx.ExecutionPlan,
		VisibilityTable: compCtx.VisibilityTable,
	}
	return details, nil
}

// formatExecutionPlan provides a readable string representation of ExecutionPlan
func formatExecutionPlan(plan *pb.CompiledPlan) string {
	var builder strings.Builder

	// Parties
	if len(plan.Parties) > 0 {
		fmt.Fprintf(&builder, "Parties [%d]:\n", len(plan.Parties))
		for i, party := range plan.Parties {
			fmt.Fprintf(&builder, "  [%d] PartyID: %s\n", i, party.GetCode())
		}
		builder.WriteString("\n")
	}

	// Schema
	if plan.Schema != nil {
		fmt.Fprintf(&builder, "Output Schema [%d columns]:\n", len(plan.Schema.Columns))
		for _, col := range plan.Schema.Columns {
			fmt.Fprintf(&builder, "  - %s: %s\n", col.Name, col.Type)
		}
		builder.WriteString("\n")
	}

	// SubGraphs
	if len(plan.SubGraphs) > 0 {
		fmt.Fprintf(&builder, "SubGraphs [%d]:\n", len(plan.SubGraphs))
		for partyID, subgraph := range plan.SubGraphs {
			if subgraph.SubGraphChecksum != "" {
				fmt.Fprintf(&builder, "\nParty: %s (Checksum: %s)\n", partyID, subgraph.SubGraphChecksum)
			} else {
				fmt.Fprintf(&builder, "\nParty: %s\n", partyID)
			}
			fmt.Fprintf(&builder, "  Nodes [%d]:\n", len(subgraph.Nodes))

			// Sort node keys by node number for ordered output
			type nodeInfo struct {
				key   string
				node  *pb.ExecNode
				index int
			}
			nodes := make([]nodeInfo, 0, len(subgraph.Nodes))
			for nodeKey, node := range subgraph.Nodes {
				// Extract index from node name (format usually like "index_0", "index_1", etc.)
				var index int
				if node.NodeName != "" {
					_, err := fmt.Sscanf(node.NodeName, "%*[^0-9]%d", &index)
					if err != nil {
						// Try different format patterns
						for i, r := range node.NodeName {
							if r >= '0' && r <= '9' {
								_, err := fmt.Sscanf(node.NodeName[i:], "%d", &index)
								if err == nil {
									break
								}
							}
						}
					}
				}
				nodes = append(nodes, nodeInfo{key: nodeKey, node: node, index: index})
			}

			// Sort by index
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].index < nodes[j].index
			})

			// Print sorted nodes
			for _, nodeInfo := range nodes {
				fmt.Fprintf(&builder, "    %s: %s\n", nodeInfo.key, nodeInfo.node.OpType)
				// Show input tensors count
				if len(nodeInfo.node.Inputs) > 0 {
					fmt.Fprintf(&builder, "      Inputs: %d\n", len(nodeInfo.node.Inputs))
				}
				// Show output tensors count
				if len(nodeInfo.node.Outputs) > 0 {
					fmt.Fprintf(&builder, "      Outputs: %d\n", len(nodeInfo.node.Outputs))
				}
			}
		}
	}

	// SPU Runtime Config
	if plan.SpuRuntimeConf != nil {
		fmt.Fprintf(&builder, "\nSPU Runtime Config:\n")
		fmt.Fprintf(&builder, "  Protocol: %s\n", plan.SpuRuntimeConf.Protocol.String())
		fmt.Fprintf(&builder, "  Field: %s\n", plan.SpuRuntimeConf.Field.String())
	}

	// Whole Graph Checksum
	if plan.WholeGraphChecksum != "" {
		fmt.Fprintf(&builder, "\nWhole Graph Checksum: %s\n", plan.WholeGraphChecksum)
	}

	return builder.String()
}
