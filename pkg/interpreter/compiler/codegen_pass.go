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
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
)

// CodeGenPass generates final executable plan from execution graph
type CodeGenPass struct{}

// NewCodeGenPass creates a new codegen pass
func NewCodeGenPass() *CodeGenPass {
	return &CodeGenPass{}
}

// Name returns the pass name
func (p *CodeGenPass) Name() string {
	return "CodeGenPass"
}

// Run generates final executable plan
func (p *CodeGenPass) Run(c *CompileContext) error {
	// Ensure session context is available (should be created in LogicalPlanPass)
	if c.SessionCtx == nil {
		return fmt.Errorf("session context not found - ensure LogicalPlanPass runs before CodeGenPass")
	}

	// Process execution graph to generate execution plan
	executionPlan, err := processExecutionGraph(
		c.ExecutionGraph,
		c.Request.GetCompileOpts().GetSpuConf(),
		true,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to process execution graph: %v", err)
	}

	// Add warning info when affected by groupby threshold
	executionPlan.Warning = &pb.Warning{
		MayAffectedByGroupThreshold: c.SessionCtx.GetSessionVars().AffectedByGroupThreshold,
	}
	logrus.Debugf("execution plan: %s", executionPlan)

	c.ExecutionPlan = executionPlan

	return nil
}

// buildCompiledPlan builds a CompiledPlan from execution graph and execution plans
func buildCompiledPlan(spuConf *spu.RuntimeConfig, eGraph *graph.Graph, execPlans map[string]*graph.ExecutionPlan, enableChecksum bool) *pb.CompiledPlan {
	plan := &pb.CompiledPlan{
		Schema:         &pb.TableSchema{},
		SpuRuntimeConf: spuConf,
		SubGraphs:      make(map[string]*pb.SubGraph),
	}

	// Fill Schema
	for _, out := range eGraph.OutputNames {
		plan.Schema.Columns = append(plan.Schema.Columns, &pb.ColumnDesc{
			Name: out,
			// TODO: populate Field Type
			// Type: <column data type>
		})
	}

	// Fill Parties
	for _, party := range eGraph.GetParties() {
		plan.Parties = append(plan.Parties, &pb.PartyId{
			Code: party,
		})
	}

	// Fill Subgraphs
	for party, subGraph := range execPlans {
		graphProto := &pb.SubGraph{
			Nodes: make(map[string]*pb.ExecNode),
			Policy: &pb.SchedulingPolicy{
				WorkerNum: int32(subGraph.Policy.WorkerNumber),
			},
		}
		// Fill Nodes
		for k, v := range subGraph.Nodes {
			graphProto.Nodes[strconv.Itoa(k)] = v.ToProto()
		}
		// Fill Policy subdags
		for _, pipelineJobs := range subGraph.Policy.PipelineJobs {
			pipeline := &pb.Pipeline{}
			for _, job := range pipelineJobs.Jobs {
				subdag := &pb.SubDAG{
					Jobs:                     make([]*pb.SubDAG_Job, 0),
					NeedCallBarrierAfterJobs: job.NeedCallBarrierAfterJobs,
				}
				for k, v := range job.Jobs {
					var ids []string
					for _, id := range v {
						ids = append(ids, strconv.Itoa(id))
					}

					j := &pb.SubDAG_Job{
						WorkerId: int32(k),
						NodeIds:  ids,
					}
					subdag.Jobs = append(subdag.Jobs, j)
				}
				pipeline.Subdags = append(pipeline.Subdags, subdag)
			}
			if pipelineJobs.Batched {
				pipeline.Batched = true
				for _, t := range pipelineJobs.InputTensors {
					pipeline.Inputs = append(pipeline.Inputs, t.ToProto())
				}
				for _, t := range pipelineJobs.OutputTensors {
					pipeline.Outputs = append(pipeline.Outputs, t.ToProto())
				}
			}
			graphProto.Policy.Pipelines = append(graphProto.Policy.Pipelines, pipeline)
		}

		plan.SubGraphs[party] = graphProto
	}

	// Calculate checksums if enabled
	if enableChecksum {
		// Calculate whole graph checksum
		graphvizOutput := eGraph.DumpGraphviz()
		tableSchemaCrypt := sha256.New()
		tableSchemaCrypt.Write([]byte(graphvizOutput))
		plan.WholeGraphChecksum = fmt.Sprintf("%x", tableSchemaCrypt.Sum(nil))

		// Calculate checksum for each party
		for _, graphProto := range plan.SubGraphs {
			tableSchemaCrypt := sha256.New()
			subGraphStr := graphProto.String()
			tableSchemaCrypt.Write([]byte(subGraphStr))
			graphProto.SubGraphChecksum = fmt.Sprintf("%x", tableSchemaCrypt.Sum(nil))
		}
	}

	return plan
}

// processExecutionGraph process the execution graph and build the compiled plan
func processExecutionGraph(ep *graph.Graph, spuConf *spu.RuntimeConfig, dumpExeGraph bool, warningInfo *pb.Warning) (*pb.CompiledPlan, error) {
	// Step 1: Partition the execution graph
	partitioner := graph.NewGraphPartitioner(ep)
	if err := partitioner.NaivePartition(); err != nil {
		return nil, fmt.Errorf("graph partitioning failed: %v", err)
	}

	// Step 2: Map the execution graph
	mapper := graph.NewGraphMapper(ep, partitioner.Pipelines)
	mapper.Map()

	// Step 3: Build the compiled plan
	plan := buildCompiledPlan(spuConf, ep, mapper.Codes, false)

	// Step 4: Handle engine execution graph dumping and checksum calculation
	// TODO add related option in additional info
	graphvizOutput := ep.DumpGraphviz()
	if dumpExeGraph {
		plan.Explain = &pb.ExplainInfo{
			ExeGraphDot: graphvizOutput,
		}
	}

	// Step 5: Calculate checksum
	tableSchemaCrypt := sha256.New()
	tableSchemaCrypt.Write([]byte(graphvizOutput))
	plan.WholeGraphChecksum = fmt.Sprintf("%x", tableSchemaCrypt.Sum(nil))

	// Step 6: Add warning information if provided
	if warningInfo != nil {
		plan.Warning = warningInfo
	}

	return plan, nil
}
