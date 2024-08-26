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
	"strconv"
	"strings"

	proto "google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/interpreter/graph"

	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

const workerNumLevel1 = 8
const workerNumLevel2 = 16

type ParallelJobs struct {
	// map from worker id to node ids
	Jobs map[int][]int
	// sync among parties. Sometimes, the DAGs of Alice and Bob
	// are unbalanced. There is no need to call sync parties when
	// only one party execute its own subDAG.
	NeedCallBarrierAfterJobs bool
	NeedSyncSymbolBeforeJobs bool
}

type PipelineJobs struct {
	Batched       bool
	InputTensors  []*graph.Tensor
	OutputTensors []*graph.Tensor
	Jobs          []*ParallelJobs
}

type SchedulePolicy struct {
	WorkerNumber int
	PipelineJobs []*PipelineJobs
}

type ExecutionPlan struct {
	Nodes  map[int]*graph.ExecutionNode
	Policy *SchedulePolicy
}

func NewExecutionPlan(workerNum, pipelinesNum int) *ExecutionPlan {
	plan := &ExecutionPlan{
		Nodes: make(map[int]*graph.ExecutionNode),
		Policy: &SchedulePolicy{
			WorkerNumber: workerNum,
			PipelineJobs: make([]*PipelineJobs, pipelinesNum),
		},
	}
	for i := 0; i < pipelinesNum; i++ {
		plan.Policy.PipelineJobs[i] = &PipelineJobs{}
	}
	return plan
}

func (b *ExecutionPlan) DumpString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "SchedulePolicy: {\n")
	for _, p := range b.Policy.PipelineJobs {
		fmt.Fprintf(&builder, "  Pipeline {\n")
		for _, j := range p.Jobs {
			fmt.Fprintf(&builder, "    SubDAG {\n")
			var keys []int
			for k := range j.Jobs {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, k := range keys {
				fmt.Fprintf(&builder, "      Worker %d, nodes: %v\n", k, j.Jobs[k])
			}
			fmt.Fprintf(&builder, "      CallBarrierAfterJobs: %v\n", j.NeedCallBarrierAfterJobs)
			fmt.Fprintf(&builder, "      SyncSymbolBeforeJobs: %v\n", j.NeedSyncSymbolBeforeJobs)
			fmt.Fprintf(&builder, "    }\n")
		}
		fmt.Fprintf(&builder, "  }\n")
	}
	fmt.Fprintf(&builder, "}")
	return builder.String()
}

type GraphMapper struct {
	graph     *graph.Graph
	pipelines []*Pipeline

	syncedTensorMap map[string]bool

	Codes map[string]*ExecutionPlan // key is party code, value is execution plan
}

func NewGraphMapper(input *graph.Graph, pipelines []*Pipeline) *GraphMapper {
	return &GraphMapper{
		graph:           input,
		pipelines:       pipelines,
		Codes:           make(map[string]*ExecutionPlan),
		syncedTensorMap: make(map[string]bool),
	}
}

func (m *GraphMapper) InferenceWorkerNumber() int {
	workerNum := 1
	for _, pipeline := range m.pipelines {
		for _, subDAG := range pipeline.SubDAGs {
			if len(subDAG.Nodes) > workerNum {
				workerNum = len(subDAG.Nodes)
			}
		}
	}
	if workerNum <= workerNumLevel1 {
		return workerNum
	}
	if workerNum > workerNumLevel2 {
		return workerNumLevel1
	}
	return workerNum / 2
}

func (m *GraphMapper) inferenceNeedCallBarrier(pipelineIndex, jobIndex int) {
	var partyCodes []string
	for k, v := range m.Codes {
		if len(v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].Jobs) != 0 {
			partyCodes = append(partyCodes, k)
		}

	}
	if len(partyCodes) > 1 {
		for _, v := range m.Codes {
			v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].NeedCallBarrierAfterJobs = true
		}
	}
}

var needSyncSymbolOps = map[string]bool{
	operator.OpNameReplicate: true,
	operator.OpNameMakeShare: true,
}

func needSyncSymbol(node *graph.ExecutionNode) bool {
	_, ok := needSyncSymbolOps[node.OpType]
	return ok
}

func (m *GraphMapper) IsInputsSynced(node *graph.ExecutionNode) bool {
	for _, tensors := range node.Inputs {
		for _, tensor := range tensors {
			if !m.syncedTensorMap[tensor.UniqueName()] {
				return false
			}
		}
	}
	return true
}

// SyncedTensorMap keep info which tensors have been synced, this func update SyncedTensorMap
// after sync op
func (m *GraphMapper) UpdateSyncedTensorMap(inputs map[string][]*graph.Tensor) {
	for _, tensors := range inputs {
		for _, tensor := range tensors {
			if ok := m.syncedTensorMap[tensor.UniqueName()]; !ok {
				m.syncedTensorMap[tensor.UniqueName()] = false
			}
		}
	}
}

func (m *GraphMapper) inferenceNeedSyncSymbol(pipelineIndex, jobIndex int) {
	for _, v := range m.Codes {
		jobs := v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].Jobs
		for _, ids := range jobs {
			for _, id := range ids {
				if needSyncSymbol(v.Nodes[id]) && !m.IsInputsSynced(v.Nodes[id]) {
					v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].NeedSyncSymbolBeforeJobs = true
				}
			}
		}
	}
	// if synced in this subdag, set current SyncedTensorMap all true
	for _, v := range m.Codes {
		if v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].NeedSyncSymbolBeforeJobs {
			// set current to true
			for key := range m.syncedTensorMap {
				m.syncedTensorMap[key] = true
			}
		}
		break

	}
	// put outputs in this subdag to SyncedTensorMap
	for _, v := range m.Codes {
		jobs := v.Policy.PipelineJobs[pipelineIndex].Jobs[jobIndex].Jobs
		for _, ids := range jobs {
			for _, id := range ids {
				// update SyncedTensorMap with outputs
				m.UpdateSyncedTensorMap(v.Nodes[id].Outputs)
			}
		}

	}
}

func (m *GraphMapper) Map() {
	for i, pipeline := range m.pipelines {
		for j, subDAG := range pipeline.SubDAGs {
			partyCodeToParallelJobs := make(map[string]*ParallelJobs)
			for _, partyCode := range m.graph.PartyInfo.GetParties() {
				partyCodeToParallelJobs[partyCode] = &ParallelJobs{
					Jobs: make(map[int][]int),
				}
			}

			sortedNodes := make([]*graph.ExecutionNode, 0)
			singlePartyNodes := make([]*graph.ExecutionNode, 0)
			for node := range subDAG.Nodes {
				if len(node.Parties) == 1 {
					singlePartyNodes = append(singlePartyNodes, node)
				} else {
					sortedNodes = append(sortedNodes, node)
				}
			}
			sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i].ID < sortedNodes[j].ID })
			sort.Slice(singlePartyNodes, func(i, j int) bool { return singlePartyNodes[i].ID < singlePartyNodes[j].ID })
			sortedNodes = append(sortedNodes, singlePartyNodes...)

			for i, node := range sortedNodes {
				nodes := splitExecutionNode(node)
				for k, v := range nodes {
					_, ok := m.Codes[k]
					if !ok {
						m.Codes[k] = NewExecutionPlan(m.InferenceWorkerNumber(), len(m.pipelines))
					}
					m.Codes[k].Nodes[v.ID] = v
					workerID := i % m.Codes[k].Policy.WorkerNumber
					_, ok = partyCodeToParallelJobs[k].Jobs[workerID]
					if !ok {
						partyCodeToParallelJobs[k].Jobs[workerID] = make([]int, 0)
					}
					partyCodeToParallelJobs[k].Jobs[workerID] = append(partyCodeToParallelJobs[k].Jobs[workerID], v.ID)
				}
			}
			for k, v := range partyCodeToParallelJobs {
				_, ok := m.Codes[k]
				if !ok {
					m.Codes[k] = NewExecutionPlan(m.InferenceWorkerNumber(), len(m.pipelines))
				}
				m.Codes[k].Policy.PipelineJobs[i].Jobs = append(m.Codes[k].Policy.PipelineJobs[i].Jobs, v)
			}
			for _, code := range m.Codes {
				code.Policy.PipelineJobs[i].Batched = pipeline.Batched
			}
			m.inferenceNeedCallBarrier(i, j)
			m.inferenceNeedSyncSymbol(i, j)
		}
		if pipeline.Batched {
			// TODO: support share tensors
			for _, t := range pipeline.InputTensors {
				m.Codes[t.OwnerPartyCode].Policy.PipelineJobs[i].InputTensors = append(m.Codes[t.OwnerPartyCode].Policy.PipelineJobs[i].InputTensors, t)
			}
			for _, t := range pipeline.OutputTensors {
				m.Codes[t.OwnerPartyCode].Policy.PipelineJobs[i].OutputTensors = append(m.Codes[t.OwnerPartyCode].Policy.PipelineJobs[i].OutputTensors, t)
			}
		}

	}
}

func (m *GraphMapper) CodeGen(params *scql.JobStartParams) map[string]*scql.RunExecutionPlanRequest {
	result := make(map[string]*scql.RunExecutionPlanRequest)
	for partyCode, plan := range m.Codes {
		jobStartParams, ok := proto.Clone(params).(*scql.JobStartParams)
		if !ok {
			return nil
		}
		pb := &scql.RunExecutionPlanRequest{
			JobParams: jobStartParams,
			Graph: &scql.SubGraph{
				Nodes: make(map[string]*scql.ExecNode),
				Policy: &scql.SchedulingPolicy{
					WorkerNum: int32(plan.Policy.WorkerNumber),
				},
			},
		}
		pb.JobParams.PartyCode = partyCode
		for k, v := range plan.Nodes {
			pb.Graph.Nodes[strconv.Itoa(k)] = v.ToProto()
		}
		for _, pipelineJobs := range plan.Policy.PipelineJobs {
			pipeline := scql.Pipeline{Subdags: make([]*scql.SubDAG, 0)}
			for _, job := range pipelineJobs.Jobs {
				subdag := &scql.SubDAG{
					Jobs:                     make([]*scql.SubDAG_Job, 0),
					NeedCallBarrierAfterJobs: job.NeedCallBarrierAfterJobs,
				}
				for k, v := range job.Jobs {
					var ids []string
					for _, id := range v {
						ids = append(ids, strconv.Itoa(id))
					}
					j := &scql.SubDAG_Job{
						WorkerId: int32(k),
						NodeIds:  ids,
					}
					subdag.Jobs = append(subdag.Jobs, j)
				}
				pipeline.Subdags = append(pipeline.Subdags, subdag)
			}
			pb.Graph.Policy.Pipelines = append(pb.Graph.Policy.Pipelines, &pipeline)
		}
		result[partyCode] = pb
	}
	return result
}
