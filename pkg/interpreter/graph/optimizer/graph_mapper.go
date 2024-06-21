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

type SchedulePolicy struct {
	WorkerNumber int
	Jobs         []*ParallelJobs
}

type ExecutionPlan struct {
	Nodes  map[int]*graph.ExecutionNode
	Policy *SchedulePolicy
}

func (b *ExecutionPlan) DumpString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "SchedulePolicy: {\n")

	for i, job := range b.Policy.Jobs {
		fmt.Fprintf(&builder, "  SubDAG %d {\n", i)
		var keys []int
		for k := range job.Jobs {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			fmt.Fprintf(&builder, "    Worker %d, nodes: %v\n", k, job.Jobs[k])
		}
		fmt.Fprintf(&builder, "    CallBarrierAfterJobs: %v\n", job.NeedCallBarrierAfterJobs)
		fmt.Fprintf(&builder, "    SyncSymbolBeforeJobs: %v\n", job.NeedSyncSymbolBeforeJobs)
		fmt.Fprintf(&builder, "  }\n")
	}
	fmt.Fprintf(&builder, "}")
	return builder.String()
}

type GraphMapper struct {
	graph   *graph.Graph
	subDAGs []*SubDAG

	syncedTensorMap map[string]bool

	Codes map[string]*ExecutionPlan // key is party code, value is execution plan
}

func NewGraphMapper(input *graph.Graph, subDAGs []*SubDAG) *GraphMapper {
	return &GraphMapper{
		graph:           input,
		subDAGs:         subDAGs,
		Codes:           make(map[string]*ExecutionPlan),
		syncedTensorMap: make(map[string]bool),
	}
}

func (m *GraphMapper) InferenceWorkerNumber() int {
	workerNum := len(m.subDAGs[0].Nodes)
	for _, subDAG := range m.subDAGs {
		if len(subDAG.Nodes) > workerNum {
			workerNum = len(subDAG.Nodes)
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

func (m *GraphMapper) inferenceNeedCallBarrier(i int) {
	var partyCodes []string
	for k, v := range m.Codes {
		if len(v.Policy.Jobs[i].Jobs) != 0 {
			partyCodes = append(partyCodes, k)
		}
	}
	if len(partyCodes) > 1 {
		for _, v := range m.Codes {
			v.Policy.Jobs[i].NeedCallBarrierAfterJobs = true
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

func (m *GraphMapper) inferenceNeedSyncSymbol(i int) {
	// check need sync before jobs
	for _, v := range m.Codes {
		jobs := v.Policy.Jobs[i].Jobs
		for _, ids := range jobs {
			for _, id := range ids {
				if needSyncSymbol(v.Nodes[id]) && !m.IsInputsSynced(v.Nodes[id]) {
					v.Policy.Jobs[i].NeedSyncSymbolBeforeJobs = true
				}
			}
		}
	}
	// if synced in this subdag, set current SyncedTensorMap all true
	for _, v := range m.Codes {
		if v.Policy.Jobs[i].NeedSyncSymbolBeforeJobs {
			// set current to true
			for key := range m.syncedTensorMap {
				m.syncedTensorMap[key] = true
			}
		}
		break
	}
	// put outputs in this subdag to SyncedTensorMap
	for _, v := range m.Codes {
		jobs := v.Policy.Jobs[i].Jobs
		for _, ids := range jobs {
			for _, id := range ids {
				// update SyncedTensorMap with outputs
				m.UpdateSyncedTensorMap(v.Nodes[id].Outputs)
			}
		}
	}
}

func (m *GraphMapper) Map() {
	for j, subDAG := range m.subDAGs {
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
					m.Codes[k] = &ExecutionPlan{
						Nodes: make(map[int]*graph.ExecutionNode),
						Policy: &SchedulePolicy{
							WorkerNumber: m.InferenceWorkerNumber(),
							Jobs:         make([]*ParallelJobs, 0),
						},
					}
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
				m.Codes[k] = &ExecutionPlan{
					Nodes: make(map[int]*graph.ExecutionNode),
					Policy: &SchedulePolicy{
						WorkerNumber: m.InferenceWorkerNumber(),
						Jobs:         make([]*ParallelJobs, 0),
					},
				}
			}
			m.Codes[k].Policy.Jobs = append(m.Codes[k].Policy.Jobs, v)
		}
		m.inferenceNeedCallBarrier(j)
		m.inferenceNeedSyncSymbol(j)
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
					Subdags:   make([]*scql.SubDAG, 0),
				},
			},
		}
		pb.JobParams.PartyCode = partyCode
		for k, v := range plan.Nodes {
			pb.Graph.Nodes[strconv.Itoa(k)] = v.ToProto()
		}
		for _, job := range plan.Policy.Jobs {
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
			pb.Graph.Policy.Subdags = append(pb.Graph.Policy.Subdags, subdag)
		}
		result[partyCode] = pb
	}
	return result
}
