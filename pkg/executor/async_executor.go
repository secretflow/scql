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

package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/secretflow/scql/pkg/interpreter/optimizer"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	enginePb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// sub dags splitted by party
type splittedSubDag struct {
	partySubDAGs      map[string]*optimizer.PartySubDAG
	partySubDAGStatus map[string]bool
}

func NewSplittedSubDag() *splittedSubDag {
	return &splittedSubDag{
		partySubDAGs:      make(map[string]*optimizer.PartySubDAG),
		partySubDAGStatus: make(map[string]bool),
	}
}

type AsyncExecutor struct {
	SubDAGs             []*optimizer.SubDAG
	OutputNames         []string
	Stub                *EngineStub
	m                   sync.Mutex
	splittedSubDags     map[int]*splittedSubDag
	successCount        int
	intermediateResults []*enginePb.ReportRequest
}

func NewAsyncExecutor(subDAGs []*optimizer.SubDAG, outputNames []string, stub *EngineStub) (*AsyncExecutor, error) {
	e := &AsyncExecutor{
		SubDAGs:         subDAGs,
		OutputNames:     outputNames,
		Stub:            stub,
		splittedSubDags: make(map[int]*splittedSubDag),
	}

	for i, subDAG := range e.SubDAGs {
		splittedSubDag := NewSplittedSubDag()
		partySubDAGs, err := optimizer.Split(subDAG)
		if err != nil {
			return nil, err
		}
		for code, partySubDAG := range partySubDAGs {
			splittedSubDag.partySubDAGs[code] = partySubDAG
			splittedSubDag.partySubDAGStatus[code] = false
		}
		e.splittedSubDags[i] = splittedSubDag
	}
	return e, nil
}

// RunFirst executes first runnable node.
func (e *AsyncExecutor) RunFirst(ctx context.Context) error {
	return e.Stub.RunSubDAG(ctx, e.splittedSubDags[0].partySubDAGs, 0)
}

// check all party status
func (e *AsyncExecutor) checkSubDAGStatus(id int) bool {
	for _, status := range e.splittedSubDags[id].partySubDAGStatus {
		if !status {
			return false
		}
	}
	return true
}

// CanRunNext check if current one's running is complete
// Returns nextSubDagID < 0 if we can not run next, and isEnd if all subdags have finished
func (e *AsyncExecutor) CanRunNext(ctx context.Context, req *enginePb.ReportRequest) ( /*nextSubDagID*/ int /*isEnd*/, bool, error) {
	// NOTE(yang.y): CanRunNext may be called concurrently. For example, sequential
	// executor dispatches MakeShareOp to both Alice and Bob. Their callbacks
	// may both enter the following critical section at the same time.
	e.m.Lock()
	defer e.m.Unlock()

	e.intermediateResults = append(e.intermediateResults, req)

	id := int(req.DagId)
	if _, ok := e.splittedSubDags[id]; !ok {
		return 0, false, fmt.Errorf("callback subDAG ID not in the execution plan")
	}

	e.splittedSubDags[id].partySubDAGStatus[req.PartyCode] = true
	if !e.checkSubDAGStatus(id) {
		// Running next node can only be triggered when all parties finish running the node.
		return -1, false, nil
	}
	e.successCount++
	if e.successCount == len(e.splittedSubDags) {
		return e.successCount, true, nil
	}
	return e.successCount, false, nil
}

// RunNext executes next node if current one's running is complete.
// Returns a flag indicates whether the end of an execution plan is reached and an error string if any.
func (e *AsyncExecutor) RunNext(ctx context.Context, req *enginePb.ReportRequest) error {
	// NOTE(yang.y): RunNext may be called concurrently. For example, sequential
	// executor dispatches MakeShareOp to both Alice and Bob. Their callbacks
	// may both enter the following critical section at the same time.
	e.m.Lock()
	subDagMap := e.splittedSubDags[e.successCount].partySubDAGs
	nextSubDagId := e.successCount
	e.m.Unlock()

	return e.Stub.RunSubDAG(ctx, subDagMap, nextSubDagId)
}

func (e *AsyncExecutor) MergeQueryResults() (*enginePb.SCDBQueryResultResponse, error) {
	var affectedRows int64 = 0
	outCols := []*enginePb.Tensor{}
	for _, req := range e.intermediateResults {
		if req.GetNumRowsAffected() != 0 {
			if affectedRows == 0 {
				affectedRows = req.GetNumRowsAffected()
			} else if affectedRows != req.GetNumRowsAffected() {
				return nil, fmt.Errorf("affected rows not matched, received affectedRows=%v, req.NumRowsAffected=%v", affectedRows, req.GetNumRowsAffected())
			}
		}
		for _, col := range req.GetOutColumns() {
			if _, err := find(e.OutputNames, col.GetName()); err == nil {
				outCols = append(outCols, col)
			}
		}
	}

	outCols, err := sortTensorByOutputName(outCols, e.OutputNames)
	if err != nil {
		return nil, err
	}

	// NOTE: Remove ID field in Tensor.Name. e.g. f1.12 => f1
	for _, c := range outCols {
		c.Name = translator.TensorNameFromUniqueName(c.Name)
	}

	return &enginePb.SCDBQueryResultResponse{
		Status:        e.intermediateResults[0].Status,
		OutColumns:    outCols,
		ScdbSessionId: e.intermediateResults[0].GetSessionId(),
		AffectedRows:  affectedRows,
	}, nil
}

func find(ss []string, s string) (int, error) {
	for i, e := range ss {
		if e == s {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unable to find name %v in %v", s, ss)
}

func sortTensorByOutputName(tensors []*enginePb.Tensor, outputName []string) ([]*enginePb.Tensor, error) {
	result := []*enginePb.Tensor{}
	for _, name := range outputName {
		ts := findTensorListByName(tensors, name)
		if len(ts) == 0 {
			return nil, fmt.Errorf("sortTensorByOutputName: unable to find tensor %v", name)
		}
		if len(ts) > 1 {
			if err := mergeStringTensorsToFirstTensor(ts); err != nil {
				return nil, fmt.Errorf("sortTensorByOutputName: unable to find tensor %v", name)
			}
		}
		result = append(result, ts[0])
	}
	return result, nil
}
