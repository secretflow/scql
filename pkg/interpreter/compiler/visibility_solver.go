// Copyright 2025 Ant Group Co., Ltd.
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
	"fmt"

	"github.com/secretflow/scql/pkg/interpreter/compiler/util"
)

type VisibilitySolver struct {
	vt                 *VisibilityTable // keep track of tensor visibility
	srm                *SecurityRelaxationManager
	originalVisibility map[string]*VisibleParties // original visibility for tensor get from data source
	specifiedVis       map[int]*VisibleParties    // tensor visibility specified by user
	producerTracker    *TensorProducerTracker
	consumerTracker    *TensorConsumerTracker
	reverseInfer       bool // whether to enable reverse inference
}

func NewVisibilitySolver(vt *VisibilityTable, srm *SecurityRelaxationManager, origVis map[string]*VisibleParties, specifiedVis map[int]*VisibleParties, reverseInfer bool) *VisibilitySolver {
	return &VisibilitySolver{
		vt:                 vt,
		srm:                srm,
		originalVisibility: origVis,
		specifiedVis:       specifiedVis,
		reverseInfer:       reverseInfer,
	}
}

func (vs *VisibilitySolver) Solve(operatorGraph *OperatorGraph) error {
	vs.producerTracker = operatorGraph.producerTracker
	vs.consumerTracker = operatorGraph.consumerTracker

	if err := vs.revealResult(operatorGraph); err != nil {
		return err
	}

	nodeNum := len(operatorGraph.operators)

	// Create priority queues with priority functions
	pqf := util.NewPriorityQueue(func(node Operator) int {
		return nodeNum - node.ID()
	})
	pqr := util.NewPriorityQueue(func(node Operator) int {
		return node.ID()
	})

	for _, node := range operatorGraph.operators[:len(operatorGraph.operators)-1] {
		pqf.Enqueue(node)
	}

	for pqf.Len() > 0 || pqr.Len() > 0 {
		if pqr.Len() > 0 {
			// Always do reverse inference first rather than forward inference when pqr is not empty
			node, _ := pqr.Dequeue()
			updatedTensors, err := node.ReverseInfer(vs)
			if err != nil {
				return err
			}
			for _, tensor := range updatedTensors {
				sourceNode, err := operatorGraph.producerTracker.GetProducer(tensor)
				if err != nil {
					return err
				}
				pqr.Enqueue(sourceNode)
			}

			// forward infer again
			// We can skip this step for most operators because updatedTensors will be empty for them
			// But this requires us to carefully inspect the reverse inference logic for each operator
			// Furthermore, the reverse inference logic maybe updated in the future.
			// So just always Infer again to make life easier.
			updatedTensors, err = node.Infer(vs, false)
			if err != nil {
				return err
			}
			// Forward infer again if input tensor's visibility updated
			if len(updatedTensors) > 0 {
				pqf.Enqueue(node)
			}
		} else {
			node, _ := pqf.Dequeue()
			updatedTensors, err := node.Infer(vs, true)
			if err != nil {
				return err
			}
			for _, tensor := range updatedTensors {
				userNodes := operatorGraph.consumerTracker.GetConsumers(tensor)
				for _, userNode := range userNodes {
					pqf.Enqueue(userNode)
				}
			}

			// if tensor's visibility updated not because of the forward inference,
			// then we need to perform reverse inference on that tensor's source node
			updated := false
			for _, tensor := range GetNodeOutputs(node) {
				if vis, ok := vs.specifiedVis[tensor.ID]; ok {
					updated = vs.vt.TryUpdateVisibility(tensor, vis) || updated
				}
			}
			if updated && vs.reverseInfer {
				pqr.Enqueue(node)
			}
		}
	}

	return nil
}

func (vs *VisibilitySolver) revealResult(operatorGraph *OperatorGraph) error {
	if len(operatorGraph.operators) == 0 {
		return fmt.Errorf("revealResult: empty operator graph")
	}

	resultNode, ok := operatorGraph.operators[len(operatorGraph.operators)-1].(*OperatorResult)
	if !ok {
		return fmt.Errorf("revealResult: expect result node, but got %T", operatorGraph.operators[len(operatorGraph.operators)-1])
	}

	if resultNode.intoOpt == nil {
		for _, tensor := range GetNodeInputs(resultNode) {
			if vs.reverseInfer {
				vs.specifiedVis[tensor.ID] = NewVisibleParties([]string{resultNode.issuerPartyCode})
			} else {
				vs.vt.UpdateVisibility(tensor, NewVisibleParties([]string{resultNode.issuerPartyCode}))
			}
		}
		return nil
	}

	for _, partyFile := range resultNode.intoOpt.Opt.PartyFiles {
		visIncreament := NewVisibleParties([]string{partyFile.PartyCode})
		if len(partyFile.FieldList) == 0 {
			for _, tensor := range GetNodeInputs(resultNode) {
				if vs.reverseInfer {
					vs.vt.UpdateVisibility(tensor, visIncreament)
					vs.specifiedVis[tensor.ID] = vs.vt.TensorVisibleParties(tensor)
				} else {
					vs.vt.UpdateVisibility(tensor, visIncreament)
				}
			}
		} else {
			for idx, col := range resultNode.intoOpt.PartyColumns[partyFile.PartyCode] {
				tensor, ok := resultNode.resultTable[col.UniqueID]
				if !ok {
					return fmt.Errorf("revealResult: result tensor %d not found", idx)
				}
				if vs.reverseInfer {
					vs.specifiedVis[tensor.ID] = visIncreament
				} else {
					vs.vt.UpdateVisibility(tensor, visIncreament)
				}
			}
		}
	}
	return nil
}

func (vs *VisibilitySolver) handleRunSQL(n *OperatorRunSQL) error {
	for _, node := range n.subGraphNodes {
		_, err := node.Infer(vs, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (vs *VisibilitySolver) NewOverlayVisibilityTable() *OverlayVisibilityTable {
	return &OverlayVisibilityTable{
		overlayVisibility: make(map[int]*VisibleParties),
		baseTable:         vs.vt,
	}
}

func (vs *VisibilitySolver) outputVisSnapshot(node Operator) map[int]*VisibleParties {
	snapshot := make(map[int]*VisibleParties)
	for _, tensor := range GetNodeOutputs(node) {
		snapshot[tensor.ID] = vs.vt.TensorVisibleParties(tensor).Clone()
	}
	return snapshot
}

func (vs *VisibilitySolver) getUpdatedTensors(node Operator, snapshot map[int]*VisibleParties) []*TensorMeta {
	updatedTensors := make([]*TensorMeta, 0)
	for _, tensor := range GetNodeOutputs(node) {
		originVis, ok := snapshot[tensor.ID]
		if !ok {
			updatedTensors = append(updatedTensors, tensor)
			continue
		}
		updatedVis := vs.vt.TensorVisibleParties(tensor)
		if !originVis.Covers(updatedVis) {
			updatedTensors = append(updatedTensors, tensor)
		}
	}
	return updatedTensors
}
