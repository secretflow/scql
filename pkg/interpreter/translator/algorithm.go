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

package translator

import (
	"fmt"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

type materializedAlgorithm struct {
	cost            algCost
	inputPlacement  map[string][]placement
	outputPlacement map[string][]placement
}

type algCost struct {
	communicationCost int
	calculationCost   int
}

func newAlgCost(commCost, calCost int) algCost {
	return algCost{
		communicationCost: commCost,
		calculationCost:   calCost,
	}
}

func (c *algCost) calculateTotalCost() int {
	// TODO(xiaoyuan) fix cost calculation later, maybe weighting sum here
	return c.communicationCost + c.calculationCost
}

func (c *algCost) addCost(cost algCost) {
	c.calculationCost += cost.calculationCost
	c.communicationCost += cost.communicationCost
}

type statusConstraint interface {
	status() scql.TensorStatus
}

type placement interface {
	status() scql.TensorStatus
	partyList() []string
	toString() string
}

type privatePlacement struct {
	partyCode string
}

func (p *privatePlacement) status() scql.TensorStatus {
	return scql.TensorStatus_TENSORSTATUS_PRIVATE
}

func (p *privatePlacement) partyList() []string {
	return []string{p.partyCode}
}

func (p *privatePlacement) toString() string {
	return fmt.Sprintf("%d-%v", p.status(), p.partyList())
}

type publicPlacement struct {
	partyCodes []string
}

func (p *publicPlacement) status() scql.TensorStatus {
	return scql.TensorStatus_TENSORSTATUS_PUBLIC
}

func (p *publicPlacement) partyList() []string {
	return p.partyCodes
}

func (p *publicPlacement) toString() string {
	return fmt.Sprintf("%d-%v", p.status(), p.partyList())
}

type sharePlacement struct {
	partyCodes []string
}

func (p *sharePlacement) status() scql.TensorStatus {
	return scql.TensorStatus_TENSORSTATUS_SECRET
}

func (p *sharePlacement) partyList() []string {
	return p.partyCodes
}

func (p *sharePlacement) toString() string {
	return fmt.Sprintf("%d-%v", p.status(), p.partyList())
}

type algCreateFunc func(in map[string][]*ccl.CCL, out map[string][]*ccl.CCL, allParties []string) ([]*materializedAlgorithm, error)

type algCreator struct {
	// op name -> algorithm create function
	creators map[string]algCreateFunc
}

func NewAlgCreator() *algCreator {
	creator := &algCreator{
		creators: map[string]algCreateFunc{
			operator.OpNameAdd:          createBinaryAlgNoComm,
			operator.OpNameMinus:        createBinaryAlgNoComm,
			operator.OpNameMul:          createBinaryAlg,
			operator.OpNameDiv:          createBinaryAlg,
			operator.OpNameIntDiv:       createBinaryAlg,
			operator.OpNameGreater:      createBinaryAlg,
			operator.OpNameLess:         createBinaryAlg,
			operator.OpNameGreaterEqual: createBinaryAlg,
			operator.OpNameLessEqual:    createBinaryAlg,
			operator.OpNameEqual:        createBinaryAlg,
			operator.OpNameNotEqual:     createBinaryAlg,
			operator.OpNameLogicalAnd:   createBinaryAlg,
			operator.OpNameLogicalOr:    createBinaryAlg,
			operator.OpNameIn:           createInAlg,
		},
	}
	return creator
}

func (a *algCreator) getCreator(opName string) algCreateFunc {
	return a.creators[opName]
}

func createPlacementByCCL(cc *ccl.CCL, allParties []string) []placement {
	var result []placement
	// add share placement
	result = append(result, &sharePlacement{partyCodes: allParties})
	isPublic := true
	for _, party := range allParties {
		if cc.IsVisibleFor(party) {
			result = append(result, &privatePlacement{partyCode: party})
			continue
		}
		isPublic = false
	}
	if isPublic {
		result = append(result, &publicPlacement{partyCodes: allParties})
	}
	return result
}

func createPlacementByStatus(status scql.TensorStatus, partyCodes []string) (placement, error) {
	switch status {
	case scql.TensorStatus_TENSORSTATUS_PRIVATE:
		if len(partyCodes) != 1 {
			return nil, fmt.Errorf("unsupported party codes number(%d) for private placement", len(partyCodes))
		}
		return &privatePlacement{partyCode: partyCodes[0]}, nil
	case scql.TensorStatus_TENSORSTATUS_SECRET:
		return &sharePlacement{partyCodes: partyCodes}, nil
	case scql.TensorStatus_TENSORSTATUS_PUBLIC:
		return &publicPlacement{partyCodes: partyCodes}, nil
	}
	return nil, fmt.Errorf("unsupported status %+v", status)
}

// avoid revealing data to party who is not visible by ccl
func checkPlacementCCL(pm placement, cc *ccl.CCL) bool {
	switch x := pm.(type) {
	case *privatePlacement:
		if cc.IsVisibleFor(x.partyList()[0]) {
			return true
		}
		return false
	case *publicPlacement:
		for _, p := range pm.partyList() {
			if !cc.IsVisibleFor(p) {
				return false
			}
		}
		return true
	case *sharePlacement:
		return true
	}
	return false
}

type tensorStatusPair struct {
	left  scql.TensorStatus
	right scql.TensorStatus
}

var binaryIOStatusMap = map[tensorStatusPair]scql.TensorStatus{
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_PRIVATE, scql.TensorStatus_TENSORSTATUS_PRIVATE}: scql.TensorStatus_TENSORSTATUS_PRIVATE,
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_PRIVATE, scql.TensorStatus_TENSORSTATUS_PUBLIC}:  scql.TensorStatus_TENSORSTATUS_PRIVATE,
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_PUBLIC, scql.TensorStatus_TENSORSTATUS_PRIVATE}:  scql.TensorStatus_TENSORSTATUS_PRIVATE,
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_SECRET, scql.TensorStatus_TENSORSTATUS_SECRET}:   scql.TensorStatus_TENSORSTATUS_SECRET,
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_PUBLIC, scql.TensorStatus_TENSORSTATUS_SECRET}:   scql.TensorStatus_TENSORSTATUS_SECRET,
	tensorStatusPair{scql.TensorStatus_TENSORSTATUS_SECRET, scql.TensorStatus_TENSORSTATUS_PUBLIC}:   scql.TensorStatus_TENSORSTATUS_SECRET,
}

// create algs for binary ops which need communication when status is share
func createBinaryAlg(in map[string][]*ccl.CCL, out map[string][]*ccl.CCL, allParties []string) ([]*materializedAlgorithm, error) {
	var result []*materializedAlgorithm
	localCalCost := 1
	privatePublicCalCost := 2
	shareCalCost := 3
	commuCost := 1
	if len(in[Left]) != 1 || len(in[Right]) != 1 {
		return nil, fmt.Errorf("verifyBinary: invalid input size Left(%d)/Right(%d)", len(in[Left]), len(in[Right]))
	}
	if len(out[Out]) != 1 {
		return nil, fmt.Errorf("verifyBinary: invalid output size %v", len(out))
	}
	for _, lp := range createPlacementByCCL(in[Left][0], allParties) {
		for _, rp := range createPlacementByCCL(in[Right][0], allParties) {
			alg := &materializedAlgorithm{
				cost: newAlgCost(0, 0),
				inputPlacement: map[string][]placement{
					Left:  []placement{lp},
					Right: []placement{rp},
				},
				outputPlacement: map[string][]placement{
					Out: []placement{},
				}}
			status, exist := binaryIOStatusMap[tensorStatusPair{left: lp.status(), right: rp.status()}]
			if !exist {
				continue
			}
			outPartyCodes := lp.partyList()
			// infer placement
			if areStatusesAllPrivate(lp.status(), rp.status()) && lp.partyList()[0] != rp.partyList()[0] {
				continue
			}
			if areStatusesAllPrivate(rp.status()) {
				outPartyCodes = rp.partyList()
			}
			outPlacement, err := createPlacementByStatus(status, outPartyCodes)
			if err != nil {
				continue
			}
			// continue if check ccl failed
			if !checkPlacementCCL(outPlacement, out[Out][0]) {
				continue
			}
			alg.outputPlacement[Out] = append(alg.outputPlacement[Out], outPlacement)
			// calculate cost. No need to consider public vs public
			if oneOfStatusesPrivate(lp.status(), rp.status()) {
				if oneOfStatusesPublic(lp.status(), rp.status()) {
					alg.cost.calculationCost = privatePublicCalCost
				} else {
					alg.cost.calculationCost = localCalCost
				}
			} else {
				alg.cost.calculationCost = shareCalCost
			}
			if oneOfStatusesShare(lp.status(), rp.status()) {
				alg.cost.communicationCost = commuCost
			} else {
				alg.cost.communicationCost = 0
			}
			result = append(result, alg)
		}
	}
	return result, nil
}

// create algs for binary ops which don't need communication when status is share
func createBinaryAlgNoComm(in map[string][]*ccl.CCL, out map[string][]*ccl.CCL, allParties []string) ([]*materializedAlgorithm, error) {
	algs, err := createBinaryAlg(in, out, allParties)
	if err != nil {
		return nil, err
	}
	// assume arithmetic share，could be added without communication
	for _, alg := range algs {
		alg.cost.communicationCost = 0
	}
	return algs, nil
}

// In include psi in/local in/share in. For now, only psi in is supported
func createInAlg(in map[string][]*ccl.CCL, out map[string][]*ccl.CCL, allParties []string) ([]*materializedAlgorithm, error) {
	var result []*materializedAlgorithm
	// one side get result
	psiInCommCost := 1
	// encrypt cost 2, computing intersection cost 1
	psiCalCost := 3
	for _, lp := range createPlacementByCCL(in[Left][0], allParties) {
		for _, rp := range createPlacementByCCL(in[Right][0], allParties) {
			for _, outp := range createPlacementByCCL(out[Out][0], allParties) {
				// left and right tensor must be private
				if !areStatusesAllPrivate(lp.status(), rp.status(), outp.status()) {
					continue
				}
				// left and right tensor must not be in the same side
				if lp.partyList()[0] == rp.partyList()[0] {
					continue
				}
				if outp.partyList()[0] != lp.partyList()[0] && outp.partyList()[0] != rp.partyList()[0] {
					continue
				}
				alg := &materializedAlgorithm{
					cost: newAlgCost(psiInCommCost, psiCalCost),
					inputPlacement: map[string][]placement{
						Left:  []placement{lp},
						Right: []placement{rp},
					},
					outputPlacement: map[string][]placement{
						Out: []placement{outp},
					}}
				result = append(result, alg)
			}
		}
	}
	return result, nil
}
