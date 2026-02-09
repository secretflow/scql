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
	"slices"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type KernelResolver struct {
	srm         *SecurityRelaxationManager
	vt          *VisibilityTable
	tm          *TensorManager
	compileOpts *v1.CompileOptions
}

func NewKernelResolver(srm *SecurityRelaxationManager, vt *VisibilityTable, tm *TensorManager, compileOpts *v1.CompileOptions) *KernelResolver {
	return &KernelResolver{
		srm:         srm,
		vt:          vt,
		tm:          tm,
		compileOpts: compileOpts,
	}
}

// choosePlacement determines the optimal placement for a tensor meta with standard rules.
// Secret placement is chosen only when corresponding tensor meta has empty visible parties.
func (kr *KernelResolver) choosePlacement(meta *TensorMeta) tensorPlacement {
	return kr.vt.choosePlacement(meta, kr.tm, true, true, false)
}

// chooseSecretOrPrivatePlacement determines placement considering only private and secret options.
func (kr *KernelResolver) chooseSecretOrPrivatePlacement(meta *TensorMeta) tensorPlacement {
	return kr.vt.choosePlacement(meta, kr.tm, false, true, false)
}

// choosePlacementAllFine determines placement with relaxed constraints.
// Secret placement is chosen only when corresponding tensor meta has empty visible parties,
// or when there are only one existing tensor and its status is secret.
func (kr *KernelResolver) choosePlacementAllFine(meta *TensorMeta) tensorPlacement {
	return kr.vt.choosePlacement(meta, kr.tm, true, true, true)
}

func (kr *KernelResolver) Resolve(n Operator) (Kernel, error) {
	switch x := n.(type) {
	case *OperatorEQJoin:
		return kr.resolveEQJoin(x)
	case *OperatorRunSQL:
		return kr.resolveRunSQL(x)
	case *OperatorResult:
		return kr.resolveResult(x)
	case *OperatorReduce:
		return kr.resolveReduce(x)
	case *OperatorBroadcastTo:
		return kr.resolveBroadcastTo(x)
	case *OperatorGroupAgg:
		return kr.resolveGroupAgg(x)
	case *OperatorCrossJoin:
		return kr.resolveCrossJoin(x)
	case *OperatorLimit:
		return kr.resolveLimit(x)
	case *OperatorFilter:
		return kr.resolveFilter(x)
	case *OperatorConcat:
		return kr.resolveConcat(x)
	case *OperatorSort:
		return kr.resolveSort(x)
	case *OperatorConstant:
		return kr.resolveConstant(x)
	case *OperatorIn:
		return kr.resolveIn(x)
	case *OperatorFunction:
		return kr.resolveFunction(x)
	case *OperatorWindow:
		return kr.resolveWindow(x)
	default:
		return nil, fmt.Errorf("Resolve Kernel: unsupported node type: %T", n)
	}
}

func (kr *KernelResolver) resolveRunSQL(_ *OperatorRunSQL) (Kernel, error) {
	return &KernelRunSQL{}, nil
}

func (kr *KernelResolver) resolveResult(n *OperatorResult) (Kernel, error) {
	if n.intoOpt != nil {
		return &KernelDumpFile{intoOpt: n.intoOpt}, nil
	}
	if n.insertTableOpt != nil {
		return &KernelInsertTable{insertTableOpt: n.insertTableOpt}, nil
	}
	return &KernelPublishResult{}, nil
}

func (kr *KernelResolver) resolveEQJoin(n *OperatorEQJoin) (Kernel, error) {
	// TODO add check
	leftKeysVP := kr.vt.CommonVisibleParties(n.leftKeys)
	rightKeysVp := kr.vt.CommonVisibleParties(n.rightKeys)

	// TODO implement local join
	// try local join
	// allKeysVP := VPIntersection(leftKeysVP, rightKeysVp)
	// localJoinParty := allKeysVP.GetOneParty()

	// If len(n.leftOutputs) == 0, then the left party do not need to filter the join payload, thus do not need to get the join result index
	// TODO: Currently, in LeftOuterJoin, left party should always get the join result index. This maybe improved in the future.
	// OPRF does not allow flexible configuration of which party receives the result; when using the OPRF algorithm, by default both parties obtain the result.
	useOprf := kr.compileOpts != nil && kr.compileOpts.PsiAlgorithmType == proto.PsiAlgorithmType_OPRF
	leftTouchResult := len(n.leftOutputs) > 0 || n.joinType == core.LeftOuterJoin || useOprf
	rightTouchResult := len(n.rightOutputs) > 0 || n.joinType == core.RightOuterJoin || useOprf
	checkPsiParties := func(leftParty, rightParty string) bool {
		if leftParty == rightParty {
			return false
		}
		// leftTouchResult == true means left party should get the join result index
		if leftTouchResult {
			// right keys should be visible to left party after join
			if !kr.srm.GetCSR(RevealKeyAfterJoin).AllApplicable(n.rightKeys, kr.vt) {
				return false
			}
		}
		if rightTouchResult {
			// left keys should be visible to right party after join
			if !kr.srm.GetCSR(RevealKeyAfterJoin).AllApplicable(n.leftKeys, kr.vt) {
				return false
			}
		}
		return true
	}

	// try psi join
	getPsiParties := func(leftParties, rightParties []string) (string, string) {
		partyPairs := [][]string{}
		existingTensorCount := []int{}
		for _, leftParty := range leftParties {
			for _, rightParty := range rightParties {
				if !checkPsiParties(leftParty, rightParty) {
					continue
				}
				partyPairs = append(partyPairs, []string{leftParty, rightParty})
				count := 0
				for _, meta := range n.leftKeys {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: leftParty}) {
						count = count + 2 // heuristic
					}
				}
				for _, meta := range n.rightKeys {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: rightParty}) {
						count = count + 2
					}
				}
				for _, meta := range n.leftPayloads {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: leftParty}) || kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: rightParty}) {
						count++
					}
				}
				for _, meta := range n.rightPayloads {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: leftParty}) || kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: rightParty}) {
						count++
					}
				}
				existingTensorCount = append(existingTensorCount, count)
			}
		}

		if len(partyPairs) == 0 {
			return "", ""
		}

		chosenPairIdx := 0
		for idx, count := range existingTensorCount {
			if existingTensorCount[chosenPairIdx] < count {
				chosenPairIdx = idx
			}
		}
		return partyPairs[chosenPairIdx][0], partyPairs[chosenPairIdx][1]
	}
	leftParty, rightParty := getPsiParties(leftKeysVP.GetParties(), rightKeysVp.GetParties())

	if leftParty != "" && rightParty != "" {
		leftPayloadsPlacement := []tensorPlacement{}
		for _, payload := range n.leftPayloads {
			placement := kr.chooseSecretOrPrivatePlacement(payload)
			if IsSecret(placement) {
				return &KernelSecretEQJoin{}, nil
			}
			leftPayloadsPlacement = append(leftPayloadsPlacement, placement)
		}
		rightPayloadsPlacement := []tensorPlacement{}
		for _, payload := range n.rightPayloads {
			placement := kr.chooseSecretOrPrivatePlacement(payload)
			if IsSecret(placement) {
				return &KernelSecretEQJoin{}, nil
			}
			rightPayloadsPlacement = append(rightPayloadsPlacement, placement)
		}
		psiAlg := proto.PsiAlgorithmType_AUTO
		if kr.compileOpts != nil {
			psiAlg = kr.compileOpts.GetPsiAlgorithmType()
		}

		return &KernelPsiEQJoin{
			leftParty:              leftParty,
			rightParty:             rightParty,
			psiAlgorithmType:       psiAlg,
			leftPayloadsPlacement:  leftPayloadsPlacement,
			rightPayloadsPlacement: rightPayloadsPlacement,
			leftTouchResult:        leftTouchResult,
			rightTouchResult:       rightTouchResult,
		}, nil
	}

	return &KernelSecretEQJoin{}, nil
}

func (kr *KernelResolver) resolveReduce(n *OperatorReduce) (Kernel, error) {
	switch n.aggFunc.Name {
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncAvg, ast.AggPercentileDisc:
		// simple reduce only support private and secret inputs
		placement := kr.chooseSecretOrPrivatePlacement(n.input)
		return &KernelSimpleReduce{placement: placement, aggFunc: n.aggFunc}, nil
	case ast.AggFuncCount:
		switch n.aggFunc.Mode {
		case aggregation.CompleteMode:
			if n.aggFunc.HasDistinct {
				vis := kr.vt.TensorVisibleParties(n.input)
				// for now we only support private and secret distinct count
				if vis.IsEmpty() {
					return &KernelSecretDistinctCount{}, nil
				}
				return &KernelPrivateDistinctCount{partyCode: vis.GetOneParty()}, nil
			} else {
				if _, ok := n.aggFunc.Args[0].(*expression.Constant); ok {
					placement := kr.choosePlacement(n.input)
					if pp, ok := placement.(*privatePlacement); ok {
						return &KernelShapeCount{inputPlacement: placement, partyCode: pp.partyCode}, nil
					}
					return &KernelShapeCount{inputPlacement: placement, partyCode: kr.vt.PublicVisibility().GetOneParty()}, nil
				}
				placement := kr.chooseSecretOrPrivatePlacement(n.input)
				return &KernelSimpleReduce{placement: placement, aggFunc: n.aggFunc}, nil
			}
		case aggregation.FinalMode:
			placement := kr.chooseSecretOrPrivatePlacement(n.input)
			return &KernelSimpleReduce{placement: placement, aggFunc: n.aggFunc}, nil
		default:
			return nil, fmt.Errorf("resolveReduce: unsupported aggregation mode %v", n.aggFunc.Mode)
		}
	}
	return nil, fmt.Errorf("resolveReduce: unsupported aggregation function %v", n.aggFunc)
}

func (kr *KernelResolver) resolveBroadcastTo(n *OperatorBroadcastTo) (Kernel, error) {
	for _, scalar := range n.scalars {
		if !kr.vt.VisibilityPublic(kr.vt.TensorVisibleParties(scalar)) {
			return nil, fmt.Errorf("resolveBroadcastTo: scalar %v is not public", scalar)
		}
	}

	// TODO consider cost of status conversion
	placement := kr.choosePlacementAllFine(n.shapeRef)
	return &KernelBroadcastTo{refPlacement: placement}, nil
}

func (kr *KernelResolver) resolveGroupAgg(n *OperatorGroupAgg) (Kernel, error) {
	privateCandidates := kr.vt.CommonVisibleParties(n.groupKeys)

	for idx, aggFunc := range n.aggFuncsWithArg {
		argVP := kr.vt.TensorVisibleParties(n.aggArgs[idx])
		// check if private groupby secret sum(PGS) is suitable, ref: https://github.com/secretflow/spu/pull/1312
		if (aggFunc.Name == ast.AggFuncSum || aggFunc.Name == ast.AggFuncAvg) && !argVP.IsEmpty() {
			continue
		}

		privateCandidates = VPIntersection(privateCandidates, argVP)
	}

	if !privateCandidates.IsEmpty() {
		// TODO choose best candidate
		partyCode := privateCandidates.GetOneParty()
		kernel := &KernelPrivateGroupAgg{
			partyCode:  partyCode,
			PGSParties: make(map[int]string),
		}
		PGSFuncNum := 0
		for idx, aggFunc := range n.aggFuncsWithArg {
			if aggFunc.Name == ast.AggFuncSum || aggFunc.Name == ast.AggFuncAvg {
				argVP := kr.vt.TensorVisibleParties(n.aggArgs[idx])
				if !argVP.Contains(partyCode) {
					kernel.PGSParties[n.aggArgs[idx].ID] = argVP.GetOneParty()
					PGSFuncNum++
				}
			}
		}
		kernel.cost = float64(PGSFuncNum) / float64(len(n.aggFuncsWithArg)) * 0.3
		return kernel, nil
	}

	privateSortCandidates := NewVisibleParties([]string{})
	if kr.srm.global.RevealGroupCount {
		if n.keysVisAfterAgg != nil {
			// keysVisAfterAgg will be set if reverse inference is performed on this OperatorGroupAgg.
			privateSortCandidates = n.keysVisAfterAgg.CommonVisibleParties(n.groupKeys)
		} else {
			privateSortCandidates = kr.vt.CommonVisibleParties(n.groupKeys)
		}

		// skip if only one group-by key
		// TODO: check logic here
		if len(n.groupKeys) == 1 {
			privateSortCandidates = NewVisibleParties([]string{})
		}
	}
	return &KernelObliviousGroupAgg{privateSortParty: privateSortCandidates.GetOneParty(), revealGroupMark: kr.srm.global.RevealGroupMark}, nil
}

func (kr *KernelResolver) resolveCrossJoin(n *OperatorCrossJoin) (Kernel, error) {
	leftVP := kr.vt.CommonVisibleParties(n.leftInputs)
	rightVP := kr.vt.CommonVisibleParties(n.rightInputs)

	// TODO: support local replicate
	// inputsVP := VPIntersection(leftVP, rightVP)
	// if !inputsVP.IsEmpty() {
	// 	party := inputsVP.GetOneParty()
	// 	return &KernelCrossJoin{leftParty: party, rightParty: party}, nil
	// }

	getPartyPair := func(leftParties, rightParties []string) (string, string) {
		leftRes := ""
		rightRes := ""
		resExitingCount := -1
		for _, lp := range leftParties {
			for _, rp := range rightParties {
				existingTensorCount := 0
				for _, meta := range n.leftInputs {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: lp}) {
						existingTensorCount++
					}
				}
				for _, meta := range n.rightInputs {
					if kr.tm.hasPlacedTensor(meta, &privatePlacement{partyCode: rp}) {
						existingTensorCount++
					}
				}

				if existingTensorCount > resExitingCount {
					leftRes = lp
					rightRes = rp
					resExitingCount = existingTensorCount
				}
			}
		}
		return leftRes, rightRes
	}

	leftParty, rightParty := getPartyPair(leftVP.GetParties(), rightVP.GetParties())
	if leftParty != "" && rightParty != "" {
		return &KernelCrossJoin{leftParty: leftParty, rightParty: rightParty}, nil
	}

	return nil, fmt.Errorf("resolveCrossJoin: no valid kernel, left inputs VP : %v, right inputs VP : %v", leftVP, rightVP)
}

func (kr *KernelResolver) resolveLimit(_ *OperatorLimit) (Kernel, error) {
	return &KernelLimit{}, nil
}

func (kr *KernelResolver) resolveFilter(n *OperatorFilter) (Kernel, error) {
	// for now, filter Op in engine support secret and private input:
	// when input is secret, a public filter is required,
	// when input is private, a public or colocated private filter is required

	filterVP := kr.vt.TensorVisibleParties(n.mask)

	// tensor2Partition := make(map[*TensorMeta]*VisibleParties)
	privatePartitions := []*VisibleParties{}
	tensor2PartitionIdx := make(map[*TensorMeta]int)
	for _, idx := range sliceutil.ArgSort(n.inputs, func(a, b *TensorMeta) bool { return a.ID < b.ID }) {
		input := n.inputs[idx]
		inputVP := kr.vt.TensorVisibleParties(input)
		findPartition := false
		for idx, partitionVP := range privatePartitions {
			intersection := VPIntersection(partitionVP, inputVP)
			if !intersection.IsEmpty() {
				partitionVP.CopyFrom(intersection)
				tensor2PartitionIdx[input] = idx
				findPartition = true
				break
			}
		}
		if !findPartition {
			intersection := VPIntersection(filterVP, inputVP)
			if !intersection.IsEmpty() {
				privatePartitions = append(privatePartitions, intersection)
				tensor2PartitionIdx[input] = len(privatePartitions) - 1
			} else {
				if !kr.vt.VisibilityPublic(filterVP) {
					return nil, fmt.Errorf("resolveFilter: can not find proper party for input: %v, input VP: %v, filter VP: %v", input, inputVP, filterVP)
				}
			}
		}
	}

	privateParties := make([]string, 0, len(privatePartitions))
	for _, partition := range privatePartitions {
		privateParties = append(privateParties, partition.GetOneParty())
	}

	inputPlacements := make([]tensorPlacement, 0, len(n.inputs))
	for _, input := range n.inputs {
		partitionIdx, ok := tensor2PartitionIdx[input]
		if !ok {
			inputPlacements = append(inputPlacements, &secretPlacement{})
			continue
		}
		chosenParty := privateParties[partitionIdx]

		existingParties := kr.tm.existingPrivateParties(input)
		for _, party := range privateParties {
			if slices.Contains(existingParties, party) {
				chosenParty = party
				break
			}
		}
		inputPlacements = append(inputPlacements, &privatePlacement{partyCode: chosenParty})
	}
	return &KernelFilter{inputPlacements: inputPlacements}, nil
}

func (kr *KernelResolver) resolveConcat(_ *OperatorConcat) (Kernel, error) {
	// only support secret concat now
	// question: should we support private concat?

	return &KernelSecretConcat{}, nil
}

func (kr *KernelResolver) resolveSort(n *OperatorSort) (Kernel, error) {
	privateCandidates := kr.vt.CommonVisibleParties(append(n.sortKeys, n.payloads...))
	if privateCandidates.IsEmpty() {
		return &KernelSecretSort{}, nil
	}
	return &KernelPrivateSort{partyCode: privateCandidates.GetOneParty()}, nil
}

func (kr *KernelResolver) resolveConstant(_ *OperatorConstant) (Kernel, error) {
	return &KernelConstant{}, nil
}

func (kr *KernelResolver) resolveIn(n *OperatorIn) (Kernel, error) {
	// TODO consider transition cost
	// TODO support secret In
	leftVP := kr.vt.TensorVisibleParties(n.left)
	rightVP := kr.vt.TensorVisibleParties(n.right)
	localInCandidates := VPIntersection(leftVP, rightVP)
	if !localInCandidates.IsEmpty() {
		return &KernelLocalIn{partyCode: localInCandidates.GetOneParty()}, nil
	}
	if !leftVP.IsEmpty() && !rightVP.IsEmpty() {
		psiAlg := proto.PsiAlgorithmType_AUTO
		if kr.compileOpts != nil {
			psiAlg = kr.compileOpts.GetPsiAlgorithmType()
		}
		return &KernelPsiIn{leftParty: leftVP.GetOneParty(), rightParty: rightVP.GetOneParty(), psiAlgorithmType: psiAlg}, nil
	}

	return nil, fmt.Errorf("resolveIn: no valid kernel, left VP : %v, right VP : %v", leftVP, rightVP)
}

func (kr *KernelResolver) resolveFunction(n *OperatorFunction) (Kernel, error) {
	// TODO consider transition cost
	privateCandidates := kr.vt.CommonVisibleParties(n.inputs)
	getPublicOrSecretPlace := func(v *VisibleParties) tensorPlacement {
		if kr.vt.VisibilityPublic(v) {
			return &publicPlacement{}
		}
		return &secretPlacement{}
	}

	switch n.funcName {
	case ast.Lower, ast.Upper, ast.Trim, ast.Substr, ast.Substring, ast.StrToDate:
		if privateCandidates.IsEmpty() {
			return nil, fmt.Errorf("resolveFunction: could not find proper candidate for ArrowFunc")
		}
		return &KernelArrowFunction{partyCode: privateCandidates.GetOneParty()}, nil
	case ast.Abs, ast.Ceil, ast.Floor, ast.Round, ast.Radians, ast.Degrees, ast.Ln, ast.Log10, ast.Log2, ast.Sqrt, ast.Exp, ast.UnaryNot, ast.Cos, ast.Sin, ast.Acos, ast.Asin, ast.Tan, ast.Atan:
		return &KernelUnary{place: kr.choosePlacement(n.inputs[0])}, nil
	case ast.LT, ast.GT, ast.LE, ast.GE, ast.EQ, ast.NE, ast.LogicOr, ast.LogicAnd, ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.IntDiv, ast.Mod, ast.AddDate, ast.SubDate, ast.DateDiff, ast.Pow, ast.Atan2:
		if !privateCandidates.IsEmpty() {
			// TODO consider all public
			placement := kr.tm.choosePrivatePlacement(privateCandidates, n.inputs)
			return &KernelBinary{lhsPlace: placement, rhsPlace: placement, outPlace: placement}, nil
		}

		lhsVP := kr.vt.TensorVisibleParties(n.inputs[0])
		rhsVP := kr.vt.TensorVisibleParties(n.inputs[1])

		return &KernelBinary{lhsPlace: getPublicOrSecretPlace(lhsVP), rhsPlace: getPublicOrSecretPlace(rhsVP), outPlace: &secretPlacement{}}, nil
	case ast.Greatest, ast.Least:
		if privateCandidates.IsEmpty() {
			return &KernelVariadicCompare{placement: &secretPlacement{}}, nil
		}
		return &KernelVariadicCompare{placement: kr.tm.choosePrivatePlacement(privateCandidates, n.inputs)}, nil
	case ast.Ifnull, ast.IsNull, ast.Coalesce:
		if privateCandidates.IsEmpty() {
			return nil, fmt.Errorf("resolveFunction: could not find proper candidate for %s", n.funcName)
		}
		return &KernelPrivateFunc{partyCode: kr.tm.choosePrivatePlacement(privateCandidates, n.inputs).partyCode}, nil
	case ast.Cast:
		// TODO consider public cast
		placement := kr.chooseSecretOrPrivatePlacement(n.inputs[0])
		if IsSecret(placement) && n.output.DType.IsStringType() {
			return nil, fmt.Errorf("resolveFunction:not support cast for string in spu, which exists in hash form")
		}
		return &KernelCast{placement: placement}, nil
	case ast.If:
		party := privateCandidates.GetOneParty()
		if party != "" {
			place := &privatePlacement{partyCode: party}
			return &KernelIf{condPlace: place, trueValuePlace: place, falseValuePlace: place, outputPlace: place}, nil
		} else {
			condPlace := getPublicOrSecretPlace(kr.vt.TensorVisibleParties(n.inputs[0]))
			trueValuePlace := getPublicOrSecretPlace(kr.vt.TensorVisibleParties(n.inputs[1]))
			falseValuePlace := getPublicOrSecretPlace(kr.vt.TensorVisibleParties(n.inputs[2]))
			return &KernelIf{condPlace: condPlace, trueValuePlace: trueValuePlace, falseValuePlace: falseValuePlace, outputPlace: &secretPlacement{}}, nil
		}
	case ast.Case:
		inputPlacements := make([]tensorPlacement, 0, len(n.inputs))
		if privateCandidates.IsEmpty() {
			for _, input := range n.inputs {
				inputPlacements = append(inputPlacements, getPublicOrSecretPlace(kr.vt.TensorVisibleParties(input)))
			}
			return &KernelCase{inputPlacements: inputPlacements, outputPlacement: &secretPlacement{}}, nil
		} else {
			place := kr.tm.choosePrivatePlacement(privateCandidates, n.inputs)
			for range n.inputs {
				inputPlacements = append(inputPlacements, place)
			}
			return &KernelCase{inputPlacements: inputPlacements, outputPlacement: place}, nil
		}
	case ast.Concat:
		partyCode := privateCandidates.GetOneParty()
		if partyCode == "" {
			return nil, fmt.Errorf("resolveFunction: could not find proper candidate for string concat")
		}
		return &KernelConcatString{partyCode: partyCode}, nil
	default:
		return nil, fmt.Errorf("resolveFunction: unsupported function %s", n.funcName)
	}
}

func (kr *KernelResolver) resolveWindow(n *OperatorWindow) (Kernel, error) {
	privateCandidates := kr.vt.CommonVisibleParties(append(n.partitionKeys, n.orderKeys...))
	if privateCandidates.IsEmpty() {
		return &KernelObliviousWindow{}, nil
	} else {
		return &KernelPrivateWindow{partyCode: privateCandidates.GetOneParty()}, nil
	}
}
