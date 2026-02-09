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
	"strings"

	"github.com/secretflow/scql/pkg/parser/ast"
)

func (n *OperatorDataSource) InitVis(vr VisibilityRegistry, initialVisibility map[string]*VisibleParties) error {
	for idx, output := range n.outputs {
		vr.UpdateVisibility(output, NewVisibleParties([]string{n.sourceParty}))
		oriVis, ok := initialVisibility[strings.ToLower(n.originNames[idx])]
		if ok {
			vr.UpdateVisibility(output, oriVis)
		}

	}
	return nil
}

func (n *OperatorDataSource) InferVis(vr VisibilityRegistry) error {
	// Nothing to do here
	return nil
}

func (n *OperatorRunSQL) InferVis(vr VisibilityRegistry) error {
	// Nothing to do here
	return nil
}

func (n *OperatorEQJoin) InferVis(vr VisibilityRegistry) error {
	keysVP := VPIntersection(vr.CommonVisibleParties(n.leftKeys), vr.CommonVisibleParties(n.rightKeys))

	for idx := range n.leftPayloads {
		outputVP := VPIntersection(keysVP, vr.TensorVisibleParties(n.leftPayloads[idx]))
		vr.UpdateVisibility(n.leftOutputs[idx], outputVP)
	}
	for idx := range n.rightPayloads {
		outputVP := VPIntersection(keysVP, vr.TensorVisibleParties(n.rightPayloads[idx]))
		vr.UpdateVisibility(n.rightOutputs[idx], outputVP)
	}

	return nil
}

func (n *OperatorCrossJoin) InferVis(vr VisibilityRegistry) error {
	for idx := range n.leftInputs {
		vr.UpdateVisibility(n.leftOutputs[idx], vr.TensorVisibleParties(n.leftInputs[idx]))
	}
	for idx := range n.rightInputs {
		vr.UpdateVisibility(n.rightOutputs[idx], vr.TensorVisibleParties(n.rightInputs[idx]))
	}
	return nil
}

func (n *OperatorLimit) InferVis(vr VisibilityRegistry) error {
	for idx := range n.inputs {
		vr.UpdateVisibility(n.outputs[idx], vr.TensorVisibleParties(n.inputs[idx]))
	}
	return nil
}

func (n *OperatorFilter) InferVis(vr VisibilityRegistry) error {
	filterVP := vr.TensorVisibleParties(n.mask)
	for idx, input := range n.inputs {
		outputVP := VPIntersection(vr.TensorVisibleParties(input), filterVP)
		vr.UpdateVisibility(n.outputs[idx], outputVP)
	}
	return nil
}

func (n *OperatorBroadcastTo) InferVis(vr VisibilityRegistry) error {
	// TODO question: should we consider the shape ref's visibility here?
	for idx, input := range n.scalars {
		vr.UpdateVisibility(n.outputs[idx], vr.TensorVisibleParties(input))
	}
	return nil
}

func (n *OperatorConcat) InferVis(vr VisibilityRegistry) error {
	inputsVP := vr.CommonVisibleParties(n.inputs)
	vr.UpdateVisibility(n.output, inputsVP)
	return nil
}

func (n *OperatorSort) InferVis(vr VisibilityRegistry) error {
	rankVP := vr.CommonVisibleParties(n.sortKeys)

	for idx, input := range n.payloads {
		outputVP := VPIntersection(vr.TensorVisibleParties(input), rankVP)
		vr.UpdateVisibility(n.outputs[idx], outputVP)
	}
	return nil
}

func (n *OperatorReduce) InferVis(vr VisibilityRegistry) error {
	switch n.aggFunc.Name {
	case ast.AggFuncCount:
		vr.UpdateVisibility(n.output, vr.PublicVisibility())
	case ast.AggFuncFirstRow, ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncMax, ast.AggFuncMin, ast.AggPercentileDisc:
		vr.UpdateVisibility(n.output, vr.TensorVisibleParties(n.input))
	default:
		return fmt.Errorf("OperatorReduce InferVis: unsupported agg func %s", n.aggFunc.Name)
	}
	return nil
}

func (n *OperatorGroupAgg) InferVis(vr VisibilityRegistry) error {
	keysVP := vr.CommonVisibleParties(n.groupKeys)

	for idx, aggFunc := range n.aggFuncsWithArg {
		switch aggFunc.Name {
		case ast.AggFuncCount:
			vr.UpdateVisibility(n.argFuncOutputs[idx], keysVP)
		case ast.AggFuncFirstRow, ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncMax, ast.AggFuncMin, ast.AggPercentileDisc:
			outputVP := VPIntersection(vr.TensorVisibleParties(n.aggArgs[idx]), keysVP)
			vr.UpdateVisibility(n.argFuncOutputs[idx], outputVP)
		default:
			return fmt.Errorf("OperatorGroupAgg InferVis: unsupported agg func %s", aggFunc.Name)
		}
	}
	for idx := range n.simpleCountOutputs {
		vr.UpdateVisibility(n.simpleCountOutputs[idx], keysVP)
	}
	return nil
}

func (n *OperatorWindow) InferVis(vr VisibilityRegistry) error {
	keysVP := vr.CommonVisibleParties(n.orderKeys)

	vr.UpdateVisibility(n.funcOutput, keysVP)
	for idx, input := range n.payloads {
		// FIXME should we consider keysVP here?
		vr.UpdateVisibility(n.payloadOutputs[idx], vr.TensorVisibleParties(input))
	}
	return nil
}

func (n *OperatorIn) InferVis(vr VisibilityRegistry) error {
	outVP := VPIntersection(vr.TensorVisibleParties(n.left), vr.TensorVisibleParties(n.right))
	vr.UpdateVisibility(n.output, outVP)
	return nil
}

func (n *OperatorFunction) InferVis(vr VisibilityRegistry) error {
	vr.UpdateVisibility(n.output, vr.CommonVisibleParties(n.inputs))
	return nil
}

func (n *OperatorConstant) InferVis(vr VisibilityRegistry) error {
	outVP := vr.PublicVisibility()
	vr.UpdateVisibility(n.output, outVP)
	return nil
}

func (n *OperatorResult) InferVis(vr VisibilityRegistry) error {
	return fmt.Errorf("OperatorResult's InferVis should not be called")
}
