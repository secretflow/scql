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

	"github.com/secretflow/scql/pkg/parser/ast"
)

func (n *OperatorResult) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	return nil, fmt.Errorf("OperatorResult is not any tensor's source node. Thus, we nerver need to call ReverseInfer on it")
}

func (n *OperatorRunSQL) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// RunSQL node does not have any inputs, so we do not need to perform reverse inference
	return nil, nil
}

func (n *OperatorDataSource) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	return nil, fmt.Errorf("OperatorDataSource should only exist in OperatorRunSQL's subPlanNodes. Thus, we never need to call ReverseInfer on it")
}

func (n *OperatorEQJoin) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on EQJoin
	return nil, nil
}

func (n *OperatorCrossJoin) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	updatedTensors := make([]*TensorMeta, 0)

	for idx, output := range n.leftOutputs {
		input := n.leftInputs[idx]
		if vs.vt.TryUpdateVisibility(input, vs.vt.TensorVisibleParties(output)) {
			updatedTensors = append(updatedTensors, input)
		}
	}
	for idx, output := range n.rightOutputs {
		input := n.rightInputs[idx]
		if vs.vt.TryUpdateVisibility(input, vs.vt.TensorVisibleParties(output)) {
			updatedTensors = append(updatedTensors, input)
		}
	}

	return updatedTensors, nil
}

func (n *OperatorLimit) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on Limit
	return nil, nil
}

func (n *OperatorFilter) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on Filter
	return nil, nil
}

func (n *OperatorBroadcastTo) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// no need to perform reverse inference on Broadcast
	// it's input scalar tensor always has public visibility
	return nil, nil
}

func (n *OperatorConcat) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	updatedTensors := make([]*TensorMeta, 0)

	vis := vs.vt.TensorVisibleParties(n.output)
	for _, input := range n.inputs {
		if vs.vt.TryUpdateVisibility(input, vis) {
			updatedTensors = append(updatedTensors, input)
		}
	}

	return updatedTensors, nil
}

func (n *OperatorSort) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	updatedTensors := make([]*TensorMeta, 0)

	for idx, output := range n.outputs {
		input := n.payloads[idx]
		if vs.vt.TryUpdateVisibility(input, vs.vt.TensorVisibleParties(output)) {
			updatedTensors = append(updatedTensors, input)
		}
	}

	return updatedTensors, nil
}

func (n *OperatorReduce) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on Reduce
	return nil, nil
}

func (n *OperatorGroupAgg) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on GroupAgg
	// here we just fill the keysVisAfterAgg, which will be useful when resolving kernels
	isGroupKey := func(t *TensorMeta) bool {
		for _, key := range n.groupKeys {
			if key.ID == t.ID {
				return true
			}
		}
		return false
	}

	ovt := vs.NewOverlayVisibilityTable()
	for idx, arg := range n.aggArgs {
		aggFunc := n.aggFuncsWithArg[idx]
		if aggFunc.Name != ast.AggFuncFirstRow {
			continue
		}

		if isGroupKey(arg) {
			output := n.argFuncOutputs[idx]
			ovt.UpdateVisibility(arg, vs.vt.TensorVisibleParties(output))
		}
	}
	n.keysVisAfterAgg = ovt

	return nil, nil
}

func (n *OperatorWindow) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	updatedTensors := make([]*TensorMeta, 0)

	for idx, output := range n.payloadOutputs {
		input := n.payloads[idx]
		if vs.vt.TryUpdateVisibility(input, vs.vt.TensorVisibleParties(output)) {
			updatedTensors = append(updatedTensors, input)
		}
	}

	return updatedTensors, nil
}

func (n *OperatorIn) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// can not perform reverse inference on In
	return nil, nil
}

func (n *OperatorConstant) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	// no need to perform reverse inference on Constant
	// it's output tensor always has public visibility
	return nil, nil
}

func (n *OperatorFunction) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	updatedTensors := make([]*TensorMeta, 0)
	outputVis := vs.vt.TensorVisibleParties(n.output)
	switch n.funcName {
	case ast.UnaryNot:
		if vs.vt.TryUpdateVisibility(n.inputs[0], outputVis) {
			updatedTensors = append(updatedTensors, n.inputs[0])
		}
	case ast.Lower, ast.Upper, ast.Trim:
		// question: can we update visibility of n.inputs[0] here?
		if vs.vt.TryUpdateVisibility(n.inputs[0], outputVis) {
			updatedTensors = append(updatedTensors, n.inputs[0])
		}
	case ast.Abs, ast.Floor, ast.Ceil, ast.Round:
		// question: can we update visibility of n.inputs[0] here?
		// if vs.vt.TryUpdateVisibility(n.inputs[0], outputVis) {
		// 	updatedTensors = append(updatedTensors, n.inputs[0])
		// }
	case ast.Ln, ast.Log10, ast.Log2, ast.Exp, ast.Sqrt:
		if vs.vt.TryUpdateVisibility(n.inputs[0], outputVis) {
			updatedTensors = append(updatedTensors, n.inputs[0])
		}
	case ast.Cos, ast.Sin, ast.Tan, ast.Acos, ast.Asin, ast.Atan:
		// question: can we update visibility of n.inputs[0] here?
		// if vs.vt.TryUpdateVisibility(n.inputs[0], outputVis) {
		// 	updatedTensors = append(updatedTensors, n.inputs[0])
		// }
	case ast.Pow, ast.IntDiv:
		// question: can we update visibility of inputs here?
	case ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.AddDate, ast.SubDate, ast.DateDiff:
		oldLhsVis := vs.vt.TensorVisibleParties(n.inputs[0])
		oldRhsVis := vs.vt.TensorVisibleParties(n.inputs[1])
		// infer one input visibility from the visibility of the other input and the output
		newLhsVis := VPIntersection(oldRhsVis, outputVis)
		newRhsVis := VPIntersection(oldLhsVis, outputVis)

		if vs.vt.TryUpdateVisibility(n.inputs[0], newLhsVis) {
			updatedTensors = append(updatedTensors, n.inputs[0])
		}
		if vs.vt.TryUpdateVisibility(n.inputs[1], newRhsVis) {
			updatedTensors = append(updatedTensors, n.inputs[1])
		}
	}
	return updatedTensors, nil
}
