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
)

func (n *OperatorDataSource) InitCSR(csr ColumnSecurityRelaxation, applicableColNames []string) error {
	// Convert applicableColNames to lowercase and store in map for O(1) lookup
	applicableColMap := make(map[string]struct{}, len(applicableColNames))
	for _, name := range applicableColNames {
		applicableColMap[strings.ToLower(name)] = struct{}{}
	}

	for idx, originName := range n.originNames {
		if _, exists := applicableColMap[strings.ToLower(originName)]; exists {
			csr.MakeApplicable(n.outputs[idx])
		}
	}
	return nil
}

func (n *OperatorDataSource) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	// Nothing to do here
	return nil
}

func (n *OperatorRunSQL) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	// Nothing to do here
	return nil
}

func (n *OperatorConstant) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	// Nothing to do here
	return nil
}

func (n *OperatorEQJoin) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	// TODO question shold we consider keys' column security relaxation or not
	for idx, payload := range n.leftPayloads {
		if csr.ApplicableTo(payload, tvc) {
			csr.MakeApplicable(n.leftOutputs[idx])
		}
	}
	for idx, payload := range n.rightPayloads {
		if csr.ApplicableTo(payload, tvc) {
			csr.MakeApplicable(n.rightOutputs[idx])
		}
	}
	return nil
}

func (n *OperatorCrossJoin) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	for idx, input := range n.leftInputs {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.leftOutputs[idx])
		}
	}
	for idx, input := range n.rightInputs {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.rightOutputs[idx])
		}
	}
	return nil
}

func (n *OperatorLimit) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	for idx, input := range n.inputs {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.outputs[idx])
		}
	}
	return nil
}

func (n *OperatorFilter) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.ApplicableTo(n.mask, tvc) {
		return nil
	}
	for idx, input := range n.inputs {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.outputs[idx])
		}
	}
	return nil
}

func (n *OperatorBroadcastTo) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.ApplicableTo(n.shapeRef, tvc) {
		return nil
	}
	for idx, input := range n.scalars {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.outputs[idx])
		}
	}
	return nil
}

func (n *OperatorConcat) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if csr.AllApplicable(n.inputs, tvc) {
		csr.MakeApplicable(n.output)
	}
	return nil
}

func (n *OperatorSort) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	// questtion: should we consider sort keys' column security relaxation here?
	if !csr.AllApplicable(n.sortKeys, tvc) {
		return nil
	}
	for idx, input := range n.payloads {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.outputs[idx])
		}
	}
	return nil
}

func (n *OperatorReduce) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if csr.ApplicableTo(n.input, tvc) {
		csr.MakeApplicable(n.output)
	}
	return nil
}

func (n *OperatorGroupAgg) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.AllApplicable(n.groupKeys, tvc) {
		return nil
	}

	for idx, input := range n.aggArgs {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.argFuncOutputs[idx])
		}
	}
	for _, output := range n.simpleCountOutputs {
		csr.MakeApplicable(output)
	}

	return nil
}

func (n *OperatorWindow) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.AllApplicable(n.partitionKeys, tvc) {
		return nil
	}
	if !csr.AllApplicable(n.orderKeys, tvc) {
		return nil
	}
	csr.MakeApplicable(n.funcOutput)
	for idx, input := range n.payloads {
		if csr.ApplicableTo(input, tvc) {
			csr.MakeApplicable(n.payloadOutputs[idx])
		}
	}
	return nil
}

func (n *OperatorIn) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.ApplicableTo(n.left, tvc) || !csr.ApplicableTo(n.right, tvc) {
		return nil
	}
	csr.MakeApplicable(n.output)
	return nil
}

func (n *OperatorFunction) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	if !csr.AllApplicable(n.inputs, tvc) {
		return nil
	}
	csr.MakeApplicable(n.output)
	return nil
}

func (n *OperatorResult) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	return fmt.Errorf("OperatorResult's InferCSR should not be called")
}
