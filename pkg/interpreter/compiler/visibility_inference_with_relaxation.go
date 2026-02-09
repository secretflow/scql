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

	"github.com/secretflow/scql/pkg/planner/core"
)

func (n *OperatorFunction) ApplyRevealFilterMask(vr VisibilityRegistry, csr *ColumnSecurityRelaxationBase, consumerTracker *TensorConsumerTracker) error {
	if !csr.ApplicableTo(n.output, vr) {
		return nil
	}

	_, isCompare := isCompareAstFuncMap[n.funcName]
	_, isLogical := isLogicalAstFuncMap[n.funcName]
	if !isCompare && !isLogical {
		return nil
	}

	if !consumerTracker.UsedAsFilterMask(n.output) {
		return nil
	}

	vr.UpdateVisibility(n.output, vr.PublicVisibility())
	return nil
}

func (n *OperatorIn) ApplyRevealFilterMask(vr VisibilityRegistry, csr *ColumnSecurityRelaxationBase, consumerTracker *TensorConsumerTracker) error {
	if !csr.ApplicableTo(n.left, vr) || !csr.ApplicableTo(n.right, vr) {
		return nil
	}

	if !consumerTracker.UsedAsFilterMask(n.output) {
		return nil
	}

	vr.UpdateVisibility(n.output, vr.PublicVisibility())
	return nil
}

func (n *OperatorEQJoin) ApplyRevealKeyAfterJoin(vr VisibilityRegistry, csr *ColumnSecurityRelaxationBase) error {
	// TODO question: should we allow partially applicable?
	if !csr.AllApplicable(n.leftKeys, vr) || !csr.AllApplicable(n.rightKeys, vr) {
		return nil
	}

	if n.joinType == core.InnerJoin || n.joinType == core.RightOuterJoin {
		for _, input := range n.leftKeys {
			vr.UpdateVisibility(input, vr.PublicVisibility())
		}
	}

	if n.joinType == core.InnerJoin || n.joinType == core.LeftOuterJoin {
		for _, input := range n.rightKeys {
			vr.UpdateVisibility(input, vr.PublicVisibility())
		}
	}

	if len(n.leftPayloads) != len(n.leftOutputs) {
		return fmt.Errorf("payloads and outputs num not match in left side")
	}
	if len(n.rightPayloads) != len(n.rightOutputs) {
		return fmt.Errorf("payloads and outputs num not match in right side")
	}

	for idx := range n.leftPayloads {
		vr.UpdateVisibility(n.leftOutputs[idx], vr.TensorVisibleParties(n.leftPayloads[idx]))
	}
	for idx := range n.rightPayloads {
		vr.UpdateVisibility(n.rightOutputs[idx], vr.TensorVisibleParties(n.rightPayloads[idx]))
	}

	return nil
}
