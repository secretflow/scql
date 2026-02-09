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

// inferStandard implements the common inference pattern for most operators
func inferStandard(vs *VisibilitySolver, n Operator) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if err := n.InferVis(vs.vt); err != nil {
		return nil, err
	}
	for _, csr := range vs.srm.csrTable {
		if err := n.InferCSR(csr, vs.vt); err != nil {
			return nil, err
		}
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorResult) Infer(_ *VisibilitySolver, _ bool) ([]*TensorMeta, error) {
	// OperatorResult is the last node in the plan, so we don't need to infer its visibility
	return nil, nil
}

func (n *OperatorDataSource) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if err := n.InitVis(vs.vt, vs.originalVisibility); err != nil {
		return nil, err
	}
	for name, csr := range vs.srm.csrTable {
		applicableColNames, ok := vs.srm.sourceSecurityRelaxation[name]
		if !ok {
			continue
		}
		if err := n.InitCSR(csr, applicableColNames); err != nil {
			return nil, err
		}
	}
	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorRunSQL) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if n.subGraphTracker != nil {
		outerUnt := vs.consumerTracker
		vs.consumerTracker = n.subGraphTracker
		if err := vs.handleRunSQL(n); err != nil {
			return nil, err
		}
		vs.consumerTracker = outerUnt
	} else {
		for _, output := range n.outputs {
			vs.vt.UpdateVisibility(output, NewVisibleParties([]string{n.sourceParty}))
			// TODO: try vs.originalVisibility
		}
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorEQJoin) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	ovt := vs.NewOverlayVisibilityTable()

	if applySecurityRelaxation {
		csrRevealKeyAfterJoin := vs.srm.GetCSR(RevealKeyAfterJoin)
		if err := n.ApplyRevealKeyAfterJoin(ovt, csrRevealKeyAfterJoin); err != nil {
			return nil, err
		}
	}

	if err := n.InferVis(ovt); err != nil {
		return nil, err
	}

	vs.vt.UpdateTensorsVisibilityLike(ovt, GetNodeOutputs(n))

	for _, csr := range vs.srm.csrTable {
		if err := n.InferCSR(csr, ovt); err != nil {
			return nil, err
		}
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorCrossJoin) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorLimit) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorFilter) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorBroadcastTo) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorConcat) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorSort) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorReduce) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorGroupAgg) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorWindow) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	return inferStandard(vs, n)
}

func (n *OperatorIn) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if err := n.InferVis(vs.vt); err != nil {
		return nil, err
	}

	for _, csr := range vs.srm.csrTable {
		if err := n.InferCSR(csr, vs.vt); err != nil {
			return nil, err
		}
	}

	if applySecurityRelaxation {
		csrRevealFilterMask := vs.srm.GetCSR(RevealFilterMask)
		if err := n.ApplyRevealFilterMask(vs.vt, csrRevealFilterMask, vs.consumerTracker); err != nil {
			return nil, err
		}
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorFunction) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if err := n.InferVis(vs.vt); err != nil {
		return nil, err
	}
	for _, csr := range vs.srm.csrTable {
		if err := n.InferCSR(csr, vs.vt); err != nil {
			return nil, err
		}
	}

	// use relaxation in even in subPlan to handle selection push down
	csrRevealFilterMask := vs.srm.GetCSR(RevealFilterMask)
	if err := n.ApplyRevealFilterMask(vs.vt, csrRevealFilterMask, vs.consumerTracker); err != nil {
		return nil, err
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}

func (n *OperatorConstant) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	snapshot := vs.outputVisSnapshot(n)

	if err := n.InferVis(vs.vt); err != nil {
		return nil, err
	}

	return vs.getUpdatedTensors(n, snapshot), nil
}
