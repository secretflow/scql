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

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
)

// ResultTable maps column IDs to their corresponding tensor metas for logical plan results.
type ResultTable map[int64]*TensorMeta

func (rt ResultTable) getColumnTensorMeta(col *expression.Column) (*TensorMeta, error) {
	tensor, ok := rt[col.UniqueID]
	if !ok {
		return nil, fmt.Errorf("getColumnTensorMeta: unable to find columnID %v", col.UniqueID)
	}
	return tensor, nil
}

// TensorMetaManager manages the lifecycle of all tensor and maintain the mapping between logical plans and their result tensors.
type TensorMetaManager struct {
	tensors         []*TensorMeta
	tensorNum       int
	lpResultTensors map[core.LogicalPlan]ResultTable
}

func NewTensorMetaManager() *TensorMetaManager {
	tmm := &TensorMetaManager{
		tensors:         []*TensorMeta{},
		tensorNum:       0,
		lpResultTensors: map[core.LogicalPlan]ResultTable{},
	}
	return tmm
}

// ============================================================================
// TensorMeta Management
// ============================================================================

func (tmm *TensorMetaManager) Tensors() []*TensorMeta {
	return tmm.tensors
}

func (tmm *TensorMetaManager) newTensorMeta() *TensorMeta {
	tmm.tensorNum++
	t := &TensorMeta{
		ID: tmm.tensorNum,
	}
	tmm.tensors = append(tmm.tensors, t)
	return t
}

func (tmm *TensorMetaManager) CreateTensorMeta(name string, dType *graph.DataType) *TensorMeta {
	t := tmm.newTensorMeta()

	t.Name = name
	t.DType = dType

	return t
}

func (tmm *TensorMetaManager) CreateTensorMetaAs(ref *TensorMeta) *TensorMeta {
	return tmm.CreateTensorMeta(ref.Name, ref.DType)
}

func areTensorMetasSimilar(t1, t2 *TensorMeta) bool {
	return t1.IsConstScalar == t2.IsConstScalar && t1.DType.Equal(t2.DType)
}

// FIXME: this is a hack for OperatorWindow, where private and secret kernels have different output behaviors.
// OperatorWindow will not create new payloads tensor when the private kernel is used.
func (tmm *TensorMetaManager) setTensorsEquivalent(mainTensor, replicaTensor *TensorMeta) error {
	if !areTensorMetasSimilar(mainTensor, replicaTensor) {
		return fmt.Errorf("setTensorsEquivalent: tensor metas %v and %v need to be similar", mainTensor, replicaTensor)
	}
	replicaTensor.ID = mainTensor.ID
	return nil
}

// ============================================================================
// LogicalPlan ResultTable Management
// ============================================================================

// checkLPResultTable validates that result tensor metas match the logical plan's schema.
// Returns an error if any column is missing or has mismatched data types.
func (tmm *TensorMetaManager) checkLPResultTable(lp core.LogicalPlan, resultTable ResultTable) error {
	for _, column := range lp.Schema().Columns {
		tensorMeta, ok := resultTable[column.UniqueID]
		if !ok {
			return fmt.Errorf("checkLPResultTable: result tensors%v does not contain column %v", resultTable, column)
		}
		expectTp, err := graph.ConvertDataType(column.RetType)
		if err != nil {
			return fmt.Errorf("checkLPResultTable: %v", err)
		}
		if !tensorMeta.DType.Equal(expectTp) {
			return fmt.Errorf("checkLPResultTable: tensor %v type(=%v) doesn't match scheme type(=%v)", tensorMeta.UniqueName(), tensorMeta.DType, expectTp)
		}
	}
	return nil
}

// setLPResultTable stores result tensor metas for a logical plan.
// The flag checkType is provided, so we can skip the type check if necessary.
// This is important because we can not do type check for single party OperatorGraph, which may contains unknown function.
func (tmm *TensorMetaManager) setLPResultTable(lp core.LogicalPlan, resultTable ResultTable, checkType bool) error {
	if _, exist := tmm.lpResultTensors[lp]; exist {
		return fmt.Errorf("setLPResultTable: logical plan %v has already set result tensors", lp)
	}
	if checkType {
		if err := tmm.checkLPResultTable(lp, resultTable); err != nil {
			return fmt.Errorf("setLPResultTable: %v", err)
		}
	}

	tmm.lpResultTensors[lp] = resultTable
	return nil
}

func (tmm *TensorMetaManager) getLPResultTable(lp core.LogicalPlan) (ResultTable, error) {
	if _, exist := tmm.lpResultTensors[lp]; !exist {
		return nil, fmt.Errorf("getLPResultTable: logical plan %v has not set result tensors", lp)
	}
	return tmm.lpResultTensors[lp], nil
}

// getLPResultTensors returns result tensors with the same order as logical plan's schema.
func (tmm *TensorMetaManager) getLPSchemaTensors(lp core.LogicalPlan) ([]*TensorMeta, error) {
	resultTable, err := tmm.getLPResultTable(lp)
	if err != nil {
		return nil, fmt.Errorf("getLPResultTensors: %v", err)
	}

	results := make([]*TensorMeta, 0, len(lp.Schema().Columns))
	for _, col := range lp.Schema().Columns {
		tensor, ok := resultTable[col.UniqueID]
		if !ok {
			return nil, fmt.Errorf("getLPSchemaTensors: unable to find columnID %v", col.UniqueID)
		}
		results = append(results, tensor)
	}
	return results, nil
}

func (tmm *TensorMetaManager) getLPColumnTensor(lp core.LogicalPlan, col *expression.Column) (*TensorMeta, error) {
	resultTable, err := tmm.getLPResultTable(lp)
	if err != nil {
		return nil, fmt.Errorf("getLPColumnTensor: %v", err)
	}
	return resultTable.getColumnTensorMeta(col)
}

// DumpTensorMetas returns a string representation of all tensors managed by this TensorMetaManager.
// If visibilityRegistry is not nil, includes visible parties information for each tensor.
func (tmm *TensorMetaManager) DumpTensorMetas(visibilityRegistry VisibilityRegistry) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TensorMetaManager Dump:\n")
	fmt.Fprintf(&sb, "Total TensorMetas: %d\n", len(tmm.tensors))

	for _, tensor := range tmm.tensors {
		fmt.Fprintf(&sb, "Tensor[%d]: %s, Type: %v", tensor.ID, tensor.Name, tensor.DType)
		if tensor.IsConstScalar {
			fmt.Fprintf(&sb, ", ConstScalar: true")
		}

		if visibilityRegistry != nil {
			visibleParties := visibilityRegistry.TensorVisibleParties(tensor)
			if visibleParties != nil && !visibleParties.IsEmpty() {
				fmt.Fprintf(&sb, ", VisibleParties: [%s]", visibleParties.String())
			} else {
				fmt.Fprintf(&sb, ", VisibleParties: []")
			}
		}

		fmt.Fprintf(&sb, "\n")
	}

	return sb.String()
}
