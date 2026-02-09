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

type TensorVisibilityChecker interface {
	IsTensorPublic(t *TensorMeta) bool
}

type ColumnSecurityRelaxation interface {
	// ApplicableTo returns true if the tensor is applicable to the column security relaxation
	ApplicableTo(t *TensorMeta, tvc TensorVisibilityChecker) bool

	// AllApplicable returns true if all the tensors are applicable to the column security relaxation
	AllApplicable(tensors []*TensorMeta, tvc TensorVisibilityChecker) bool

	// MakeApplicable makes the tensor applicable to the column security relaxation
	MakeApplicable(t *TensorMeta)
}

type ColumnSecurityRelaxationBase struct {
	withTensor  map[int]struct{} // tensors where this relaxation applies
	alwaysApply bool             // whether to always apply regardless of tensor
}

func NewColumnSecurityRelaxationBase(alwaysApply bool) *ColumnSecurityRelaxationBase {
	return &ColumnSecurityRelaxationBase{
		withTensor:  make(map[int]struct{}),
		alwaysApply: alwaysApply,
	}
}

// IsAlwaysApply returns whether the CSR always applies regardless of tensor state
func (csr *ColumnSecurityRelaxationBase) IsAlwaysApply() bool {
	return csr.alwaysApply
}

func (csr *ColumnSecurityRelaxationBase) ApplicableTo(t *TensorMeta, tvc TensorVisibilityChecker) bool {
	if t == nil {
		return false
	}
	if csr.alwaysApply {
		return true
	}
	if tvc != nil && tvc.IsTensorPublic(t) {
		return true
	}
	_, ok := csr.withTensor[t.ID]
	return ok
}

func (csr *ColumnSecurityRelaxationBase) MakeApplicable(t *TensorMeta) {
	csr.withTensor[t.ID] = struct{}{}
}

func (csr *ColumnSecurityRelaxationBase) AllApplicable(tensors []*TensorMeta, tvc TensorVisibilityChecker) bool {
	for _, t := range tensors {
		if !csr.ApplicableTo(t, tvc) {
			return false
		}
	}
	return true
}
