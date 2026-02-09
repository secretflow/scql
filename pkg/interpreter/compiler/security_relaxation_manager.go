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

type SecurityRelaxationManager struct {
	global                   *GlobalSecurityRelaxation                // global relaxation settings
	csrTable                 map[string]*ColumnSecurityRelaxationBase // column-level relaxation policies
	sourceSecurityRelaxation map[string][]string                      // relaxation name -> list of column full names; used during inference to initialize CSR for specific columns
}

func NewSecurityRelaxationManager(global *GlobalSecurityRelaxation, sourceSecurityRelaxation map[string][]string) *SecurityRelaxationManager {
	csrTable := make(map[string]*ColumnSecurityRelaxationBase)
	csrTable[RevealKeyAfterJoin] = NewColumnSecurityRelaxationBase(global.RevealKeyAfterJoin)
	csrTable[RevealFilterMask] = NewColumnSecurityRelaxationBase(global.RevealFilterMask)

	return &SecurityRelaxationManager{
		global:                   global,
		csrTable:                 csrTable,
		sourceSecurityRelaxation: sourceSecurityRelaxation,
	}
}

type GlobalSecurityRelaxation struct {
	RevealFilterMask   bool // reveal tensor when used as filter mask, this relaxation affects tensor visibility
	RevealKeyAfterJoin bool // reveal join keys after joining, this relaxation affects tensor visibility
	RevealGroupCount   bool // reveal group aggregation counts, this relaxation does not affect tensor visibility, but may affect behavior in KernelObliviousGroupAgg
	RevealGroupMark    bool // reveal group aggregation markers, this relaxation does not affect tensor visibility, but may affect behavior in KernelObliviousGroupAgg
}

// GetCSR returns the ColumnSecurityRelaxationBase for the given name
func (srm *SecurityRelaxationManager) GetCSR(name string) *ColumnSecurityRelaxationBase {
	return srm.csrTable[name]
}
