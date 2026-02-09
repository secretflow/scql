// Copyright 2026 Ant Group Co., Ltd.
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

	"github.com/sirupsen/logrus"

	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
)

// VisibilityAnalysisPass handles visibility analysis and computation
type VisibilityAnalysisPass struct{}

// NewVisibilityAnalysisPass creates a new visibility analysis pass
func NewVisibilityAnalysisPass() *VisibilityAnalysisPass {
	return &VisibilityAnalysisPass{}
}

// Name returns the pass name
func (p *VisibilityAnalysisPass) Name() string {
	return "VisibilityAnalysisPass"
}

// Run performs visibility analysis and computation
func (p *VisibilityAnalysisPass) Run(c *CompileContext) error {
	// 1. Ensure involved parties are available
	// They should have been set in OperatorGraphPass
	if len(c.InvolvedParties) == 0 {
		return fmt.Errorf("involved parties not found - ensure OperatorGraphPass runs before VisibilityAnalysisPass")
	}
	logrus.Debugf("involved parties: %v", c.InvolvedParties)

	// 2. Build visibility table and security relaxation manager
	vt := NewVisibilityTable(c.InvolvedParties)
	globalSecurityRelaxation := convertGlobalSecurityRelaxation(c.Request.GetSecurityConfig().GetGlobalRelaxation())
	sourceSecurityRelaxation := convertColumnSecurityRelaxation(c.Request.GetSecurityConfig().GetColumnRelaxationList())
	srm := NewSecurityRelaxationManager(globalSecurityRelaxation, sourceSecurityRelaxation)
	c.VisibilityTable = vt
	c.SecurityRelaxationManager = srm

	// 3. Create specified visibility from column visibility list
	// TODO: support specified column visibility
	specifiedVis := map[int]*VisibleParties{}

	// 4. Get original visibility from column visibility list
	originalVis, err := buildOriginalVisibility(c.Request.GetSecurityConfig().GetColumnVisibilityList())
	if err != nil {
		return fmt.Errorf("failed to build original visibility: %v", err)
	}

	// 5. Solve visibility
	enableReverseInference := c.Request.GetSecurityConfig().GetReverseInferenceConf().GetEnableReverseInference()
	visibilitySolver := NewVisibilitySolver(vt, srm, originalVis, specifiedVis, enableReverseInference)
	logrus.Debugf("%v", visibilitySolver.originalVisibility)
	err = visibilitySolver.Solve(c.OperatorGraph)
	if err != nil {
		return fmt.Errorf("failed to solve visibility: %v", err)
	}

	logrus.Debugf("TensorMetas: %s", c.TensorMetaManager.DumpTensorMetas(vt))

	return nil
}

// Helper functions moved from compiler.go

func convertGlobalSecurityRelaxation(global *v1.GlobalSecurityRelaxation) *GlobalSecurityRelaxation {
	if global == nil {
		return &GlobalSecurityRelaxation{}
	}

	return &GlobalSecurityRelaxation{
		RevealGroupCount:   global.GetRevealGroupCount(),
		RevealGroupMark:    global.GetRevealGroupMark(),
		RevealKeyAfterJoin: global.GetRevealKeyAfterJoin(),
		RevealFilterMask:   global.GetRevealFilterMask(),
	}
}

func convertColumnSecurityRelaxation(relaxationList []*v1.ColumnSecurityRelaxation) map[string][]string {
	sourceSecurityRelaxation := make(map[string][]string)

	for _, relaxation := range relaxationList {
		fullName := strings.ToLower(toFullQualifiedColumnName(
			relaxation.GetDatabase(),
			relaxation.GetTable(),
			relaxation.GetColumn(),
		))

		if relaxation.GetRevealKeyAfterJoin() {
			sourceSecurityRelaxation[RevealKeyAfterJoin] = append(sourceSecurityRelaxation[RevealKeyAfterJoin], fullName)
		}

		if relaxation.GetRevealFilterMask() {
			sourceSecurityRelaxation[RevealFilterMask] = append(sourceSecurityRelaxation[RevealFilterMask], fullName)
		}
	}

	return sourceSecurityRelaxation
}

func buildOriginalVisibility(columnVisibilities []*v1.ColumnVisibility) (map[string]*VisibleParties, error) {
	originalVis := make(map[string]*VisibleParties)

	// Build a map from column full name to visible parties
	for _, colVis := range columnVisibilities {
		fullName := strings.ToLower(toFullQualifiedColumnName(
			colVis.GetDatabase(),
			colVis.GetTable(),
			colVis.GetColumn(),
		))
		if _, exist := originalVis[fullName]; exist {
			return nil, fmt.Errorf("column %s is duplicated in column visibility list", fullName)
		}
		parties := make([]string, 0, len(colVis.GetVisibleParties()))
		for _, party := range colVis.GetVisibleParties() {
			parties = append(parties, party.GetCode())
		}
		originalVis[fullName] = NewVisibleParties(parties)
	}

	return originalVis, nil
}

func toFullQualifiedColumnName(dbName, tblName, colName string) string {
	if len(dbName) == 0 && len(tblName) == 0 {
		return colName
	}
	if len(dbName) == 0 {
		return fmt.Sprintf("%s.%s", tblName, colName)
	}
	return fmt.Sprintf("%s.%s.%s", dbName, tblName, colName)
}
