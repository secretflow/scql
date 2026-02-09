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
	"sort"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// OperatorGraphPass builds operator graph from logical plan
type OperatorGraphPass struct{}

// NewOperatorGraphPass creates a new operator graph pass
func NewOperatorGraphPass() *OperatorGraphPass {
	return &OperatorGraphPass{}
}

// Name returns the pass name
func (p *OperatorGraphPass) Name() string {
	return "OperatorGraphPass"
}

// Run builds operator graph and related components
func (p *OperatorGraphPass) Run(c *CompileContext) error {
	// 1. Build engines info from catalog
	// TODO: support issuerAsParticipant
	enginesInfo, err := buildEnginesInfoFromCatalog(
		c.LogicalPlan,
		c.Request.GetCatalog(),
		c.Request.GetDb(),
		c.Request.GetIssuer().GetCode(),
		true,
	)
	if err != nil {
		return fmt.Errorf("failed to build engines info: %v", err)
	}
	c.EnginesInfo = enginesInfo

	// 2. Create tensor meta manager
	tensorMetaManager := NewTensorMetaManager()
	c.TensorMetaManager = tensorMetaManager

	// 3. Build operator graph
	operatorGraphBuilder := NewOperatorGraphBuilder(
		tensorMetaManager,
		enginesInfo,
		c.Request.GetIssuer().GetCode(),
		c.Request.GetIssueTime().AsTime(),
	)
	operatorGraph, err := operatorGraphBuilder.Build(c.LogicalPlan)
	if err != nil {
		return fmt.Errorf("failed to build operator graph: %v", err)
	}
	logrus.Debugf("operator graph: %s", operatorGraph)
	c.OperatorGraph = operatorGraph

	// 4. Get and store involved parties (to avoid re-creating builder later)
	involvedParties, err := operatorGraphBuilder.GetInvolvedParties(c.LogicalPlan)
	if err != nil {
		return fmt.Errorf("failed to get involved parties: %v", err)
	}
	c.InvolvedParties = involvedParties

	// TODO: Add operator graph optimizer here in future

	return nil
}

// collectDataSourceNodes recursively collects all DataSource nodes from a logical plan
func collectDataSourceNodes(lp core.LogicalPlan) []*core.DataSource {
	var result []*core.DataSource
	for _, child := range lp.Children() {
		dsList := collectDataSourceNodes(child)
		result = append(result, dsList...)
	}

	if len(lp.Children()) > 0 {
		return result
	}

	if ds, ok := lp.(*core.DataSource); ok {
		return []*core.DataSource{ds}
	}
	return nil
}

// buildEnginesInfoFromCatalog builds EnginesInfo from catalog and logical plan
func buildEnginesInfoFromCatalog(lp core.LogicalPlan, catalog *pb.Catalog, currentDb string, queryIssuer string, issuerAsParticipant bool) (*graph.EnginesInfo, error) {
	// Construct catalog map
	catalogMap := make(map[string]*pb.TableEntry)
	for _, table := range catalog.GetTables() {
		tn := table.GetTableName()
		if _, exists := catalogMap[tn]; exists {
			return nil, fmt.Errorf("duplicate table exists in catalog")
		}
		catalogMap[tn] = table
	}

	party2Tables := make(map[string][]core.DbTable)
	tableToRefs := make(map[core.DbTable]core.DbTable)

	// Collect data source nodes
	dsList := collectDataSourceNodes(lp)
	if len(dsList) == 0 {
		return nil, fmt.Errorf("no data source in query")
	}

	for _, ds := range dsList {
		dbName := ds.DBName.String()
		tblName := ds.TableInfo().Name.String()

		if len(dbName) == 0 {
			dbName = currentDb
		}
		dbTable := core.NewDbTable(dbName, tblName)
		tn := dbTable.String()

		tblEntry, exists := catalogMap[tn]
		if !exists {
			return nil, fmt.Errorf("table `%s` not found in catalog", tn)
		}

		tblOwner := tblEntry.GetOwner().GetCode()
		party2Tables[tblOwner] = append(party2Tables[tblOwner], dbTable)

		refTblName := tblEntry.GetRefTable()
		if len(refTblName) == 0 {
			refTblName = tblEntry.GetTableName()
		}
		refDbTable, err := core.NewDbTableFromString(refTblName)
		if err != nil {
			return nil, fmt.Errorf("failed to create DbTable from %s: %+v", tblEntry.GetRefTable(), err)
		}

		err = refDbTable.SetDBTypeFromString(tblEntry.GetDbType(), tblEntry.TableName)
		if err != nil {
			return nil, fmt.Errorf("buildEnginesInfoFromCatalog: %v", err)
		}

		tableToRefs[dbTable] = refDbTable
	}

	parties := make([]*graph.Participant, 0)
	for party := range party2Tables {
		parties = append(parties, &graph.Participant{
			PartyCode: party,
		})
	}

	// Add query issuer if not already present and should participate
	if _, exists := party2Tables[queryIssuer]; !exists {
		if issuerAsParticipant {
			parties = append(parties, &graph.Participant{
				PartyCode: queryIssuer,
			})
		}
	}

	// Handle INTO clause party codes
	var intoPartyCodes []string
	if lp.IntoOpt() != nil {
		for _, partyFile := range lp.IntoOpt().Opt.PartyFiles {
			if partyFile.PartyCode == "" {
				intoPartyCodes = append(intoPartyCodes, queryIssuer)
			} else {
				intoPartyCodes = append(intoPartyCodes, partyFile.PartyCode)
			}
		}
	}

	// Add into party codes if specified
	if len(intoPartyCodes) > 0 {
		for _, partyCode := range intoPartyCodes {
			parties = append(parties, &graph.Participant{
				PartyCode: partyCode,
			})
		}
	}

	// Sort parties for deterministic behavior
	sort.Slice(parties, func(i, j int) bool {
		return parties[i].PartyCode < parties[j].PartyCode
	})
	parties = slices.CompactFunc(parties, func(i, j *graph.Participant) bool {
		return i.PartyCode == j.PartyCode
	})

	partyInfo := graph.NewPartyInfo(parties)
	engineInfo := graph.NewEnginesInfo(partyInfo, party2Tables)
	engineInfo.UpdateTableToRefs(tableToRefs)

	return engineInfo, nil
}
