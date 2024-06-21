// Copyright 2023 Ant Group Co., Ltd.
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

package translator

import (
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func ConvertMockEnginesToEnginesInfo(info *mock.MockEnginesInfo) (*graph.EnginesInfo, error) {
	participants := make([]*graph.Participant, 0, len(info.PartyToUrls))
	for _, code := range sliceutil.SortMapKeyForDeterminism(info.PartyToUrls) {
		participants = append(participants, &graph.Participant{
			PartyCode: code,
			Endpoints: []string{info.PartyToUrls[code]},
			Token:     info.PartyToCredentials[code],
		})
	}
	partyToTables := make(map[string][]core.DbTable)
	tableToRefs := make(map[core.DbTable]core.DbTable)
	for p, tables := range info.PartyToTables {
		var dbTables []core.DbTable
		for _, t := range tables {
			dt, err := core.NewDbTableFromString(t)
			if err != nil {
				return nil, err
			}
			dbTables = append(dbTables, dt)
		}
		partyToTables[p] = dbTables
	}

	engineInfo := graph.NewEnginesInfo(graph.NewPartyInfo(participants), partyToTables)
	for table, refTable := range info.TableToRefs {
		ref, err := core.NewDbTableFromString(refTable)
		if err != nil {
			return nil, err
		}
		tbl, err := core.NewDbTableFromString(table)
		if err != nil {
			return nil, err
		}
		tableToRefs[tbl] = ref
	}
	engineInfo.UpdateTableToRefs(tableToRefs)
	return engineInfo, nil
}
