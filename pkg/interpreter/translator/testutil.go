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
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func ConvertMockEnginesToEnginesInfo(info *mock.MockEnginesInfo) (*EnginesInfo, error) {
	var parties []string
	var urls []string
	var credentials []string
	for _, p := range sliceutil.SortMapKeyForDeterminism(info.PartyToUrls) {
		url := info.PartyToUrls[p]
		credential := info.PartyToCredentials[p]
		parties = append(parties, p)
		urls = append(urls, url)
		credentials = append(credentials, credential)
	}
	partyToTables := make(map[string][]DbTable)
	tableToRefs := make(map[DbTable]DbTable)
	for p, tables := range info.PartyToTables {
		var dbTables []DbTable
		for _, t := range tables {
			dt, err := newDbTable(t)
			if err != nil {
				return nil, err
			}
			dbTables = append(dbTables, dt)
		}
		partyToTables[p] = dbTables
	}
	pi := &PartyInfo{
		parties:     parties,
		urls:        urls,
		credentials: credentials,
	}
	engineInfo := NewEnginesInfo(pi, partyToTables)
	for table, refTable := range info.TableToRefs {
		ref, err := newDbTable(refTable)
		if err != nil {
			return nil, err
		}
		tbl, err := newDbTable(table)
		if err != nil {
			return nil, err
		}
		tableToRefs[tbl] = ref
	}
	engineInfo.UpdateTableToRefs(tableToRefs)
	return engineInfo, nil
}
