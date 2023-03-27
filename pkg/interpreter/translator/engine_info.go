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

import "fmt"

type EnginesInfo struct {
	partyInfo     *PartyInfo
	partyToTables map[string][]DbTable
	tableToParty  map[DbTable]string
	tableToRefs   map[DbTable]DbTable
}

func (h *EnginesInfo) GetPartyInfo() *PartyInfo {
	return h.partyInfo
}

func (h *EnginesInfo) GetParties() []string {
	return h.partyInfo.GetParties()
}

func (h *EnginesInfo) GetUrlByParty(party string) (string, error) {
	return h.partyInfo.GetUrlByParty(party)
}

func (h *EnginesInfo) GetCredentialByParty(party string) (string, error) {
	return h.partyInfo.GetCredentialByParty(party)
}

func (h *EnginesInfo) GetTablesByParty(party string) []DbTable {
	// if party don't exist in partyInfo just return nil slice
	return h.partyToTables[party]
}

func (h *EnginesInfo) GetPartyByTable(t DbTable) string {
	return h.tableToParty[t]
}

func (h *EnginesInfo) GetRefTableName(tableName string) (DbTable, error) {
	dt, err := newDbTable(tableName)
	if err != nil {
		return DbTable{}, err
	}
	return h.tableToRefs[dt], nil
}

func (h *EnginesInfo) GetDBTableMap() map[DbTable]DbTable {
	return h.tableToRefs
}

func (h *EnginesInfo) String() string {
	return fmt.Sprintf("engine infos party info: %+v, tables: %+v", h.partyInfo, h.partyToTables)
}

func (h *EnginesInfo) UpdateTableToRefs(tableToRefs map[DbTable]DbTable) {
	for table, ref := range tableToRefs {
		h.tableToRefs[table] = ref
	}
}

func NewEnginesInfo(p *PartyInfo, party2Tables map[string][]DbTable) *EnginesInfo {
	table2Party := make(map[DbTable]string)
	for p, tables := range party2Tables {
		for _, dt := range tables {
			table2Party[dt] = p
		}
	}
	return &EnginesInfo{
		partyInfo:     p,
		partyToTables: party2Tables,
		tableToParty:  table2Party,
		tableToRefs:   make(map[DbTable]DbTable),
	}
}

type PartyInfo struct {
	parties     []string
	urls        []string
	credentials []string
}

func NewPartyInfo(parties []string, urls []string, credentials []string) (*PartyInfo, error) {
	if len(parties) == 0 || len(parties) != len(urls) || len(parties) != len(credentials) {
		return nil, fmt.Errorf("invalid party information")
	}
	return &PartyInfo{
		parties:     parties,
		urls:        urls,
		credentials: credentials,
	}, nil
}

func (pi *PartyInfo) GetParties() []string {
	return pi.parties
}

func (pi *PartyInfo) GetCredentialByParty(party string) (credential string, err error) {
	for i, p := range pi.parties {
		if p == party {
			return pi.credentials[i], nil
		}
	}
	return "", fmt.Errorf("no party named %s, all parties include (%+v)", party, pi.parties)
}

func (pi *PartyInfo) GetCredentials() []string {
	return pi.credentials
}

func (pi *PartyInfo) GetUrlByParty(party string) (url string, err error) {
	for i, p := range pi.parties {
		if p == party {
			return pi.urls[i], nil
		}
	}
	return "", fmt.Errorf("no party named %s, all parties include (%+v)", party, pi.parties)
}

func (pi *PartyInfo) GetUrls() []string {
	return pi.urls
}

func (pi *PartyInfo) UpdateUrls(urls []string) {
	pi.urls = urls
}
