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

package graph

import (
	"fmt"

	"github.com/secretflow/scql/pkg/planner/core"
)

type EnginesInfo struct {
	partyInfo     *PartyInfo
	partyToTables map[string][]core.DbTable
	tableToParty  map[core.DbTable]string
	tableToRefs   map[core.DbTable]core.DbTable
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

func (h *EnginesInfo) GetTablesByParty(party string) []core.DbTable {
	// if party don't exist in partyInfo just return nil slice
	return h.partyToTables[party]
}

func (h *EnginesInfo) GetPartyByTable(t core.DbTable) string {
	return h.tableToParty[t]
}

func (h *EnginesInfo) GetRefTableName(tableName string) (core.DbTable, error) {
	dt, err := core.NewDbTableFromString(tableName)
	if err != nil {
		return core.DbTable{}, err
	}
	return h.tableToRefs[dt], nil
}

func (h *EnginesInfo) GetDbTableMap() map[core.DbTable]core.DbTable {
	return h.tableToRefs
}

func (h *EnginesInfo) String() string {
	return fmt.Sprintf("engine infos party info: %+v, tables: %+v", h.partyInfo, h.partyToTables)
}

func (h *EnginesInfo) UpdateTableToRefs(tableToRefs map[core.DbTable]core.DbTable) {
	for table, ref := range tableToRefs {
		h.tableToRefs[table] = ref
	}
}

func NewEnginesInfo(p *PartyInfo, party2Tables map[string][]core.DbTable) *EnginesInfo {
	table2Party := make(map[core.DbTable]string)
	for p, tables := range party2Tables {
		for _, dt := range tables {
			table2Party[dt] = p
		}
	}
	return &EnginesInfo{
		partyInfo:     p,
		partyToTables: party2Tables,
		tableToParty:  table2Party,
		tableToRefs:   make(map[core.DbTable]core.DbTable),
	}
}

type Participant struct {
	PartyCode string
	Endpoints []string
	Token     string
	PubKey    string
}

type PartyInfo struct {
	participants []*Participant
}

func NewPartyInfo(parties []*Participant) *PartyInfo {
	return &PartyInfo{
		participants: parties,
	}
}

func (p *PartyInfo) GetParticipants() []*Participant {
	return p.participants
}

func (pi *PartyInfo) GetParties() []string {
	partyCodes := make([]string, 0, len(pi.participants))
	for _, p := range pi.participants {
		partyCodes = append(partyCodes, p.PartyCode)
	}
	return partyCodes
}

func (pi *PartyInfo) GetCredentialByParty(party string) (credential string, err error) {
	for _, p := range pi.participants {
		if p.PartyCode == party {
			return p.Token, nil
		}
	}
	return "", fmt.Errorf("no party named %s", party)
}

func (pi *PartyInfo) GetCredentials() []string {
	credentials := make([]string, 0, len(pi.participants))
	for _, p := range pi.participants {
		credentials = append(credentials, p.Token)
	}
	return credentials
}

func (pi *PartyInfo) GetUrlByParty(party string) (url string, err error) {
	for _, p := range pi.participants {
		if p.PartyCode == party {
			return p.Endpoints[0], nil
		}
	}
	return "", fmt.Errorf("no party named %s", party)
}

func (pi *PartyInfo) GetUrls() []string {
	urls := make([]string, 0, len(pi.participants))
	for _, p := range pi.participants {
		urls = append(urls, p.Endpoints[0])
	}
	return urls
}
