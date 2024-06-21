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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/planner/core"
)

func TestPartyInfos(t *testing.T) {
	r := require.New(t)
	partyInfo := NewPartyInfo([]*Participant{
		{
			PartyCode: "alice",
			Endpoints: []string{"alice.com"},
			Token:     "alice_credential",
		},
		{
			PartyCode: "bob",
			Endpoints: []string{"bob.com"},
			Token:     "bob_credential",
		},
	})
	r.Equal([]string{"alice", "bob"}, partyInfo.GetParties())
	r.Equal([]string{"alice.com", "bob.com"}, partyInfo.GetUrls())
	url, err := partyInfo.GetUrlByParty("alice")
	r.NoError(err)
	r.Equal("alice.com", url)
	url, err = partyInfo.GetUrlByParty("bob")
	r.NoError(err)
	r.Equal("bob.com", url)
	_, err = partyInfo.GetUrlByParty("jojo")
	r.Error(err)
}

func TestEnginesInfo(t *testing.T) {
	r := require.New(t)
	mockPartyA := &MockPartyAndTableInfos{
		party:     "alice",
		tables:    []string{"dba.ta", "dba.tb"},
		refTables: []string{"dbar.tar", "dbar.tbr"},
	}
	mockPartyB := &MockPartyAndTableInfos{
		party:     "bob",
		tables:    []string{"dbb.ta", "dbb.tb"},
		refTables: []string{"dbbr.tar", "dbbr.tbr"},
	}
	mockEnginesInfo, err := MockEnginesInfo([]*MockPartyAndTableInfos{mockPartyA, mockPartyB})
	r.NoError(err)
	r.Equal([]string{"alice", "bob"}, mockEnginesInfo.GetParties())
	url, err := mockEnginesInfo.GetUrlByParty("alice")
	r.NoError(err)
	r.Equal("alice.com", url)
	url, err = mockEnginesInfo.GetUrlByParty("bob")
	r.NoError(err)
	r.Equal("bob.com", url)
	tb, err := mockEnginesInfo.GetRefTableName("dba.ta")
	r.NoError(err)
	r.Equal(core.NewDbTable("dbar", "tar"), tb)
	tb, err = mockEnginesInfo.GetRefTableName("dbb.tb")
	r.NoError(err)
	r.Equal(core.NewDbTable("dbbr", "tbr"), tb)
	r.Equal(2, len(mockEnginesInfo.GetTablesByParty("alice")))
	r.Equal(2, len(mockEnginesInfo.GetTablesByParty("bob")))
	r.Equal("alice", mockEnginesInfo.GetPartyByTable(core.NewDbTable("dbar", "tar")))
	r.Equal("bob", mockEnginesInfo.GetPartyByTable(core.NewDbTable("dbbr", "tbr")))
}

type MockPartyAndTableInfos struct {
	party     string
	tables    []string
	refTables []string
}

func MockEnginesInfo(mockInfos []*MockPartyAndTableInfos) (*EnginesInfo, error) {
	if len(mockInfos) != 2 {
		return nil, fmt.Errorf("unsupported party number %d", len(mockInfos))
	}
	var participants []*Participant
	for _, info := range mockInfos {
		participants = append(participants, &Participant{
			PartyCode: info.party,
			Endpoints: []string{fmt.Sprintf("%s.com", info.party)},
			Token:     fmt.Sprintf("%s_credential", info.party),
		})
	}
	partyInfo := NewPartyInfo(participants)
	tableNum := len(mockInfos[0].tables)
	party2Tables := make(map[string][]core.DbTable)
	mockRefTables := make([]core.DbTable, 0)
	mockTables := make([]core.DbTable, 0)
	for _, info := range mockInfos {
		for _, qualifiedTable := range info.refTables {
			dt, err := core.NewDbTableFromString(qualifiedTable)
			if err != nil {
				return nil, err
			}
			mockRefTables = append(mockRefTables, dt)
		}
		for _, refQualifiedTable := range info.tables {
			dt, err := core.NewDbTableFromString(refQualifiedTable)
			if err != nil {
				return nil, err
			}
			mockTables = append(mockTables, dt)
		}
	}
	party2Tables[mockInfos[0].party] = mockRefTables[:tableNum]
	party2Tables[mockInfos[1].party] = mockRefTables[tableNum:]
	mockEnginesInfo := NewEnginesInfo(partyInfo, party2Tables)
	tableToRefs := make(map[core.DbTable]core.DbTable)
	for i, tbl := range mockRefTables {
		tableToRefs[mockTables[i]] = tbl
	}

	mockEnginesInfo.UpdateTableToRefs(tableToRefs)
	return mockEnginesInfo, nil
}
