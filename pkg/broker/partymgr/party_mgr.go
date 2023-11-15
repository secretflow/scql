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

package partymgr

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/secretflow/scql/pkg/interpreter/translator"
)

var (
	_ PartyMgr = &FilePartyMgr{}
)

type PartyMgr interface {
	GetBrokerUrlByParty(party string) (string, error)
	GetPubKeyByParty(party string) (string, error)
	GetPartyInfoByParties(parties []string) (*translator.PartyInfo, error)
	GetSelfInfo() *SelfInfo
}

type SelfInfo struct {
	Endpoints []string
	Code      string
}

func (info *SelfInfo) GetEndpoint() string {
	// TODO: support load balance
	return info.Endpoints[0]
}

// TODO: renamed to avoid confusion with engine partyInfo structure
type Participant struct {
	PartyCode string `json:"party_code"`
	Endpoint  string `json:"endpoint"`
	Token     string `json:"token"`
	PubKey    string `json:"pubkey"`
}

type BrokerInfo struct {
	Participants []*Participant `json:"participants"`
}

type FilePartyMgr struct {
	urlMap    map[string]string
	pubKeyMap map[string]string
	self      *SelfInfo
}

func NewFilePartyMgr(partyPath string, selfCode string, selfEngineUrls []string) (PartyMgr, error) {
	content, err := os.ReadFile(partyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %+v", partyPath, err)
	}

	var partyInfo BrokerInfo
	err = json.Unmarshal(content, &partyInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal from json: %v", err)
	}
	if len(selfEngineUrls) == 0 {
		return nil, fmt.Errorf("failed to find any engine urls")
	}
	selfPartInfo := &SelfInfo{
		Endpoints: selfEngineUrls,
		Code:      selfCode,
	}
	filePartyMgr := FilePartyMgr{
		urlMap:    map[string]string{},
		pubKeyMap: map[string]string{},
		self:      selfPartInfo,
	}
	for _, party := range partyInfo.Participants {
		filePartyMgr.urlMap[party.PartyCode] = party.Endpoint
		filePartyMgr.pubKeyMap[party.PartyCode] = party.PubKey
	}

	return &filePartyMgr, nil

}

func (m *FilePartyMgr) GetBrokerUrlByParty(party string) (string, error) {
	url, ok := m.urlMap[party]
	if !ok {
		return "", fmt.Errorf("GetBrokerUrlByParty: no url for party: %v", party)
	}
	return url, nil
}

func (m *FilePartyMgr) GetPubKeyByParty(party string) (string, error) {
	pubKey, ok := m.pubKeyMap[party]
	if !ok {
		return "", fmt.Errorf("GetPubKeyByParty: no pubKey for party: %v", party)
	}
	return pubKey, nil
}

func (m *FilePartyMgr) GetPartyInfoByParties(parties []string) (*translator.PartyInfo, error) {
	var participants []*translator.Participant
	for _, party := range parties {
		participant := &translator.Participant{PartyCode: party, Endpoints: []string{}}
		// find pub key
		pubKey, exist := m.pubKeyMap[party]
		if !exist {
			return nil, fmt.Errorf("GetPartyInfoByParties: failed to find pub key for %s", party)
		}
		participant.PubKey = pubKey
		participants = append(participants, participant)
	}
	return translator.NewPartyInfo(participants), nil
}

func (m *FilePartyMgr) GetSelfInfo() *SelfInfo {
	return m.self
}
