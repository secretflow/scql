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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartyInfos(t *testing.T) {
	r := require.New(t)
	partyMgr, err := NewFilePartyMgr("party_info_test.json")
	r.NoError(err)
	r.NotNil(partyMgr)

	// test GetUrlByParty
	aliceUrl, err := partyMgr.GetBrokerUrlByParty("alice")
	r.NoError(err)
	r.Equal("http://localhost:8081", aliceUrl)
	bobUrl, err := partyMgr.GetBrokerUrlByParty("bob")
	r.NoError(err)
	r.Equal("http://localhost:8082", bobUrl)
	carolUrl, err := partyMgr.GetBrokerUrlByParty("carol")
	r.NoError(err)
	r.Equal("http://localhost:8083", carolUrl)

	// test GetUrlByParty
	alicePubKey, err := partyMgr.GetPubKeyByParty("alice")
	r.NoError(err)
	r.Equal("fake_pub_key_alice", alicePubKey)
	bobPubKey, err := partyMgr.GetPubKeyByParty("bob")
	r.NoError(err)
	r.Equal("fake_pub_key_bob", bobPubKey)
	carolPubKey, err := partyMgr.GetPubKeyByParty("carol")
	r.NoError(err)
	r.Equal("fake_pub_key_carol", carolPubKey)
}
