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

	"github.com/secretflow/scql/pkg/broker/testdata"
)

func TestPartyInfos(t *testing.T) {
	r := require.New(t)
	err := testdata.CreateTestPemFiles("../testdata")
	r.NoError(err)
	partyMgr, err := NewFilePartyMgr("../testdata/party_info_test.json", "alice")
	r.NoError(err)
	r.NotNil(partyMgr)

	// test GetUrlByParty
	aliceUrl, err := partyMgr.GetBrokerUrlByParty("alice")
	r.NoError(err)
	r.Equal("http://localhost:8082", aliceUrl)
	bobUrl, err := partyMgr.GetBrokerUrlByParty("bob")
	r.NoError(err)
	r.Equal("http://localhost:8084", bobUrl)
	carolUrl, err := partyMgr.GetBrokerUrlByParty("carol")
	r.NoError(err)
	r.Equal("http://localhost:8086", carolUrl)

	// test GetUrlByParty
	alicePubKey, err := partyMgr.GetPubKeyByParty("alice")
	r.NoError(err)
	r.Equal("MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA=", alicePubKey)
	bobPubKey, err := partyMgr.GetPubKeyByParty("bob")
	r.NoError(err)
	r.Equal("MCowBQYDK2VwAyEAN3w+v2uks/QEaVZiprZ8oRChMkBOZJSAl6V/5LvOnt4=", bobPubKey)
	carolPubKey, err := partyMgr.GetPubKeyByParty("carol")
	r.NoError(err)
	r.Equal("MCowBQYDK2VwAyEANhiAXTvL4x2jYUiAbQRo9XuOTrFFnAX4Q+YlEAgULs8=", carolPubKey)
}
