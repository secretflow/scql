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

package auth

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/broker/testdata"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestAuth(t *testing.T) {
	r := require.New(t)
	err := testdata.CreateTestPemFiles("../../testdata")
	r.NoError(err)
	auth, err := NewAuth("../../testdata/private_key_alice.pem")
	r.NoError(err)

	interReq := &pb.InviteToProjectRequest{
		ClientId: &pb.PartyId{
			Code: "alice",
		},
		Inviter: "alice",
	}
	r.Equal(len(interReq.GetSignature()), 0)

	// add signature with private key
	err = auth.SignMessage(interReq)
	r.NoError(err)
	r.Greater(len(interReq.GetSignature()), 0)

	// check signature with public key(corresponding to the private key)
	pubKey := "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA="
	err = auth.CheckSign(interReq, pubKey)
	r.Greater(len(interReq.GetSignature()), 0)
	r.NoError(err)
}
