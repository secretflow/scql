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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestAuth(t *testing.T) {
	r := require.New(t)
	for i, tc := range authTestCases {
		errMsg := fmt.Sprintf("#%d case failed", i)
		auth, err := NewPemAuth([]byte(tc.privateKey))
		r.NoError(err, errMsg)

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
		err = auth.CheckSign(interReq, tc.publicKey)
		r.Greater(len(interReq.GetSignature()), 0)
		r.NoError(err)
	}
}

type authTestCase struct {
	// PEM-encoded private key
	privateKey string
	publicKey  string
}

var authTestCases = []authTestCase{
	{
		// ed25519 key pair
		privateKey: `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEICh+ZViILyFPq658OPYq6iKlSj802q1LfrmrV2i1GWcn
-----END PRIVATE KEY-----
`,
		publicKey: "MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA=",
	},
	{
		privateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyC8ejfGt+In7+0E2y3qj0SP9CFrXsQGRV6GFYabxPHFtt8bY
YtyH2CVJdps9tPbwVNbGxSqApekjPgbL+16JlapX9L2K6ebYEagPfB1nNnTMMReo
AJSPvfucao8wAZatXWPwgC+cPrmUwoenflO9C8dExoNVXliRTR7ggM0lTzDLlz1z
A+l1rJZ/XafJ8wm1gVta+CdBip0eBL90FcM4d/O2F2JveCIswFRe0G5lwC/A1Fl6
p8A62RXgzXmGQQlaEWJl56VWFbgjU+RiCX1oBlonvlhGyLHRlw446EVgT6B3Ge1L
ZuBxwI+toCZZHoEt56XnBvSW2HyR5Mn74E7S1wIDAQABAoIBADEyM0YbAPA1qPGE
g1zaVOwO9h5ZhOyBQCDTFx56koqSLa6edYtGOzRJZZZ6bF3O2h1fcxX9bgRYGUsj
DHpIL3PSmy8QbdUx4CPms3eDGlxts8jU0XrjB7he33/olJmvWAe9idtW2AUqQn7U
K7uGBM2iOSq4GADJr1vOnMnKoMKnAftohVRfs7zNFk939B7lJaMx1qOfjqIpivAh
mfOjBQbjdLE/9yKG7AdFwSlQOkCHhqJx0HVwAU47KjkCgObiDa8qB1Mzfamiz64d
hlN8D4bkJPaRFgs4RkyZ4D/yLgEH9RDfdkUFXYJICoKidgamkAb59c/RnaC1Czv7
HFl5H1ECgYEA5MFt9oTxBJtVhyEB7Eq47rGBHQ4BEmxApE0klIr0qhzSff68Ra4e
PCXUxpCVZg0EHzHE1e7c1fwebO1s4VPnrt1bFrmuLZAFrfP2ybwa5uar9jC7SuG1
Kc79+n9ka4RZHNDrSeKfvV1JsvJpd2dsqxMXiVLJ4Dc4wqsDkym7mHsCgYEA4AaR
Q0LG2TOSvwHVVX1xerwGNZAF6erkMt0jwcyot1V+0FMRSZIY0RC6BEjakDv2tyAL
1t2xaMT9a/0X5ir6fsYrQV0IV7YrHDDUMC0Mf3zD2C6P6KozVz67AL23wgn30GbB
GwkrhkRZdBp5UZid1JmWROE4taOrkix7SRLN9lUCgYBQveVuSeY4VguOMpxXJti3
h3enJbZDJmp6k7ABrjpFgP7a8frKLXcGi+PaYlYZOyblS8/KIxFXhjHHCNaRgegY
1JC+KLPN6TPX/nBxLC/iqDCyKOkyuRAUvbbvn992A7Tgcu1Zmt//GtpUizOmrZ6x
k3+PPOV8eoFSVMJiQyyv2QKBgDtJgagLRBS3UV67XXoCeMXCfOIv+2CsiTfszkm7
n+rk9WqskSanQ2oQwynfHfiN5f0QvhFfGlRfzbGGiYygUyq5xzjFJjAQRdhwE7es
NGKI4kuUBwHMCUpNj6/ihX8UpEDC8Nf21U3zSLkxSGoPubVJ8DrKX/eyGPXWT8pG
77sBAoGBANMUBQxNxeuHd0QCJtYdVjkek9VYRMx+KuGFKpzzz8OtELWT13p0EUJk
FVY2Mh9M5SUX0eSbTFit18gVxqg6AShsNZBOpCrfLrouYTBrltQbhepG5rr1T2ZD
itdzFdaVhGBnlG4CgSTcvFaJUCY/Y98qsQPlf9mrhbVPTdNS/i4q
-----END RSA PRIVATE KEY-----
`, publicKey: `MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyC8ejfGt+In7+0E2y3qj
0SP9CFrXsQGRV6GFYabxPHFtt8bYYtyH2CVJdps9tPbwVNbGxSqApekjPgbL+16J
lapX9L2K6ebYEagPfB1nNnTMMReoAJSPvfucao8wAZatXWPwgC+cPrmUwoenflO9
C8dExoNVXliRTR7ggM0lTzDLlz1zA+l1rJZ/XafJ8wm1gVta+CdBip0eBL90FcM4
d/O2F2JveCIswFRe0G5lwC/A1Fl6p8A62RXgzXmGQQlaEWJl56VWFbgjU+RiCX1o
BlonvlhGyLHRlw446EVgT6B3Ge1LZuBxwI+toCZZHoEt56XnBvSW2HyR5Mn74E7S
1wIDAQAB`,
	},
	{
		// sm2 key pair
		privateKey: `-----BEGIN PRIVATE KEY-----
MIGTAgEAMBMGByqGSM49AgEGCCqBHM9VAYItBHkwdwIBAQQgV/jOWgcM+0jjlmgY
uLyBIvZPx+GZ8mjH7Po+GYYvykygCgYIKoEcz1UBgi2hRANCAATXVtQufwQwXxmU
r8fdeBfIgrBXhf+oeYswmN+iiJcSWXMl/n5nSS34j+z+5VMEh+adxx6Q8x5xfy9o
FaHj/Juf
-----END PRIVATE KEY-----
`,
		publicKey: `MFkwEwYHKoZIzj0CAQYIKoEcz1UBgi0DQgAE11bULn8EMF8ZlK/H3XgXyIKwV4X/
qHmLMJjfooiXEllzJf5+Z0kt+I/s/uVTBIfmnccekPMecX8vaBWh4/ybnw==`,
	},
}
