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

package sqlbuilder

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildCreateUserStmtWithoutAuth(t *testing.T) {
	r := require.New(t)

	expectSql := "CREATE USER `alice` PARTY_CODE 'ALICE' IDENTIFIED BY 'some_passwd'"
	builder := NewCreateUserStmtBuilder()
	sql, err := builder.SetUser("alice").SetParty("ALICE").SetPassword("some_passwd").ToSQL()
	r.NoError(err)
	r.Equal(expectSql, sql)
}

func TestBuildCreateUserStmtWithEndpoints(t *testing.T) {
	r := require.New(t)

	expectSql := "CREATE USER `alice` PARTY_CODE 'ALICE' IDENTIFIED BY 'some_passwd' WITH ENDPOINT 'addr1:port1','addr2:port2'"
	builder := NewCreateUserStmtBuilder()
	sql, err := builder.SetUser("alice").SetParty("ALICE").SetPassword("some_passwd").WithEndpoinits([]string{"addr1:port1", "addr2:port2"}).ToSQL()
	r.NoError(err)
	r.Equal(expectSql, sql)
}

func TestBuildCreateUserStmtWithTokenAndEndpoint(t *testing.T) {
	r := require.New(t)

	expectSql := "CREATE USER `alice` PARTY_CODE 'ALICE' IDENTIFIED BY 'some_passwd' WITH TOKEN 'xxxx' ENDPOINT 'addr1:port1'"
	builder := NewCreateUserStmtBuilder()
	sql, err := builder.SetUser("alice").SetParty("ALICE").SetPassword("some_passwd").AuthByToken("xxxx").WithEndpoinits([]string{"addr1:port1"}).ToSQL()
	r.NoError(err)
	r.Equal(expectSql, sql)
}

func TestBuildCreateUserStmtWithPubkeyAuth(t *testing.T) {
	r := require.New(t)

	expectSql := "CREATE USER `alice` PARTY_CODE 'ALICE' IDENTIFIED BY 'some_passwd' WITH '2023-08-18T18:12:04.00507585+08:00' 'FKNla5+qiybxx0Tx5gGpn5bHX1+0NgKJPNshq1eCT00/Pogu3QAPXJneUdtYQlmaf7dW1Vr25t+oDLRV9+TiCA==' 'MCowBQYDK2VwAyEA8tjoIkf3mIyz/HGdjBD+p/SDxlzHiNDcaTmhF3dHjZY='"
	mockTime, err := time.Parse(time.RFC3339Nano, "2023-08-18T18:12:04.00507585+08:00")
	r.NoError(err)

	privPemData := []byte(`
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIBSXcCv5G1YpIZSD127ImyGnlqA9s9HCpk7jYbl7OQZ5
-----END PRIVATE KEY-----
`)
	block, _ := pem.Decode(privPemData)
	r.True(block != nil && block.Type == "PRIVATE KEY")

	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	r.NoError(err)

	builder := NewCreateUserStmtBuilder()
	sql, err := builder.SetUser("alice").SetParty("ALICE").SetPassword("some_passwd").AuthByPubkeyWithPrivateKey(priv).MockTime(mockTime).ToSQL()

	r.NoError(err)
	r.Equal(expectSql, sql)
}
