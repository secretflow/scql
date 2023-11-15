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
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type AuthMethod int

const (
	authMethodNone AuthMethod = iota
	authMethodToken
	authMethodPubkey
)

type CreateUserStmtBuilder struct {
	ifNotExists bool
	userName    string
	passwd      string
	partyCode   string
	authMethod  AuthMethod
	token       string
	tp          time.Time
	privKey     any
	endpoints   []string
	err         error
}

func NewCreateUserStmtBuilder() *CreateUserStmtBuilder {
	return &CreateUserStmtBuilder{
		ifNotExists: false,
		authMethod:  authMethodNone,
		tp:          time.Now(),
	}
}

func (b *CreateUserStmtBuilder) SetUser(user string) *CreateUserStmtBuilder {
	b.userName = user
	return b
}

func (b *CreateUserStmtBuilder) SetPassword(passwd string) *CreateUserStmtBuilder {
	b.passwd = passwd
	return b
}

func (b *CreateUserStmtBuilder) SetParty(partyCode string) *CreateUserStmtBuilder {
	b.partyCode = partyCode
	return b
}

func (b *CreateUserStmtBuilder) WithEndpoinits(endpoints []string) *CreateUserStmtBuilder {
	b.endpoints = append([]string{}, endpoints...)
	return b
}

func (b *CreateUserStmtBuilder) IfNotExists() *CreateUserStmtBuilder {
	b.ifNotExists = true
	return b
}

func (b *CreateUserStmtBuilder) AuthByToken(token string) *CreateUserStmtBuilder {
	b.authMethod = authMethodToken
	b.token = token
	return b
}

func (b *CreateUserStmtBuilder) AuthByPubkeyWithPemFile(pemPath string) *CreateUserStmtBuilder {
	b.authMethod = authMethodPubkey
	b.privKey, b.err = LoadPrivateKeyFromPemFile(pemPath)
	return b
}

func (b *CreateUserStmtBuilder) AuthByPubkeyWithPrivateKey(key any) *CreateUserStmtBuilder {
	b.authMethod = authMethodPubkey
	b.privKey = key
	return b
}

// only for test purpose
func (b *CreateUserStmtBuilder) MockTime(t time.Time) *CreateUserStmtBuilder {
	b.tp = t
	return b
}

func (b *CreateUserStmtBuilder) ToSQL() (string, error) {
	if b.err != nil {
		return "", b.err
	}
	var sb strings.Builder
	sb.WriteString("CREATE USER ")
	if b.ifNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}

	if len(b.userName) == 0 {
		return "", errors.New("user name is empty")
	}
	sb.WriteString(fmt.Sprintf("`%s` ", b.userName))

	if len(b.partyCode) == 0 {
		return "", errors.New("party code is empty")
	}
	sb.WriteString(fmt.Sprintf("PARTY_CODE '%s' ", b.partyCode))

	if len(b.passwd) == 0 {
		return "", errors.New("password is empty")
	}
	sb.WriteString(fmt.Sprintf("IDENTIFIED BY '%s'", b.passwd))

	if b.authMethod == authMethodNone && len(b.endpoints) == 0 {
		return sb.String(), nil
	}

	sb.WriteString(" WITH")
	if b.authMethod == authMethodToken {
		if len(b.token) == 0 {
			return "", errors.New("token is empty while auth by TOKEN")
		}
		sb.WriteString(fmt.Sprintf(" TOKEN '%s'", b.token))
	} else if b.authMethod == authMethodPubkey {
		msg, err := b.tp.MarshalText()
		if err != nil {
			return "", err
		}

		sig, err := signMessage(b.privKey, msg)
		if err != nil {
			return "", err
		}

		pub, err := getPublicKeyInDER(b.privKey)
		if err != nil {
			return "", err
		}

		sb.WriteString(fmt.Sprintf(" '%s' '%s' '%s'", string(msg), sig, base64.StdEncoding.EncodeToString(pub)))
	}

	if len(b.endpoints) > 0 {
		sb.WriteString(" ENDPOINT ")
		for i, endpoint := range b.endpoints {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(fmt.Sprintf("'%s'", endpoint))
		}
	}
	return sb.String(), nil
}

// TODO(jingshi): move to appropriate place
func LoadPrivateKeyFromPemFile(pemPath string) (any, error) {
	file, err := os.Open(pemPath)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "PRIVATE KEY" {
			continue
		}
		// find private key and parse it
		return x509.ParsePKCS8PrivateKey(block.Bytes)
	}

	return nil, errors.New("failed to decode PEM block containing private key")
}

func signMessage(key any, msg []byte) (string, error) {
	switch priv := key.(type) {
	case ed25519.PrivateKey:
		sig := ed25519.Sign(priv, msg)
		return base64.StdEncoding.EncodeToString(sig), nil
	default:
		return "", errors.New("unsupported type of private key")
	}
}

func getPublicKeyInDER(privKey any) ([]byte, error) {
	switch priv := privKey.(type) {
	case ed25519.PrivateKey:
		pub := priv.Public()
		return x509.MarshalPKIXPublicKey(pub)
	default:
		return nil, errors.New("unsupported type of private key")
	}
}
