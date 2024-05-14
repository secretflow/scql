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
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tjfoc/gmsm/sm2"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/sessionctx"
)

type PartyAuthMethod int

const (
	PartyAuthMethodNone PartyAuthMethod = iota
	PartyAuthMethodToken
	PartyAuthMethodPubKey
)

type PartyAuthenticator struct {
	method PartyAuthMethod
	// check timestamp to prevent replay attacks
	enableTimestampCheck bool
	// timestamp is valid iff in time range (now()-validTimePeriod, now()]
	validTimePeriod time.Duration
}

func NewPartyAuthenticator(conf config.PartyAuthConf) *PartyAuthenticator {
	pa := &PartyAuthenticator{
		method:               PartyAuthMethodNone,
		enableTimestampCheck: conf.EnableTimestampCheck,
		validTimePeriod:      conf.ValidityPeriod,
	}
	switch conf.Method {
	case config.PartyAuthMethodPubKey:
		pa.method = PartyAuthMethodPubKey
	case config.PartyAuthMethodToken:
		pa.method = PartyAuthMethodToken
	}
	return pa
}

func (pa *PartyAuthenticator) inValidityPeriod(t time.Time) bool {
	now := time.Now()
	return t.Before(now) && t.After(now.Add(-pa.validTimePeriod))
}

func (pa *PartyAuthenticator) VerifyPartyAuthentication(engOpt *ast.EngineOption) error {
	if pa.method == PartyAuthMethodPubKey {
		if engOpt.PubKeyAuth == nil {
			return fmt.Errorf("engine auth pub key fields are required but missed")
		}
		return pa.verifyPubKey(engOpt.PubKeyAuth)
	}

	if pa.method == PartyAuthMethodToken {
		if engOpt.TokenAuth == nil {
			return fmt.Errorf("engine auth token field is required but missed")
		}
		return nil
	}
	return nil
}

func (pa *PartyAuthenticator) verifyPubKey(authOpt *ast.PubKeyAuthOption) (err error) {
	defer func() {
		// https://pkg.go.dev/crypto/ed25519@go1.19.11#Verify
		// recover if public key is invalid
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to verify engine auth: %v", r)
		}
		if err != nil {
			logrus.Error(err)
		}
	}()
	// check timestamp
	if pa.enableTimestampCheck {
		timestamp, err := time.Parse(time.RFC3339, authOpt.Message)
		if err != nil {
			return fmt.Errorf("failed to parse message as timestamp in RFC3339: %+v", err)
		}
		if !pa.inValidityPeriod(timestamp) {
			return fmt.Errorf("timestamp %s is no longer valid", authOpt.Message)
		}
	}

	// verify signature with public key
	{
		pubDER, err := base64.StdEncoding.DecodeString(authOpt.PubKey)
		if err != nil {
			return fmt.Errorf("failed to parse public key in base64 encoding: %+v", err)
		}

		pub, err := x509.ParsePKIXPublicKey(pubDER)
		if err != nil {
			return fmt.Errorf("failed to parse DER encoded public key: %+v", err)
		}

		sig, err := base64.StdEncoding.DecodeString(authOpt.Signature)
		if err != nil {
			return fmt.Errorf("failed to decode signature in base64 encoding: %+v", err)
		}
		switch pub := pub.(type) {
		case ed25519.PublicKey:
			if !ed25519.Verify(pub, []byte(authOpt.Message), sig) {
				return fmt.Errorf("failed to verify signature with ed25519 public key")
			}
		case *rsa.PublicKey:
			msgHashSum := sha256.Sum256([]byte(authOpt.Message))
			if err := rsa.VerifyPSS(pub, crypto.SHA256, msgHashSum[:], sig, nil); err != nil {
				return fmt.Errorf("failed to verify signature with rsa public key: %+v", err)
			}
		case *sm2.PublicKey:
			if ok := pub.Verify([]byte(authOpt.Message), sig); !ok {
				return fmt.Errorf("failed to verify signature with sm2 public key")
			}
		default:
			return fmt.Errorf("unknown type of public key")
		}
	}
	return nil
}

type authType int

func (a authType) String() string {
	return "party-authenticator-key"
}

const key authType = 0

func BindPartyAuthenticator(ctx sessionctx.Context, pa *PartyAuthenticator) {
	ctx.SetValue(key, pa)
}

func GetPartyAuthenticator(ctx sessionctx.Context) *PartyAuthenticator {
	if v, ok := ctx.Value(key).(*PartyAuthenticator); ok {
		return v
	}
	return nil
}
