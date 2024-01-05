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
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sirupsen/logrus"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/sqlbuilder"
)

type Auth struct {
	priv ed25519.PrivateKey
}

func NewAuth(pemPath string) (*Auth, error) {
	priv, err := sqlbuilder.LoadPrivateKeyFromPemFile(pemPath)
	if err != nil {
		logrus.Errorf("NewAuth: %v", err)
		return nil, fmt.Errorf("NewAuth: %v", err)
	}

	e, ok := priv.(ed25519.PrivateKey)
	if !ok {
		err = fmt.Errorf("NewAuth: no ed25519 private key")
		logrus.Error(err)
		return nil, err
	}
	return &Auth{priv: e}, nil
}

func (auth *Auth) SignMessage(msg proto.Message) (err error) {
	defer func() {
		// recover if protoReflect panic
		if r := recover(); r != nil {
			err = status.New(pb.Code_INTERNAL, fmt.Sprintf("SignMessage: failed to sign message: %v", r))
			logrus.Error(err)
			return
		}
		if err != nil {
			err = status.New(pb.Code_INTERNAL, err.Error())
			logrus.Error(err)
		}
	}()
	signDesc := msg.ProtoReflect().Descriptor().Fields().ByJSONName("signature")
	if msg.ProtoReflect().Has(signDesc) {
		// clear old sign
		msg.ProtoReflect().Clear(signDesc)
	}
	c, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("SignMessage: %v", err)
	}
	sign := ed25519.Sign(auth.priv, c)

	msg.ProtoReflect().Set(signDesc, protoreflect.ValueOfBytes(sign))

	return
}

func (auth *Auth) CheckSign(msg proto.Message, pubKey string) (err error) {
	defer func() {
		// https://pkg.go.dev/crypto/ed25519@go1.19.11#Verify
		// recover if public key is invalid
		if r := recover(); r != nil {
			err = status.New(pb.Code_UNAUTHENTICATED, fmt.Sprintf("CheckSign: failed to check signature: %v", r))
			logrus.Error(err)
			return
		}
		if err != nil {
			err = status.New(pb.Code_UNAUTHENTICATED, fmt.Sprintf("CheckSign: unable to check signature: %s", err.Error()))
			logrus.Error(err)
		}
	}()

	pubDER, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return fmt.Errorf("failed to parse public key in base64 encoding: %v", err)
	}

	pub, err := x509.ParsePKIXPublicKey(pubDER)
	if err != nil {
		return fmt.Errorf("failed to parse DER encoded public key: %v", err)
	}

	msg = proto.Clone(msg)
	signDesc := msg.ProtoReflect().Descriptor().Fields().ByJSONName("signature")
	if !msg.ProtoReflect().Has(signDesc) {
		return fmt.Errorf("failed to find signature in message")
	}
	sign := msg.ProtoReflect().Get(signDesc).Bytes()
	msg.ProtoReflect().Clear(signDesc)

	msgArray, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal msg: %v", err)
	}

	switch pub := pub.(type) {
	case ed25519.PublicKey:
		if !ed25519.Verify(pub, msgArray, sign) {
			return fmt.Errorf("failed to verify signature with public key")
		}
	// TODO: support sm2, rsa
	default:
		return fmt.Errorf("unknown type of public key")
	}

	return nil
}
