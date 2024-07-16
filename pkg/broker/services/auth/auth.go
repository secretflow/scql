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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sirupsen/logrus"
	"github.com/tjfoc/gmsm/sm2"

	"github.com/secretflow/scql/pkg/broker/config"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/keyutil"
)

type Auth struct {
	privKey any
}

func NewAuth(cfg *config.Config) (*Auth, error) {
	if cfg.PrivateKeyPath != "" {
		priv, err := keyutil.LoadPrivateKeyFromPemFile(cfg.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key file %s: %w", cfg.PrivateKeyPath, err)
		}
		return NewPrivAuth(priv)
	} else if cfg.PrivateKeyData != "" {
		pemData, err := base64.StdEncoding.DecodeString(cfg.PrivateKeyData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 encoded private key data: %w", err)
		}
		return NewPemAuth(pemData)
	} else {
		return nil, fmt.Errorf("private key path and content are both empty, provide at least one")
	}
}

func NewPrivAuth(priv any) (*Auth, error) {
	switch v := priv.(type) {
	case ed25519.PrivateKey, *rsa.PrivateKey, *sm2.PrivateKey:
		return &Auth{privKey: priv}, nil
	default:
		return nil, fmt.Errorf("NewAuth: unsupported private key type: %T", v)
	}
}

func NewPemAuth(pemData []byte) (*Auth, error) {
	priv, err := keyutil.LoadPrivateKeyFromPem(pemData)
	if err != nil {
		logrus.Errorf("NewAuth: %v", err)
		return nil, fmt.Errorf("NewAuth: %v", err)
	}

	return NewPrivAuth(priv)
}

func (auth *Auth) sign(msg []byte) (signature []byte, err error) {
	switch priv := auth.privKey.(type) {
	case ed25519.PrivateKey:
		return ed25519.Sign(priv, msg), nil
	case *rsa.PrivateKey:
		msgHashSum := sha256.Sum256(msg)
		return rsa.SignPSS(rand.Reader, priv, crypto.SHA256, msgHashSum[:], nil)
	case *sm2.PrivateKey:
		return priv.Sign(rand.Reader, msg, nil)
	default:
		return nil, fmt.Errorf("unsupported sign message using private key type: %T", priv)
	}
}

func verify(pub any, msg, sig []byte) error {
	switch pub := pub.(type) {
	case ed25519.PublicKey:
		if !ed25519.Verify(pub, msg, sig) {
			return fmt.Errorf("failed to verify signature with ed25519 public key")
		}
		return nil
	case *rsa.PublicKey:
		msgHashSum := sha256.Sum256(msg)
		return rsa.VerifyPSS(pub, crypto.SHA256, msgHashSum[:], sig, nil)
	case *sm2.PublicKey:
		if ok := pub.Verify(msg, sig); !ok {
			return fmt.Errorf("sm2 pub verify failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported verify message using public key type: %T", pub)
	}
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
	signature, err := auth.sign(c)
	if err != nil {
		return fmt.Errorf("SignMessage: %v", err)
	}

	msg.ProtoReflect().Set(signDesc, protoreflect.ValueOfBytes(signature))

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

	pub, err := keyutil.ParsePubKey(pubKey)
	if err != nil {
		return err
	}

	return verify(pub, msgArray, sign)
}

func (auth *Auth) GetPubKey() (string, error) {
	return keyutil.GetPubKeyInPEM(auth.privKey)
}
