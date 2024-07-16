// Copyright 2024 Ant Group Co., Ltd.
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

package keyutil

import (
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/tjfoc/gmsm/sm2"
	smx509 "github.com/tjfoc/gmsm/x509"
)

func LoadPrivateKeyFromPemFile(pemPath string) (any, error) {
	data, err := os.ReadFile(pemPath)
	if err != nil {
		return nil, err
	}
	return LoadPrivateKeyFromPem(data)
}

func LoadPrivateKeyFromPem(pemData []byte) (any, error) {
	for block, rest := pem.Decode(pemData); block != nil; block, rest = pem.Decode(rest) {
		if block.Type == "PRIVATE KEY" {
			// find private key and parse it
			pk, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				logrus.Warnf("%v\ntry sm2", err)
				return smx509.ParsePKCS8PrivateKey(block.Bytes, nil)
			}
			return pk, err
		}
		if block.Type == "RSA PRIVATE KEY" {
			return x509.ParsePKCS1PrivateKey(block.Bytes)
		}
	}
	return nil, errors.New("failed to decode PEM block containing private key")
}

func GetPublicKeyInDER(privKey any) ([]byte, error) {
	switch priv := privKey.(type) {
	case ed25519.PrivateKey:
		pub := priv.Public()
		return x509.MarshalPKIXPublicKey(pub)
	case *rsa.PrivateKey:
		return x509.MarshalPKIXPublicKey(&priv.PublicKey)
	case *sm2.PrivateKey:
		return smx509.MarshalSm2PublicKey(&priv.PublicKey)
	default:
		return nil, errors.New("unsupported type of private key")
	}
}

// pubKey is in PEM format
func ParsePubKey(pubKey string) (any, error) {
	pubDER, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key in base64 encoding: %v", err)
	}

	pub, err := x509.ParsePKIXPublicKey(pubDER)
	if err != nil {
		logrus.Warnf("%v, try sm2", err)
		pub, err = smx509.ParseSm2PublicKey(pubDER)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DER encoded public key: %v", err)
		}
	}

	return pub, nil
}

// return pubKey in PEM format
func GetPubKeyInPEM(priv any) (string, error) {
	pubKeyinDER, err := GetPublicKeyInDER(priv)
	if err != nil {
		return "", fmt.Errorf("failed to parse public key in DER: %v", err)
	}
	pubKeyBase64 := base64.StdEncoding.EncodeToString(pubKeyinDER)

	return pubKeyBase64, nil
}
