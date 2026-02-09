// Copyright 2026 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

const (
	NoTLS     string = "notls"
	TLS       string = "tls"
	MutualTLS string = "mtls"
)

type TLSConf struct {
	Mode       string `yaml:"mode"`
	CertPath   string `yaml:"cert"`
	KeyPath    string `yaml:"key"`
	CACertPath string `yaml:"cacert"`
}

func LoadTLSConfig(mode, cacertPath, certPath, keyPath string) (*tls.Config, error) {
	var tlsConfig *tls.Config
	cacert, err := os.ReadFile(cacertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cacert file")
	}
	cacertPool := x509.NewCertPool()
	if !cacertPool.AppendCertsFromPEM(cacert) {
		return nil, fmt.Errorf("failed to append CA certificates")
	}

	lowerMode := strings.ToLower(mode)
	switch lowerMode {
	case TLS:
		tlsConfig = &tls.Config{
			RootCAs: cacertPool,
		}
	case MutualTLS:
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load X509 key pair: %w", err)
		}

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      cacertPool,
		}
	}
	return tlsConfig, nil
}
