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

package kusciaclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	kusciaTokenHeader = "token"
)

type TLSMode string

const (
	NoTLS     TLSMode = "notls"
	TLS       TLSMode = "tls"
	MutualTLS TLSMode = "mtls"
)

func NewKusciaClientConn(endpoint string, tlsMode string, certPath, keyPath, cacertPath, token string) (*grpc.ClientConn, error) {
	var grpcDialOpts []grpc.DialOption
	lowerTlsMode := strings.ToLower(tlsMode)
	switch lowerTlsMode {
	case string(NoTLS):
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case string(TLS), string(MutualTLS):
		// header must contain token if TLSMode is TLS or MutualTLS
		grpcDialOpts = append(grpcDialOpts, grpc.WithUnaryInterceptor(grpcClientTokenInterceptor(token)))

		var creds credentials.TransportCredentials
		if lowerTlsMode == string(MutualTLS) {
			tlsConfig, err := loadTLSConfig(certPath, keyPath, cacertPath)
			if err != nil {
				return nil, err
			}
			creds = credentials.NewTLS(tlsConfig)
		} else {
			var err error
			creds, err = credentials.NewClientTLSFromFile(cacertPath, "")
			if err != nil {
				return nil, err
			}
		}
		grpcDialOpts = append(grpcDialOpts, grpc.WithTransportCredentials(creds))
	default:
		return nil, fmt.Errorf("unknown kusciaapi tls_mode: %s", tlsMode)
	}

	return grpc.Dial(endpoint, grpcDialOpts...)
}

func grpcClientTokenInterceptor(tokenStr string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, kusciaTokenHeader, tokenStr)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func loadTLSConfig(certPath, keyPath, cacertPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	cacert, err := os.ReadFile(cacertPath)
	if err != nil {
		return nil, err
	}

	cacertPool := x509.NewCertPool()
	cacertPool.AppendCertsFromPEM(cacert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      cacertPool,
	}
	return tlsConfig, nil
}
