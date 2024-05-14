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

package executor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	engineCredentialHeader = "Credential"
)

func NewEngineClientConn(endpoint string, credential string) (*grpc.ClientConn, error) {
	grpcDialOpts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpcClientCredentialInterceptor(credential)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return grpc.NewClient(endpoint, grpcDialOpts...)
}

func grpcClientCredentialInterceptor(credentialStr string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, engineCredentialHeader, credentialStr)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
