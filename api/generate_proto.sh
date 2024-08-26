#!/bin/bash
#
# Copyright 2023 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e
bazel build //api:scql_go_proto //api:spu_go_proto
mkdir -p pkg/proto-gen/scql
proto_gen_package=github.com/secretflow/scql/pkg/proto-gen
# copy files execpt spu.pb.go
ls bazel-bin/api/scql_go_proto_/${proto_gen_package}/scql/ | grep -v spu.* | xargs -I {} cp -r bazel-bin/api/scql_go_proto_/${proto_gen_package}/scql/{} pkg/proto-gen/scql
cp -r bazel-bin/api/spu_go_proto_/${proto_gen_package}/spu/. pkg/proto-gen/spu
chmod -R -x+X pkg/proto-gen

# generate openapi file for broker.proto/scdb_api.proto
trap "rm -rf libspu google" EXIT
# note: temporary copy spu.proto to avoid external dependency
# symlink for "bazel-<workspace-name>"
BAZEL_EXEC_ROOT=bazel-$(basename $(pwd))
mkdir -p libspu
cp -f ${BAZEL_EXEC_ROOT}/external/spulib/libspu/spu.proto libspu/spu.proto
# note: protoc need google api
mkdir -p google/api
cp -f ${BAZEL_EXEC_ROOT}/external/googleapis/google/api/*.proto google/api

GOBIN=${PWD}/tool-bin go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
PATH=${GOBIN}:${PATH} protoc --proto_path . -I${BAZEL_EXEC_ROOT}/external/com_google_protobuf/src\
    --openapiv2_out ./docs \
    --openapiv2_opt output_format=yaml \
    --openapiv2_opt preserve_rpc_order=true \
    --openapiv2_opt json_names_for_fields=false \
    --openapiv2_opt remove_internal_comments=true \
    --openapiv2_opt Mlibspu/spu.proto=spu.pb \
    --openapiv2_opt openapi_naming_strategy=fqn \
    --openapiv2_opt openapi_configuration=api/broker.config_openapi.yaml \
    api/broker.proto

PATH=${GOBIN}:${PATH} protoc --proto_path . -I${BAZEL_EXEC_ROOT}/external/com_google_protobuf/src\
    --openapiv2_out ./docs \
    --openapiv2_opt output_format=yaml \
    --openapiv2_opt preserve_rpc_order=true \
    --openapiv2_opt json_names_for_fields=false \
    --openapiv2_opt remove_internal_comments=true \
    --openapiv2_opt Mlibspu/spu.proto=spu.pb \
    --openapiv2_opt openapi_naming_strategy=fqn \
    --openapiv2_opt openapi_configuration=api/scdb_api.config_openapi.yaml \
    api/scdb_api.proto
