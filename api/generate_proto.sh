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
bazelisk build --remote_cache="" //api:scql_go_proto //api:spu_go_proto //api/v1alpha1:compiler_go_proto
mkdir -p pkg/proto-gen/scql
mkdir -p pkg/proto-gen/scql/v1alpha1
proto_gen_package=github.com/secretflow/scql/pkg/proto-gen
# copy files execpt spu.pb.go
ls bazel-bin/api/scql_go_proto_/${proto_gen_package}/scql/ | grep -v spu.* | xargs -I {} cp -r bazel-bin/api/scql_go_proto_/${proto_gen_package}/scql/{} pkg/proto-gen/scql
cp -r bazel-bin/api/spu_go_proto_/${proto_gen_package}/spu/. pkg/proto-gen/spu
cp -r bazel-bin/api/v1alpha1/compiler_go_proto_/${proto_gen_package}/scql/v1alpha1/. pkg/proto-gen/scql/v1alpha1
chmod -R -x+X pkg/proto-gen

# generate openapi file for broker.proto
trap "rm -rf libspu google" EXIT
# note: temporary copy spu.proto to avoid external dependency
# symlink for "bazel-<workspace-name>"
BAZEL_EXEC_ROOT=bazel-$(basename $(pwd))
mkdir -p libspu
cp -f ${BAZEL_EXEC_ROOT}/external/spulib~/libspu/spu.proto libspu/spu.proto
# note: protoc need google api
mkdir -p google/api
cp -f ${BAZEL_EXEC_ROOT}/external/googleapis~/google/api/*.proto google/api
