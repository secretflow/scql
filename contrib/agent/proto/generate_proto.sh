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
bazelisk build --remote_cache="" //contrib/agent/proto:task_config_go_proto
cp -f bazel-bin/contrib/agent/proto/task_config_go_proto_/github.com/secretflow/scql/contrib/agent/proto/task_config.pb.go contrib/agent/proto/task_config.pb.go
chmod -R -x+X contrib/agent/proto
