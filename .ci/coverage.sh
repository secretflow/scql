#!/bin/bash
# OPENSOURCE-CLEANUP DELETE_FILE
# Copyright 2025 Ant Group Co., Ltd.
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

#!/bin/bash

set -eu

CUR_PATH="$(realpath "$0")"
CUR_DIR="$(dirname "$CUR_PATH")"
WORK_DIR=${CUR_DIR}/..

TARGET_DIR=${CUR_DIR}/coverage

if [ ! -d "$TARGET_DIR" ]; then
    echo "create: $TARGET_DIR"
    mkdir -p $TARGET_DIR
fi

GO_INPUT=""
if [ -d "${CUR_DIR}/broker-docker-compose/coverage" ]; then
    echo "found broker coverage files!"
    go tool covdata textfmt -i=${CUR_DIR}/broker-docker-compose/coverage/broker -o=${TARGET_DIR}/go_broker_coverage.out
    GO_INPUT="${GO_INPUT} ${TARGET_DIR}/go_broker_coverage.out"
fi

if [ -d "${CUR_DIR}/docker-compose/coverage" ]; then
    echo "found scdb coverage files!"
    go tool covdata textfmt -i=${CUR_DIR}/docker-compose/coverage/broker -o=${TARGET_DIR}/go_scdb_coverage.out
    GO_INPUT="${GO_INPUT} ${TARGET_DIR}/go_scdb_coverage.out"
fi

go test -timeout=30m -v -short ${WORK_DIR}/pkg/... ${WORK_DIR}/contrib/... -coverprofile ${TARGET_DIR}/go_coverage_unit.out
GO_INPUT="${GO_INPUT} ${TARGET_DIR}/go_coverage_unit.out"
go install github.com/wadey/gocovmerge@latest
gocovmerge ${GO_INPUT} >${CUR_DIR}/coverage/go_merge.out

grep -Ev '\.pb\.go:|_mock\.go:|testutil.go:|experimental|mock_data\.go:|util\/chunk|broker_stub_util\.go:' ${TARGET_DIR}/go_merge.out >${TARGET_DIR}/go_merge_filtered.out
go tool cover -html=${TARGET_DIR}/go_merge_filtered.out -o ${TARGET_DIR}/go_coverage.html
go tool cover -func=${TARGET_DIR}/go_merge_filtered.out -o ${TARGET_DIR}/go_func.out

BUILDER=reg.docker.alibaba-inc.com/secretflow/scql-ci:latest
MOUNT_OPTIONS="--mount type=volume,source=scql-ubuntu-buildcache,target=/root/.cache --mount type=volume,source=scql-ubuntu-go-buildcache,target=/usr/local/pkg/mod"
container_id=$(docker run -it --rm --detach \
    --mount type=bind,source="${WORK_DIR}",target=/home/admin/dev/ \
    -w /home/admin/dev ${MOUNT_OPTIONS} \
    $BUILDER tail -f /dev/null)

trap "docker stop ${container_id}" EXIT

# prepare for git command
docker exec -it ${container_id} bash -c "cd /home/admin/dev &&
bazel coverage //engine/... --combined_report=lcov --copt=-DTEST_SEMI2K_ONLY \
&& cp -r .ci/broker-docker-compose/coverage/engine/bazel-out/k8-fastbuild/bin/engine bazel-out/k8-fastbuild/bin/ \
&& lcov --capture --base-directory /home/admin/dev --directory ./bazel-out/k8-fastbuild/bin/engine --output-file /tmp/coverage.info  --ignore-errors gcov \
&& lcov --remove /tmp/coverage.info '*/third_party/*' '*/external/*' '*/usr/*' '*.pb.h' '*.pb.cc' -o /tmp/filtered_coverage.info \
&& cp /tmp/filtered_coverage.info ./bazel-testlogs/coverage.dat && bash .ci/combine_coverage_report.sh ./ .ci/coverage"

cp -f ${TARGET_DIR}/go_merge_filtered.out ${WORK_DIR}/go_merge_filtered.out
cp -f ${TARGET_DIR}/coverage.xml ${WORK_DIR}/coverage.xml
