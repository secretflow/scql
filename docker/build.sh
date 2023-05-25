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
set -eux

SCQL_IMAGE=scql
IMAGE_TAG=latest

# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

WORK_DIR=$(
  cd $SCRIPT_DIR/..
  pwd
)

if [[ $# -eq 2 ]]; then
  SCQL_IMAGE=$1
  IMAGE_TAG=$2
elif [[ $# -eq 1 ]]; then
  IMAGE_TAG=$1
fi
echo "build image $SCQL_IMAGE:$IMAGE_TAG"

# prepare temporary path $TMP_PATH for file copies
TMP_PATH=$WORK_DIR/.buildtmp/$IMAGE_TAG
echo "copy files to dir: $TMP_PATH"
rm -rf $TMP_PATH
mkdir -p $TMP_PATH
trap "rm -rf $TMP_PATH" EXIT

container_id=$(docker run -it --rm --detach \
  --mount type=bind,source="${WORK_DIR}",target=/home/admin/dev/ \
  -w /home/admin/dev \
  secretflow/release-ci:latest)

trap "docker stop ${container_id}" EXIT

# build engine binary
docker exec -it ${container_id} bash -c "cd /home/admin/dev && bazel build //engine/exe:scqlengine -c opt"
docker cp ${container_id}:/home/admin/dev/bazel-bin/engine/exe/scqlengine $TMP_PATH

# build scdbserver + scdbclient binary
docker exec -it ${container_id} bash -c "cd /home/admin/dev && make"
docker cp ${container_id}:/home/admin/dev/bin/scdbserver $TMP_PATH
docker cp ${container_id}:/home/admin/dev/bin/scdbclient $TMP_PATH

# copy dockerfile
cp $SCRIPT_DIR/scql.Dockerfile $TMP_PATH

# build docker image
cd $TMP_PATH
echo "start to build scql image in $(pwd)"

docker build -f scql.Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
