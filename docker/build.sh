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
set -eu

LATEST_COMMIT_HASH=$(git log -1 --pretty="format:%h")
DATETIME=$(date +"%Y%m%d%H%M%S")
REVISION="${DATETIME}-${LATEST_COMMIT_HASH}"
IMAGE_TAG=$REVISION
SCQL_IMAGE=scql

# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

WORK_DIR=$(
  cd $SCRIPT_DIR/..
  pwd
)

# prepare temporary path /tmp/$IMAGE_TAG for file copies
echo "copy files to dir: /tmp/$IMAGE_TAG"
rm -rf /tmp/$IMAGE_TAG
mkdir -p /tmp/$IMAGE_TAG

# check whether build container exists, create it if not exits
devbox=scql-dev-$(whoami)

if [[ -n $(docker ps -a -f "status=exited" | grep ${devbox}) ]]; then
  docker rm ${devbox}
  echo "remove container"
fi

rm_devbox=false
if [[ -z $(docker ps -q -f "name=^${devbox}$") ]]; then
  rm_devbox=true
  docker run -d -it --name ${devbox} \
    --mount type=bind,source="${WORK_DIR}",target=/home/admin/dev/ \
    -w /home/admin/dev \
    secretflow/scql-ci:latest
fi

# build engine binary
docker exec -it ${devbox} bash -c "cd /home/admin/dev && bazel build //engine/exe:scqlengine -c opt"
docker cp ${devbox}:/home/admin/dev/bazel-bin/engine/exe/scqlengine /tmp/$IMAGE_TAG

# build scdbserver + scdbclient binary
docker exec -it ${devbox} bash -c "cd /home/admin/dev && make"
docker cp ${devbox}:/home/admin/dev/bin/scdbserver /tmp/$IMAGE_TAG
docker cp ${devbox}:/home/admin/dev/bin/scdbclient /tmp/$IMAGE_TAG

# clean build container on-demand
if [ "$rm_devbox" = true ]; then
  docker stop ${devbox} && docker rm ${devbox}
fi

# copy dockerfile
cp $SCRIPT_DIR/scql.Dockerfile /tmp/$IMAGE_TAG

# build docker image
cd /tmp/$IMAGE_TAG
echo "start to build scql image in $(pwd)"
#docker build -f  scql.Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
docker build -f scql.Dockerfile -t scql:latest .

rm -rf /tmp/$IMAGE_TAG
