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

SCQL_IMAGE=scql
IMAGE_TAG=latest
ENABLE_CACHE=false
TARGET_STAGE=image-prod
TARGET_PLATFORM=""

usage() {
  echo "Usage: $0 [-n Name] [-t Tag] [-p Platform] [-s Stage] [-c]"
  echo ""
  echo "Options:"
  echo "  -n name, image name, default is \"scql\""
  echo "  -t tag, image tag, default is \"latest\""
  echo "  -s target build stage, default is \"image-prod\", set it to \"image-dev\" for debug purpose."
  echo "  -p target platform, default is \"linux/amd64\", support \"linux/arm64\" and \"linux/amd64\"."
  echo "  -c, enable host disk bazel cache to speedup build process"
}

while getopts "n:t:s:p:c" options; do
  case "${options}" in
  n)
    SCQL_IMAGE=${OPTARG}
    ;;
  t)
    IMAGE_TAG=${OPTARG}
    ;;
  s)
    TARGET_STAGE=${OPTARG}
    ;;
  c)
    ENABLE_CACHE=true
    ;;
  p)
    TARGET_PLATFORM=${OPTARG}
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

set -x

# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

WORK_DIR=$(
  cd $SCRIPT_DIR/..
  pwd
)

echo "build image $SCQL_IMAGE:$IMAGE_TAG"

MOUNT_OPTIONS=""
if $ENABLE_CACHE; then
  MOUNT_OPTIONS="--mount type=volume,source=scql-rel-build-cache,target=/root/.cache"
fi

MACHINE_TYPE=`arch`
HOST_PLATFORM=""

if [ "$MACHINE_TYPE" == "x86_64" ]; then
  HOST_PLATFORM=linux/amd64
else
  HOST_PLATFORM=linux/arm64
fi

# If TargetPlatform is not set, set to host platform
if [ -z "$TARGET_PLATFORM" ]; then
  TARGET_PLATFORM=$HOST_PLATFORM
fi

BUILDER=secretflow/release-ci:latest
if [ "$TARGET_PLATFORM" == "linux/arm64" ]; then
  BUILDER=secretflow/release-ci-aarch64:latest
fi

container_id=$(docker run -it --rm --detach \
  -w /home/admin/dev ${MOUNT_OPTIONS} \
  $BUILDER)

trap "docker stop ${container_id}" EXIT

# copy code to docker container
dirs=("pkg" "engine" "api" "bazel" "cmd" ".bazelrc" ".bazelversion" "BUILD.bazel" "go.mod" "go.sum" "Makefile" "WORKSPACE")
for dir in ${dirs[@]}; do
  docker cp ${WORK_DIR}/${dir} ${container_id}:/home/admin/dev
done

# prepare version information before build
version=$(grep "version" $SCRIPT_DIR/version.txt | awk -F'"' '{print $2}')
version+=$(date '+%Y%m%d-%H:%M:%S')
version+=".$(git rev-parse --short HEAD)"
echo "binary version: ${version}"
# build engine binary
docker exec -it ${container_id} bash -c "cd /home/admin/dev && sed -i "s/SCQL_VERSION/$version/g" engine/exe/version.h && bazel build //engine/exe:scqlengine -c opt"
# build scdbserver + scdbclient binary
docker exec -it ${container_id} bash -c "cd /home/admin/dev && export SCQL_VERSION=$version && make"

# prepare temporary path $TMP_PATH for file copies
TMP_PATH=$WORK_DIR/.buildtmp/$IMAGE_TAG
rm -rf $TMP_PATH
mkdir -p $TMP_PATH
mkdir -p $TMP_PATH/$TARGET_PLATFORM
echo "copy files to dir: $TMP_PATH"

docker cp ${container_id}:/home/admin/dev/bazel-bin/engine/exe/scqlengine $TMP_PATH/$TARGET_PLATFORM
docker cp ${container_id}:/home/admin/dev/bin/scdbserver $TMP_PATH/$TARGET_PLATFORM
docker cp ${container_id}:/home/admin/dev/bin/scdbclient $TMP_PATH/$TARGET_PLATFORM
docker cp ${container_id}:/home/admin/dev/bin/broker $TMP_PATH/$TARGET_PLATFORM
docker cp ${container_id}:/home/admin/dev/bin/brokerctl $TMP_PATH/$TARGET_PLATFORM

# copy dockerfile
cp $SCRIPT_DIR/scql.Dockerfile $TMP_PATH

# build docker image
cd $TMP_PATH
echo "start to build scql image in $(pwd)"

# If target == host, no need to use buildx
if [ "$HOST_PLATFORM" == "$TARGET_PLATFORM" ]; then
  docker build --build-arg="TARGETPLATFORM=${TARGET_PLATFORM}" --target $TARGET_STAGE -f scql.Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
else
  docker buildx build --platform $TARGET_PLATFORM --target $TARGET_STAGE -f scql.Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
fi

# cleanup
rm -rf ${TMP_PATH}
