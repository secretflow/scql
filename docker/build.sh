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
BASE_IMAGE=ubuntu
PROTECT_DEV_ENV=false

usage() {
  echo "Usage: $0 [-n Name] [-t Tag] [-p Platform] [-s Stage] [-c]"
  echo ""
  echo "Options:"
  echo "  -n name, image name, default is \"scql\""
  echo "  -t tag, image tag, default is \"latest\""
  echo "  -s target build stage, default is \"image-prod\", set it to \"image-dev\" for debug purpose."
  echo "  -p target platform, default is \"linux/amd64\", support \"linux/arm64\" and \"linux/amd64\"."
  echo "  -b base image, default is \"ubuntu\", support \"ubuntu\" and \"anolis\"."
  echo "  -c, enable host disk bazel cache to speedup build process"
  echo "  -d, protect development environment (use isolated build and preserve symlinks)"
}

while getopts "n:t:s:p:b:cd" options; do
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
  d)
    PROTECT_DEV_ENV=true
    ;;
  c)
    ENABLE_CACHE=true
    ;;
  p)
    TARGET_PLATFORM=${OPTARG}
    ;;
  b)
    BASE_IMAGE=${OPTARG}
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
  if [ "$BASE_IMAGE" == "ubuntu" ]; then
    MOUNT_OPTIONS="--mount type=volume,source=scql-ubuntu-buildcache,target=/root/.cache --mount type=volume,source=scql-ubuntu-go-buildcache,target=/usr/local/pkg/mod"
  else
    MOUNT_OPTIONS="--mount type=volume,source=scql-anolis-buildcache,target=/root/.cache --mount type=volume,source=scql-ubuntu-go-buildcache,target=/usr/local/pkg/mod"
  fi
fi

MACHINE_TYPE=$(arch)
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

BUILDER=secretflow/scql-ci:latest
if [ "$BASE_IMAGE" == "anolis" ]; then
  BUILDER=secretflow/release-ci:latest
  if [ "$TARGET_PLATFORM" == "linux/arm64" ]; then
    BUILDER=secretflow/release-ci-aarch64:latest
  fi
fi

container_id=$(docker run -it --rm --detach \
  --mount type=bind,source="${WORK_DIR}",target=/home/admin/dev/ \
  -w /home/admin/dev ${MOUNT_OPTIONS} \
  $BUILDER tail -f /dev/null)

trap "docker stop ${container_id}" EXIT


# prepare for git command
docker exec -it ${container_id} bash -c "git config --global --add safe.directory /home/admin/dev"

# Backup existing bazel symlinks to preserve host development environment (if protection enabled)
if $PROTECT_DEV_ENV; then
  echo "Development environment protection enabled - backing up existing Bazel symlinks..."
  BACKUP_DIR="/tmp/bazel_symlinks_backup_$$"
  mkdir -p "$BACKUP_DIR"
  for link in bazel-bin bazel-out bazel-testlogs bazel-scql external; do
    if [ -L "$link" ]; then
      cp -P "$link" "$BACKUP_DIR/"
      echo "Backed up: $link -> $(readlink "$link")"
    fi
  done
else
  echo "Using standard build mode (development environment protection disabled)"
fi

# Select build command based on protection mode
if $PROTECT_DEV_ENV; then
  build_cmd="make binary-dev"
else
  build_cmd="make binary"
fi

# Build binary
docker exec -it ${container_id} bash -c "cd /home/admin/dev && ${build_cmd}"

# Restore the original Bazel symlinks (if protection enabled)
if $PROTECT_DEV_ENV; then
  echo "Restoring original Bazel symlinks..."
  if [ -d "$BACKUP_DIR" ]; then
    # Remove any Docker-created symlinks first
    for link in bazel-bin bazel-out bazel-testlogs bazel-dev external; do
      if [ -L "$link" ]; then
        rm -f "$link"
      fi
    done

    # Restore original symlinks
    for backup_link in "$BACKUP_DIR"/*; do
      if [ -L "$backup_link" ]; then
        link_name=$(basename "$backup_link")
        cp -P "$backup_link" "./"
        echo "Restored: $link_name -> $(readlink "$link_name")"
      fi
    done

    # Cleanup backup directory
    rm -rf "$BACKUP_DIR"
    echo "Successfully restored development environment symlinks"
  else
    echo "Warning: No backup directory found, symlinks may need manual restoration"
  fi
else
  echo "Standard build mode - no symlink restoration needed"
fi

# prepare temporary path $TMP_PATH for file copies
TMP_PATH=$WORK_DIR/.buildtmp/$IMAGE_TAG
rm -rf $TMP_PATH
mkdir -p $TMP_PATH
mkdir -p $TMP_PATH/$TARGET_PLATFORM
echo "copy files to dir: $TMP_PATH"

docker cp ${container_id}:/home/admin/dev/bin/. $TMP_PATH/$TARGET_PLATFORM

# Copy scqlengine binary based on build mode
if $PROTECT_DEV_ENV; then
  # Development protection mode: use isolated output_base
  echo "Copying scqlengine from isolated build location..."
  docker cp ${container_id}:/tmp/bazel_docker_build/execroot/_main/bazel-out/k8-opt/bin/engine/exe/scqlengine $TMP_PATH/$TARGET_PLATFORM 2>/dev/null || {
    echo "k8-opt path failed, trying k8-fastbuild..."
    docker cp ${container_id}:/tmp/bazel_docker_build/execroot/_main/bazel-out/k8-fastbuild/bin/engine/exe/scqlengine $TMP_PATH/$TARGET_PLATFORM 2>/dev/null || {
      # Fallback: Dynamic search as last resort
      echo "Standard paths failed, searching dynamically..."
      SCQLENGINE_PATH=$(docker exec ${container_id} find /tmp/bazel_docker_build -name 'scqlengine' -type f 2>/dev/null | head -1)
      if [ -n "$SCQLENGINE_PATH" ]; then
        docker cp ${container_id}:$SCQLENGINE_PATH $TMP_PATH/$TARGET_PLATFORM/scqlengine
        echo "Found scqlengine at: $SCQLENGINE_PATH"
      else
        echo "Error: Could not find scqlengine binary in /tmp/bazel_docker_build"
        exit 1
      fi
    }
  }
else
  # Standard mode: use normal bazel-bin location
  echo "Copying scqlengine from standard build location..."
  docker cp ${container_id}:/home/admin/dev/bazel-bin/engine/exe/scqlengine $TMP_PATH/$TARGET_PLATFORM 2>/dev/null || {
    echo "Error: Could not find scqlengine binary in standard location"
    exit 1
  }
fi

# copy dockerfile
cp ${SCRIPT_DIR}/scql-${BASE_IMAGE}.Dockerfile $TMP_PATH/Dockerfile

# copy scripts
cp -r ${WORK_DIR}/scripts $TMP_PATH/scripts

# build docker image
cd $TMP_PATH
echo "start to build scql image in $(pwd)"

# If target == host, no need to use buildx
if [ "$HOST_PLATFORM" == "$TARGET_PLATFORM" ]; then
  docker build --build-arg="TARGETPLATFORM=${TARGET_PLATFORM}" --target $TARGET_STAGE -f Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
else
  docker buildx build --platform $TARGET_PLATFORM --target $TARGET_STAGE -f Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
fi

# cleanup
rm -rf ${TMP_PATH}
