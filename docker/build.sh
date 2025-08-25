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

# Global variables
SCQL_IMAGE=scql
IMAGE_TAG=latest
ENABLE_CACHE=false
TARGET_STAGE=image-prod
TARGET_PLATFORM=""
BASE_IMAGE=ubuntu
PROTECT_DEV_ENV=false
container_id=""
TMP_PATH=""

# Display usage information
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

# Parse command line arguments
parse_arguments() {
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
}

# Setup environment variables and directories
setup_environment() {
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
}

# Configure Docker mount options and platform settings
configure_docker_settings() {
  MOUNT_OPTIONS=""
  if ${ENABLE_CACHE}; then
    local cache_vol_name="scql-${BASE_IMAGE}-buildcache"
    local go_cache_vol_name="scql-${BASE_IMAGE}-go-buildcache"
    MOUNT_OPTIONS="--mount type=volume,source=${cache_vol_name},target=/root/.cache --mount type=volume,source=${go_cache_vol_name},target=/usr/local/pkg/mod"
  fi

  HOST_PLATFORM=""
  case "$(arch)" in
    x86_64) HOST_PLATFORM="linux/amd64" ;;
    aarch64 | arm64) HOST_PLATFORM="linux/arm64" ;;
    *)
      echo "Unsupported host architecture: $(arch)" >&2
      exit 1
      ;;
  esac

  # If TargetPlatform is not set, set to host platform
  TARGET_PLATFORM=${TARGET_PLATFORM:-$HOST_PLATFORM}

  BUILDER=secretflow/scql-ci:latest
  if [ "$BASE_IMAGE" == "anolis" ]; then
    BUILDER=secretflow/release-ci:latest
    if [ "$TARGET_PLATFORM" == "linux/arm64" ]; then
      BUILDER=secretflow/release-ci-aarch64:latest
    fi
  fi
}

# Create and configure Docker container
create_docker_container() {
  container_id=$(docker run -it --rm --detach \
    --mount type=bind,source="${WORK_DIR}",target=/home/admin/dev/ \
    -w /home/admin/dev ${MOUNT_OPTIONS} \
    $BUILDER tail -f /dev/null)

  # Setup cleanup trap for both container and build artifacts
  trap 'cleanup_on_exit' EXIT

  # prepare for git command
  docker exec -it ${container_id} bash -c "git config --global --add safe.directory /home/admin/dev"
}

# Build binaries inside Docker container
build_binaries() {
  # Select build command based on protection mode
  local build_cmd
  if [ "$PROTECT_DEV_ENV" = "true" ]; then
    build_cmd="make binary-dev"
  else
    build_cmd="make binary"
  fi

  echo "Building binaries with command: ${build_cmd}"
  docker exec -it ${container_id} bash -c "cd /home/admin/dev && ${build_cmd}"
}

# Prepare temporary directory and copy all build files
prepare_build_files() {
  # prepare temporary path for file copies
  TMP_PATH=$WORK_DIR/.buildtmp/$IMAGE_TAG
  rm -rf $TMP_PATH
  mkdir -p $TMP_PATH
  mkdir -p $TMP_PATH/$TARGET_PLATFORM
  echo "copy files to dir: $TMP_PATH"

  # Copy standard binaries
  docker cp ${container_id}:/home/admin/dev/bin/. $TMP_PATH/$TARGET_PLATFORM

  # Copy scqlengine binary
  copy_scqlengine_binary

  # copy dockerfile
  cp ${SCRIPT_DIR}/scql-${BASE_IMAGE}.Dockerfile $TMP_PATH/Dockerfile

  # copy scripts
  cp -r ${WORK_DIR}/scripts $TMP_PATH/scripts
}

# Copy scqlengine binary based on build mode
copy_scqlengine_binary() {
  echo "Copying scqlengine binary..."

  local dest_path="$TMP_PATH/$TARGET_PLATFORM"
  mkdir -p "$dest_path"

  local source_paths=()
  if [ "$PROTECT_DEV_ENV" = "true" ]; then
    echo "In development protection mode, using isolated symlink location..."
    # With --symlink_prefix=/tmp/bazel_isolated_build/, the symlink is created there
    source_paths+=(
      "/tmp/bazel_isolated_build/bin/engine/exe/scqlengine"
    )

    # Fallback to output_base locations if symlink doesn't exist
    source_paths+=(
      "/tmp/bazel_docker_build/execroot/_main/bazel-out/k8-opt/bin/engine/exe/scqlengine"
      "/tmp/bazel_docker_build/execroot/_main/bazel-out/k8-fastbuild/bin/engine/exe/scqlengine"
    )
  else
    echo "In standard mode, using the default build location..."
    source_paths+=(
      "/home/admin/dev/bazel-bin/engine/exe/scqlengine"
    )
  fi

  for src_path in "${source_paths[@]}"; do
    echo "Attempting to copy from: ${container_id}:${src_path}"
    if docker cp "${container_id}:${src_path}" "$dest_path/scqlengine" 2>/dev/null; then
      echo "Success! Copied scqlengine to $dest_path"
      return 0
    fi
  done

  echo "Error: Could not find scqlengine binary after trying all candidate locations."
  exit 1
}

# Build Docker image
build_docker_image() {
  cd $TMP_PATH
  echo "start to build scql image in $(pwd)"

  # If target == host, no need to use buildx
  if [ "$HOST_PLATFORM" == "$TARGET_PLATFORM" ]; then
    docker build --build-arg="TARGETPLATFORM=${TARGET_PLATFORM}" --target $TARGET_STAGE -f Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
  else
    docker buildx build --platform $TARGET_PLATFORM --target $TARGET_STAGE -f Dockerfile -t $SCQL_IMAGE:$IMAGE_TAG .
  fi
}

# Cleanup function that runs on exit (success or failure)
cleanup_on_exit() {
  echo "Starting cleanup process..."

  # Clean up root-owned directories BEFORE stopping container
  if [ "$PROTECT_DEV_ENV" = "true" ] && [ -n "${container_id}" ]; then
    echo "Cleaning up root-owned directories created during build..."
    docker exec -it ${container_id} bash -c "cd /home/admin/dev && rm -rf bin tool-bin" 2>/dev/null || true
  fi

  # Stop Docker container if it exists
  if [ -n "${container_id}" ]; then
    echo "Stopping Docker container: ${container_id}"
    docker stop ${container_id} 2>/dev/null || true
  fi

  # Clean up .buildtmp directory
  local buildtmp_dir="$WORK_DIR/.buildtmp"
  if [ -d "$buildtmp_dir" ]; then
    rm -rf "$buildtmp_dir"
    echo "Successfully cleaned up .buildtmp directory"
  fi
}

main() {
  parse_arguments "$@"
  setup_environment
  configure_docker_settings
  create_docker_container
  build_binaries
  prepare_build_files
  build_docker_image
  echo "Build completed successfully!"
}

main "$@"
