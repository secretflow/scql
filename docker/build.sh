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
CONTAINER_ID=""
OVERLAY_VOLUME=""
WORKSPACE_MOUNT_ARGS=""
CACHE_MOUNT_ARGS=""
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
  echo "  -d, protect development environment (use isolated build)"
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
  # Configure cache mount options
  CACHE_MOUNT_ARGS=""
  if ${ENABLE_CACHE}; then
    local cache_vol_name="scql-${BASE_IMAGE}-buildcache"
    local go_cache_vol_name="scql-${BASE_IMAGE}-go-buildcache"
    CACHE_MOUNT_ARGS="--mount type=volume,source=${cache_vol_name},target=/root/.cache --mount type=volume,source=${go_cache_vol_name},target=/usr/local/pkg/mod"
  fi

  # Configure workspace mount strategy based on protection mode
  if [ "$PROTECT_DEV_ENV" = "true" ]; then
    # Use overlay volume for complete isolation with direct mount
    local overlay_volume="scql-build-overlay-$(date +%s)"
    local changes_dir="$WORK_DIR/.build_changes"
    echo "Creating overlay volume with direct mount: $overlay_volume"

    # Create overlay directories in workspace
    mkdir -p "$changes_dir/upper" "$changes_dir/work"

    # Create overlay volume with workspace-relative paths
    docker volume create "${overlay_volume}" \
      --driver local \
      --opt type=overlay \
      --opt device=overlay \
      --opt "o=lowerdir=${WORK_DIR},upperdir=${changes_dir}/upper,workdir=${changes_dir}/work" \
      >/dev/null

    OVERLAY_VOLUME="$overlay_volume"

    WORKSPACE_MOUNT_ARGS="--mount type=volume,source=${overlay_volume},target=/home/admin/dev/"
  else
    # Standard bind mount for non-protected builds
    WORKSPACE_MOUNT_ARGS="--mount type=bind,source=${WORK_DIR},target=/home/admin/dev/"
  fi

  # Configure platform settings
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

  # Configure builder image
  BUILDER=secretflow/scql-ci:latest
  if [ "$BASE_IMAGE" == "anolis" ]; then
    BUILDER=secretflow/release-ci:latest
    if [ "$TARGET_PLATFORM" == "linux/arm64" ]; then
      BUILDER=secretflow/release-ci-aarch64:latest
    fi
  fi
}

# Create Docker container and build binaries
create_container_and_build() {
  echo "Creating Docker container and building binaries..."

  # Create and start the container
  CONTAINER_ID=$(docker run -it --rm --env GOPROXY=https://goproxy.cn,direct --detach \
    ${WORKSPACE_MOUNT_ARGS} \
    -w /home/admin/dev ${CACHE_MOUNT_ARGS} \
    $BUILDER tail -f /dev/null)

  # Setup cleanup trap for both container and build artifacts
  trap 'cleanup_on_exit' EXIT

  # Prepare git configuration
  docker exec -it ${CONTAINER_ID} bash -c "git config --global --add safe.directory /home/admin/dev"

  # Build binaries
  docker exec -it ${CONTAINER_ID} bash -c "cd /home/admin/dev && make binary"
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
  docker cp ${CONTAINER_ID}:/home/admin/dev/bin/. $TMP_PATH/$TARGET_PLATFORM

  # Copy scqlengine binary
  docker cp ${CONTAINER_ID}:/home/admin/dev/bazel-bin/engine/exe/scqlengine $TMP_PATH/$TARGET_PLATFORM

  # copy dockerfile
  cp ${SCRIPT_DIR}/scql-${BASE_IMAGE}.Dockerfile $TMP_PATH/Dockerfile

  # copy scripts
  cp -r ${WORK_DIR}/scripts $TMP_PATH/scripts
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

  # Clean up root-owned directories
  if [ "$PROTECT_DEV_ENV" = "true" ] && [ -n "${OVERLAY_VOLUME:-}" ]; then
    local changes_dir="$WORK_DIR/.build_changes"
    if [ -d "$changes_dir" ]; then
      # Use a separate container to clean up root-owned files by mounting the changes directory
      docker run --rm --mount type=bind,source="$changes_dir",target=/cleanup alpine sh -c "rm -rf /cleanup/*" 2>/dev/null || true
    fi
  fi

  # Stop Docker container if it exists
  if [ -n "${CONTAINER_ID}" ]; then
    echo "Stopping Docker container: ${CONTAINER_ID}"
    docker stop ${CONTAINER_ID} 2>/dev/null || true
  fi

  # Clean up overlay volume if it was created
  if [ "$PROTECT_DEV_ENV" = "true" ] && [ -n "${OVERLAY_VOLUME:-}" ]; then
    docker volume rm "${OVERLAY_VOLUME}" 2>/dev/null || true

    local changes_dir="$WORK_DIR/.build_changes"
    if [ -d "$changes_dir" ]; then
      rm -rf "$changes_dir" 2>/dev/null || true
    fi
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
  create_container_and_build
  prepare_build_files
  build_docker_image
  echo "Build completed successfully!"
}

main "$@"
