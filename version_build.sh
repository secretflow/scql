#!/bin/bash
#
# Copyright 2024 Ant Group Co., Ltd.
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

# NOTE: used internally by Makefile, please not call it manually

set -eu

RESTORE_FLAG=false

usage() {
  echo "Usage: $0 [-r]"
  echo "default build version info and change version.h in engine"
  echo "Options:"
  echo "  -r, restore version changes in version.h, using after version builded."
}

while getopts "r" options; do
  case "${options}" in
  r)
    RESTORE_FLAG=true
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

if $RESTORE_FLAG; then
  if [ -e ${SCRIPT_DIR}/engine/exe/version.h.bak ]; then
    cp -p ${SCRIPT_DIR}/engine/exe/version.h.bak ${SCRIPT_DIR}/engine/exe/version.h
    rm ${SCRIPT_DIR}/engine/exe/version.h.bak
    echo "restore version.h succeed"
  else
    echo "no bak file existing, please build version first"
  fi
  exit
fi

VERSION=$(grep "version" ${SCRIPT_DIR}/version.txt | awk -F'"' '{print $2}')
VERSION+=".$(date '+%Y%m%d-%H:%M:%S')"
VERSION+=".$(git rev-parse --short HEAD)"

# backup and replace version info for engine
cp -p ${SCRIPT_DIR}/engine/exe/version.h ${SCRIPT_DIR}/engine/exe/version.h.bak
sed -i "s/ENGINE_VERSION_STRING.*/ENGINE_VERSION_STRING \"${VERSION}\"/g" ${SCRIPT_DIR}/engine/exe/version.h


echo ${VERSION}