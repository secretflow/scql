#!/bin/bash
#
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

set -ex

SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

project_name=$1

output_logs=${SCRIPT_DIR}/../logs
if [ ! -d ${output_logs} ]; then
    mkdir ${output_logs}
fi
# get logs
docker cp ${project_name}-engine-alice-1:/logs/scqlengine.log ${output_logs}/alice_scqlengine.log
docker cp ${project_name}-engine-bob-1:/logs/scqlengine.log ${output_logs}/bob_scqlengine.log
docker cp ${project_name}-engine-carol-1:/logs/scqlengine.log ${output_logs}/carol_scqlengine.log
# read log
python ${SCRIPT_DIR}/get_op.py ${output_logs}/alice_scqlengine.log ${output_logs} alice_op.csv
python ${SCRIPT_DIR}/get_op.py ${output_logs}/bob_scqlengine.log ${output_logs} bob_op.csv
python ${SCRIPT_DIR}/get_op.py ${output_logs}/carol_scqlengine.log ${output_logs} carol_op.csv