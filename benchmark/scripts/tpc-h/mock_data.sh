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

set -eu

CUR_DIR="$(pwd)"
WORK_DIR=${CUR_DIR}/scripts/tpc-h
CSV_DIR=${CUR_DIR}/docker-compose/csv
echo $WORK_DIR

BUILDER=reg.docker.alibaba-inc.com/secretflow/scql-ci:latest

container_id=$(docker run -it --rm --detach \
                -w /home/admin \
                $BUILDER tail -f /dev/null)
trap "docker rm -f ${container_id}" EXIT
docker cp ${WORK_DIR}/tpch.patch ${container_id}:/home/admin
docker cp ${WORK_DIR}/clean_data.py ${container_id}:/home/admin
docker cp ${WORK_DIR}/create_data.sh ${container_id}:/home/admin

docker exec -i ${container_id} bash create_data.sh

files=$(docker exec ${container_id} find /home/admin -name "*.csv")
for file in $files; do
    docker cp "${container_id}:$file" ${CSV_DIR}
done
