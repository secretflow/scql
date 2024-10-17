#!/bin/bash
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
python ${SCRIPT_DIR}/get_op.py ${output_logs}/alice_scqlengine.log ${output_logs}/alice_op.csv
python ${SCRIPT_DIR}/get_op.py ${output_logs}/bob_scqlengine.log ${output_logs}/bob_op.csv
python ${SCRIPT_DIR}/get_op.py ${output_logs}/carol_scqlengine.log ${output_logs}/carol_op.csv