#!/bin/bash

# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

WORK_DIR=$(
  cd $SCRIPT_DIR/../../..
  pwd
)

cd ${SCRIPT_DIR}
python mock_from_testdata.py -dd=testdata -s="testdata/db.json"

mv testdata/mysql_*_init.sql ${WORK_DIR}/.ci/test-data/mysql/
mv testdata/postgres_*_init.sql ${WORK_DIR}/.ci/test-data/postgres
mv testdata/*.csv ${WORK_DIR}/.ci/test-data/csv

find . -type f -name '*.py' -print0 | xargs -0 black