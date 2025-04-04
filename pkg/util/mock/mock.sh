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