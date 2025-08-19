#!/bin/bash
# OPENSOURCE-CLEANUP DELETE_FILE
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

#!/bin/bash

set -eu

# build image
bash docker/build.sh -t latest -v -c

## Startup
(cd .ci/broker-docker-compose && python setup.py)
(cd .ci/broker-docker-compose && docker compose -p broker-test down)
(cd .ci/broker-docker-compose && docker compose -p broker-test up -d)
export ENABLE_COVERAGE=true
## Run test
go test ./cmd/regtest/p2p/... -run TestRunQueryWithNormalCCL -v -count=1 -timeout=30m -args --conf=../../../.ci/broker-docker-compose/regtest.yml

## End test
docker compose -p broker-test down

## Coverage
bash .ci/coverage.sh
