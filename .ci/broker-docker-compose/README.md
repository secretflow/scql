# Test

## build image

only for test

```bash
bash docker/build.sh -t latest -c
```

for coverage

```bash
bash docker/build.sh -t latest -c -v
```

## Startup

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `PROTOCOLS` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/broker-docker-compose/.env | xargs)

(cd .ci/broker-docker-compose && python setup.py)

# You could specify project name via flag `-p project_name` to
# avoid container name conflict in multi-user environments.
(cd .ci/broker-docker-compose && docker compose -p broker-test up -d)
```

## Run test

```bash

go test ./cmd/regtest/p2p/... -run TestRunQueryWithNormalCCL -v -count=1 -timeout=30m -args --conf=../../../.ci/broker-docker-compose/regtest.yml

```

## End test

```bash

docker compose -p broker-test down

```

## Coverage

```bash
bash .ci/coverage.sh
```
