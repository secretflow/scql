# Regression Test

## PreBuild

### Build images

You could build images from source if you would like to use the latest code.

```bash
# build image scql:latest
bash docker/build.sh
```

## Test Central with Local Docker Compose

### Customize images and port

You could customize scdbserver published port and container images and protocols by configuring env file `.ci/docker-compose/.env`.

### Prepare docker files

If you want to test more than one protocols you can modify `.ci/docker-compose/.env` and set `PROTOCOLS` as the protocols which you want to test and separate them with commas. At the same time you must set scdb port for every protocol. Such as if you want to test protocols ABY3 and SEMI2K, you must set `PROTOCOLS=ABY3,SEMI2K` and `SCDB_PORTS=8080,8081`.

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `PROTOCOLS` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/docker-compose/.env | xargs)

(cd .ci/docker-compose && python setup.py)
```

More to say: you may need run `pip install -r requirements.txt` when first time running docker-compose.

### Turn up all containers

```bash
(cd .ci/docker-compose && docker compose -p regtest up -d)
```

### Run regtest

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `PROTOCOLS` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/docker-compose/.env | xargs)
# export SKIP_CONCURRENT_TEST=true if you want to skip current tests
# export SKIP_PLAINTEXT_CCL_TEST=true if you want to skip all ccl plaintext tests
# go test will use package path as working directory
go test ./cmd/regtest/scdb_test/... -v -count=1 -timeout=30m -args -alicePem ../../../.ci/docker-compose/engine/alice/conf/ed25519key.pem -bobPem ../../../.ci/docker-compose/engine/bob/conf/ed25519key.pem -carolPem ../../../.ci/docker-compose/engine/carol/conf/ed25519key.pem

# debugging set SKIP_CREATE_USER_CCL true to skip create user ccl ...
export SKIP_CREATE_USER_CCL=true
# you could run sql interactively if needed
go run cmd/scdbclient/main.go prompt --host="http://localhost:$SCDB_PORTS"
>switch alice;
alice> use scdb
alice> select ...
```

### Turn down all containers

```bash
(cd .ci/docker-compose && docker compose -p regtest down)
```

## Test P2P with Local Docker Compose

### Customize images and port

You could customize scdbserver published port and container images and protocols by configuring env file `.ci/broker-docker-compose/.env`.

### Prepare docker files

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `SCQL_IMAGE_TAG` defined in `.ci/broker-docker-compose/.env`
export $(grep -v '^#' .ci/broker-docker-compose/.env | xargs)

(cd .ci/broker-docker-compose && python setup.py)
```

More to say: you may need run `pip install -r requirements.txt` when first time running docker-compose.

### Turn up all containers

```bash
(cd .ci/broker-docker-compose && docker compose -p regtest-p2p up -d)
```

### Run regtest

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `PROJECT_CONF` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/broker-docker-compose/.env | xargs)
# export SKIP_CONCURRENT_TEST=true if you want to skip all concurrency tests, including concurrent execution of queries and concurrent modification of project information.
# export SKIP_PLAINTEXT_CCL_TEST=true if you want to skip all ccl plaintext tests
# go test will use package path as working directory
go test ./cmd/regtest/p2p_test/... -v

# set SKIP_CREATE_TABLE_CCL true to skip create user ccl fro debugging mode when run tests repeatedly
export SKIP_CREATE_TABLE_CCL=true
```

### Turn down all containers

```bash
(cd .ci/broker-docker-compose && docker compose -p regtest-p2p down)
```