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
(cd .ci/docker-compose && python setup.py)
```

More to say: you may need run `pip install -r requirements.txt` when first time running docker-compose.

### Turn up all containers

```bash
(cd .ci/docker-compose && docker compose -p regtest up -d)
```

### Run regtest

```bash
# All test flags are stored in regtest.yml. If necessary, please modify the corresponding parameters in regtest.yml before running the tests.

# go test will use package path as working directory
go test ./cmd/regtest/scdb/... -v -count=1 -timeout=30m -args --conf=../../../.ci/docker-compose/regtest.yml


# you could run sql interactively if needed
# export `SCDB_PORTS` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/docker-compose/.env | xargs)
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
# please set SPU_PROTOCOL in .env file if you want to test ABY3 or CHEETAH
(cd .ci/broker-docker-compose && python setup.py)
```

More to say: you may need run `pip install -r requirements.txt` when first time running docker-compose.

### Turn up all containers

```bash
(cd .ci/broker-docker-compose && docker compose -p regtest-p2p up -d)
```

### Run regtest

```bash
# All test flags are stored in regtest.yml. If necessary, please modify the corresponding parameters in regtest.yml before running the tests.
# go test will use package path as working directory
go test ./cmd/regtest/p2p/... -v -count=1 -timeout=30m -args --conf=../../../.ci/broker-docker-compose/regtest.yml
```

You could run sql manually if needed.
```bash
# please modify project-id or host if they are changed manually in .broker-docker-compose/.env
./brokerctl run "select plain_datetime_0 from alice_tbl_0 limit 1;" --project-id "scdb_SEMI2K" --host http://127.0.0.1:8880 --timeout 30
```

### Turn down all containers

```bash
(cd .ci/broker-docker-compose && docker compose -p regtest-p2p down)
```