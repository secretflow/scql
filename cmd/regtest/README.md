# Regression Test

## Test with Local Docker Compose

### Env Prepare

#### Build images

You could build images from source if you would like to use the latest code.

```bash
# build image scql:latest
bash docker/build.sh
```

#### Customize images and port

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
go test ./cmd/regtest/... -v -count=1 -timeout=30m

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
cd .ci/docker-compose
docker compose -p regtest down
```
