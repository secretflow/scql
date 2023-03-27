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

You could customize scdbserver published port and container images by configuring env file `.ci/docker-compose/.env`.



### Turn up all containers

```bash
cd .ci/docker-compose
docker compose -p regtest up -d
```

### Run regtest

```bash
# export `SCDB_PORT` and `MYSQL_PORT` defined in `.ci/docker-compose/.env`
$ export $(grep -v '^#' .ci/docker-compose/.env | xargs)
# export SKIP_CONCURRENT_TEST=true if you want to skip current tests
$ go test ./cmd/regtest/... -v -count=1 -timeout=30m

# debugging set SKIP_CREATE_USER_CCL true to skip create user ccl ...
export SKIP_CREATE_USER_CCL=true
# you could run sql interactively if needed
$ go run cmd/scdbclient/main.go prompt --host="http://localhost:$SCDB_PORT"
>switch alice;
alice> use scdb
alice> select ...
```

### Turn down all containers

```bash
$ cd .ci/docker-compose
$ docker compose -p regtest down
```
