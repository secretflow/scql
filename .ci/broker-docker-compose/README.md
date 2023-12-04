# Startup

```bash
# export `SCDB_PORTS` and `MYSQL_PORT` and `PROTOCOLS` defined in `.ci/docker-compose/.env`
export $(grep -v '^#' .ci/broker-docker-compose/.env | xargs)

(cd .ci/broker-docker-compose && python setup.py)

# You could specify project name via flag `-p project_name` to
# avoid container name conflict in multi-user environments.
(cd .ci/broker-docker-compose && docker compose -p broker-test up -d)
```

# Run test

```bash

(cd .ci/broker-docker-compose && bash run_test.sh)

```

# End test

```bash

docker compose -p broker-test down

```