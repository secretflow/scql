# Startup

```bash
# You could specify project name via flag `-p project_name` to 
# avoid container name conflict in multi-user environments.
docker compose up -d
```


# Notes

1. You could customize scdbserver and mysql container published port env `SCDB_PORT` and `MYSQL_PORT` in file `.env`, default port is `8080`.

2. You could customize scql image tag via env `SCQL_IMAGE_TAG` in file `.env`.


