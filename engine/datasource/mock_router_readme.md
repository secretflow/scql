mock router
===========

1. start mock router server

```shell
pip install -r ./requirements.txt

uvicorn --port 8000 mock_router_server:app
```

2. open http://127.0.0.1:8000/ in browser to see the mock router

3. register datasource & add route rule

```shell
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:8000/datasource/register \
    -d '{"name": "mysql source", "kind": "mysql", "connection_str": "xxx"}'

curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:8000/datasource/route_rule \
    -d '{"db": "*", "table": "*", "datasource_id": "ds_0"}'

# test with route request
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:8000/datasource/route \
    -d '{"tables": [{"db": "mydb", "table": "mytbl"}]}'
```
