==================
SCQL Config Manual
==================

This configuration manual is designed to help users understand the various configuration options available for the SCQL system. It can be divided into two parts: SCDB configuration options and Engine configuration options.

SCDB configuration options
==========================

In SCDB, configuration parameters are provided as yaml file.

Example for SCDB
----------------

.. code-block:: yaml

  scdb_host: http://localhost:8080
  port: 8080
  protocol: http
  query_result_callback_timeout: 200ms
  session_expire_time: 1h
  session_expire_check_time: 100ms
  password_check: true
  log_level: debug
  storage:
    type: sqlite
    conn_str: scdb.db
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m
  grm:
    grm_mode: stdgrm
    host: http://your-grm-host
    timeout: 5s
  engine:
      timeout: 120s
      protocol: http
      content_type: application/json
      spu:
        protocol: SEMI2K
        field: FM64
        sigmoid_mode: SIGMOID_REAL


Config in SCDB
--------------

+-------------------------------+---------+------------------------------------------------------------+
| Name                          | Default | Description                                                |
+===============================+=========+============================================================+
| scdb_host                     | none    | The callback URL for the engine to notify SCDB             |
+-------------------------------+---------+------------------------------------------------------------+
| port                          | none    | The listening port of the SCDB server                      |
+-------------------------------+---------+------------------------------------------------------------+
| protocol                      | http    | The transfer protocol of SCDB server, supports HTTP/HTTPS  |
+-------------------------------+---------+------------------------------------------------------------+
| query_result_callback_timeout | 200ms   | Timeout for SCDB to notify the query result                |
+-------------------------------+---------+------------------------------------------------------------+
| session_expire_time           | 48h     | The expiration duration of a session                       |
+-------------------------------+---------+------------------------------------------------------------+
| session_expire_check_time     | 1h      | The cleanup interval of expired session                    |
+-------------------------------+---------+------------------------------------------------------------+
| password_check                | true    | Whether to validate password strength when create a user   |
+-------------------------------+---------+------------------------------------------------------------+
| log_level                     | info    | The type and severity of a logged event                    |
+-------------------------------+---------+------------------------------------------------------------+
| tls.cert_file                 | none    | Certificate file path to enable TSL, supports crt/pem type |
+-------------------------------+---------+------------------------------------------------------------+
| tls.key_file                  | none    | Private key file path to enable TSL, supports key/pem type |
+-------------------------------+---------+------------------------------------------------------------+
| storage.type                  | none    | Database Kind in SCDB, supports mysql/sqlite               |
+-------------------------------+---------+------------------------------------------------------------+
| storage.conn_str              | none    | Used to connect to a database                              |
+-------------------------------+---------+------------------------------------------------------------+
| storage.max_idle_conns        | 1       | Maximum number of connections in idle connection pool      |
+-------------------------------+---------+------------------------------------------------------------+
| storage.max_open_conns        | 1       | Maximum number of open connections to the database         |
+-------------------------------+---------+------------------------------------------------------------+
| storage.conn_max_idle_time    | -1s     | Maximum amount of time a connection may be idle            |
+-------------------------------+---------+------------------------------------------------------------+
| storage.conn_max_lifetime     | -1s     | Maximum amount of time a connection may be reused          |
+-------------------------------+---------+------------------------------------------------------------+
| grm.grm_mode                  | none    | The grm service type, support toygrm/stdgrm                |
+-------------------------------+---------+------------------------------------------------------------+
| grm.host                      | none    | The host of stdgrm                                         |
+-------------------------------+---------+------------------------------------------------------------+
| grm.timeout                   | none    | Timeout for SCDB to get info from stdgrm                   |
+-------------------------------+---------+------------------------------------------------------------+
| grm.toy_grm_conf              | none    | The config file path of toygrm                             |
+-------------------------------+---------+------------------------------------------------------------+
| engine.timeout                | none    | Timeout for SCDB to send message to engine                 |
+-------------------------------+---------+------------------------------------------------------------+
| engine.protocol               | https   | The transfer protocol of Engine, support http/https        |
+-------------------------------+---------+------------------------------------------------------------+
| engine.content_type           | none    | The original media type in post body from SCDB to engine   |
+-------------------------------+---------+------------------------------------------------------------+
| engine.spu.protocol           | none    | The mpc protocol for engine to work with                   |
+-------------------------------+---------+------------------------------------------------------------+
| engine.spu.field              | none    | A security parameter type for engine to work with          |
+-------------------------------+---------+------------------------------------------------------------+
| engine.spu.sigmoid_mode       | none    | The sigmoid approximation method for engine to work with   |
+-------------------------------+---------+------------------------------------------------------------+


Config for GRM
^^^^^^^^^^^^^^
In addition to being provided by developers, GRM services can also be simulated by reading local JSON files, which is used for testing and development, you can choose them as follows.

1. stdgrm

If you want to use your own developed GRM service, grm_mode need to be set as stdgrm.

.. code-block:: yaml
  
  grm:
    grm_mode: stdgrm  
    host: ${host of grm service} # eg. http://localhost:8080
    timeout: ${timeout of grm service} # eg. 2m

2. toygrm

If you want to directly mock a GRM service from a json file, except set grm_mode as toygrm, toy_grm_conf also need to be set.

.. code-block:: yaml

  grm:
    grm_mode: toygrm
    toy_grm_conf: ${file path of toy grm config} # eg. toy_grm.json


.. _scdb-tls:

Config for TLS
^^^^^^^^^^^^^^

If you need to enable TLS in SCDB, please refer to the following configuration.

.. code-block:: yaml

  scdb_host: ${host of scdb service}  # eg. https://localhost:8080
  protocol: https
  tls:
    cert_file: ${file path of server cert}  # eg. path_of_server_cert.pem
    key_file: ${file path of server key}  # eg. path_of_server_key.pem
  engine:
    protocol: https

Additionally, it is necessary to configure the engine to work with SSL, please refer `Config for SSL`_.


Config for SPU
^^^^^^^^^^^^^^
SCQL supports different mpc protocol powered by SPU, you can choose different mpc protocol by setting SPU runtime config. Protocol **SEMI2K** is suggested, which is fully tested and support multi parties. See `SPU runtime config <https://www.secretflow.org.cn/docs/spu/en/reference/runtime_config.html>`_ to get more information.

.. code-block:: yaml

  spu:
    protocol: SEMI2K
    field: FM64
    sigmoid_mode: SIGMOID_REAL


Engine configuration options
============================
SCQLEngine uses Gflags to manage configurations when SCQLEngine set up.

Example for Engine
------------------

.. code-block::

  # Config for Brpc server
  --listen_port=8003
  # Config for datasource
  --datasource_router=embed
  --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"${connection_str}"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}


Config in Engine
----------------

+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| Name                                       | Default      | Description                                                                   |
+============================================+==============+===============================================================================+
| log_dir                                    | logs         | The directory to save log file                                                |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| log_enable_console_logger                  | true         | Whether logging to stdout while logging to file                               |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_protocol                       | `http:proto` | The rpc protocol between engine and engine                                    |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_connection_type                | pooled       | The rpc connection type between engine and engine                             |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_timeout_ms                     | 300000       | The rpc timeout between engine and engine, unit: ms                           |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_max_retry                      | 3            | Rpc max retries(not including the first rpc) between engine and engine        |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_enable_ssl_as_client           | true         | Whether enable ssl encryption when send message to another engine             |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_enable_ssl_client_verification | false        | Whether enable certificate verification when send message to another engine   |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| peer_engine_ssl_client_ca_certificate      | none         | The trusted CA file to verify certificate when send message to another engine |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| link_recv_timeout_ms                       | 30000        | The max time that engine will wait for message come from another engine       |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_protocol                              | `http:proto` | The rpc protocol between engine and SCDB                                      |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_connection_type                       | pooled       | The rpc connection type between engine and SCDB                               |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_timeout_ms                            | 5000         | The rpc timeout between engine and SCDB, unit: ms                             |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_max_retry                             | 3            | Rpc max retries(not including the first rpc) between engine and SCDB          |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_enable_ssl_as_client                  | true         | Whether enable ssl encryption when send message to SCDB                       |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_enable_ssl_client_verification        | false        | Whether enable certificate verification when send message to SCDB             |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| scdb_ssl_client_ca_certificate             | none         | The trusted CA file to verify certificate when send message to SCDB           |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| listen_port                                | 8003         | The listening port of engine service                                          |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| enable_builtin_service                     | false        | Whether enable brpc builtin service                                           |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| internal_port                              | 9527         | The listening port of brpc builtin services                                   |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| idle_timeout_s                             | 30           | Idle connection close delay in seconds between the engine and SCDB, unit: s   |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| server_enable_ssl                          | true         | Whether enable SSL when engine work as a server                               |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| server_ssl_certificate                     | none         | Certificate file path to enable SSL when engine work as a server              |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| server_ssl_private_key                     | none         | Private key file path to enable SSL when engine work as a server              |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| enable_client_authorization                | false        | Whether check requests' http header when engine work as a server              |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| auth_credential                            | none         | Authorization credential used to check requests' http header                  |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| enable_scdb_authorization                  | false        | Whether to authenticate the identity of SCDB                                  |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| engine_credential                          | none         | Credential used to authenticate SCDB                                          |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| session_timeout_s                          | 1800         | Expiration duration of a session between engine and SCDB, unit: s             |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| datasource_router                          | embed        | The datasource router type                                                    |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| embed_router_conf                          | none         | Configuration for embed router in json format                                 |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+
| db_connection_info                         | none         | Connection string used to connect to mysql                                    |
+--------------------------------------------+--------------+-------------------------------------------------------------------------------+

.. _datasource_router:

Config for datasource
^^^^^^^^^^^^^^^^^^^^^
datasources(MYSQL/SQLite3) are where the SCQLEngine gets its data from.

*datasource_router* is design to support multi datasources, currently only supported: embed.

embed_router is initialized with *embed_router_conf* first, a json string like::

  "datasources": [
    {
      "id": "ds001",
      "name": "mysql db for scql",
      "kind": "MYSQL",
      "connection_str": "${connection_str}"
    }
  ],
  "rules":[
    {
      "db": "*",
      "table": "*",
      "datasource_id": "ds001"
    }
  ]

if *embed_router_conf* is empty, embed_router will try to initialized with *db_connection_info*.

Embed router
""""""""""""

datasources in embed_router_conf contain information for connecting MYSQL/SQLite3::
  
  id: unique id of datasource.

  name: custom description help to distinguish datasources.

  kind: datasource type, currently support MYSQL/SQLite3.

  connection_str: string used to connect MYSQL/SQLite3.

    MYSQL Connection string format::

      <str> == <assignment> | <assignment> ';' <str>
      <assignment> == <name> '=' <value>
      <name> == 'host' | 'port' | 'user' | 'password' | 'db' | 'compress' | 'auto-reconnect' | 'reset' | 'fail-readonly'
      <value> == [~;]*
      
    MYSQL Connection string e.g::
    
      "db=${db};user=${user};password=${password};host=${host}"
      
    SQLite3 Connection string format::

      more infos: https://www.sqlite.org/c3ref/open.html

    SQLite3 Connection string e.g::
    
      "file:/tmp/data_test.db"
      "file:data_test.db?mode=memory&cache=shared"

Routing rules
"""""""""""""
embed_router's rules support wildcard '*', when given a table in format: *database_name:table_name*,
embed_router will route to the corresponding datasource by::

    1. find the exact rules first, whose `${db}:${table}` equals to database_name:table_name;
    2. try the database_name:* rules;
    3. try *:table_name in the end.

Once found, SCQLEngine will try to connect database with datasource's information correspond to the *datasource_id*.

Config for Brpc server
^^^^^^^^^^^^^^^^^^^^^^
SCQLEngine uses **Brpc** to communicate with SCDB and other peer SCQLEngines, each SCQLEngine will start a Brpc service on *local-host:listen_port* to receive data from outside. If you want to enable Brpc builtin services, add FLAGS:

.. code-block::

  --enable_builtin_service=true
  --internal_port=9527


.. _scqlengine-tls:

Config for SSL
^^^^^^^^^^^^^^
If you want to enable SSL in Engine, add FLAGS as follows. Additionally, it may be necessary to configure SCDB work with TLS please refer `Config for TLS`_.

.. code-block::

  --server_enable_ssl=true
  --server_ssl_certificate=${file path of cert}
  --server_ssl_private_key=${file path of key}
  --peer_engine_enable_ssl_as_client=true
  --scdb_enable_ssl_as_client=true