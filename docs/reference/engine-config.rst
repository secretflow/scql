==================
SCQL System Config
==================

This configuration manual is designed to help users understand the various configuration options available for the SCQL system. It can be divided into three parts: SCDB configuration options (for Centralized mode), BrokerServer configuration options (for P2P mode) and SCQLEngine configuration options (for both modes).

.. _scdb_config_options:

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
  enable_audit_logger: true
  audit:
    audit_log_file: audit/audit.log
    audit_detail_file: audit/detail.log
  security_compromise:
    reveal_group_mark: false
  storage:
    type: mysql
    conn_str: user_name:pass_word@tcp(127.0.0.1:3306)/db_name?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m
  engine:
      timeout: 120s
      protocol: http
      content_type: application/json
      spu: |
        {
          "protocol": "SEMI2K",
          "field": "FM64",
          "sigmoid_mode": "SIGMOID_REAL"
        }
  party_auth:
    method: pubkey
    enable_timestamp_check: true
    validity_period: 1m


Config in SCDB
--------------

+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
|                 Name                  |     Default      |                                                    Description                                                    |
+=======================================+==================+===================================================================================================================+
| scdb_host                             | none             | The callback URL for the SCQLEngine to notify SCDB                                                                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| port                                  | none             | The listening port of the SCDB server                                                                             |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| protocol                              | https            | The transfer protocol of SCDB server, supports HTTP/HTTPS                                                         |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| query_result_callback_timeout         | 200ms            | Timeout for SCDB to notify the query result                                                                       |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| session_expire_time                   | 48h              | The expiration duration of a session                                                                              |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| session_expire_check_time             | 1h               | The cleanup interval of expired session                                                                           |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| password_check                        | true             | Whether to validate password strength when create a user                                                          |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| log_level                             | info             | The type and severity of a logged event                                                                           |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| enable_audit_logger                   | true             | Whether to enable audit logger                                                                                    |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_log_file                  | audit/audit.log  | The file to save basic information about a query                                                                  |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_detail_file               | audit/detail.log | The file to save more detailed information about a query                                                          |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_size                  | 500              | The maximum size of the audit log file before it gets rotated, unit: MB                                           |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_backups               | 0                | The maximum number of old audit log files to retain, default 0 to retain all old log files                        |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_age_days              | 180              | The maximum number of days to retain old log files                                                                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_compress              | false            | Whether the rotated log files should be compressed                                                                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| security_compromise.reveal_group_mark | false            | Whether to reveal group_mark directly for group by                                                                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| tls.cert_file                         | none             | Certificate file path to enable TSL, supports crt/pem type                                                        |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| tls.key_file                          | none             | Private key file path to enable TSL, supports key/pem type                                                        |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.type                          | none             | Database kind in SCDB, supports MYSQL/SQLite3, MYSQL is recommended, SQLite3 may have problems with concurrency   |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.conn_str                      | none             | Used to connect to a database                                                                                     |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.max_idle_conns                | 1                | Maximum number of connections in idle connection pool                                                             |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.max_open_conns                | 1                | Maximum number of open connections to the database                                                                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_idle_time            | -1s              | Maximum amount of time a connection may be idle                                                                   |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_lifetime             | -1s              | Maximum amount of time a connection may be reused                                                                 |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.timeout                        | none             | Timeout for SCDB to send message to engine                                                                        |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.protocol                       | https            | The transfer protocol of Engine, support http/https                                                               |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.content_type                   | none             | The original media type in post body from SCDB to engine                                                          |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.spu.protocol                   | none             | The mpc protocol for engine to work with                                                                          |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.spu.field                      | none             | A security parameter type for engine to work with                                                                 |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| engine.spu.sigmoid_mode               | none             | The sigmoid approximation method for engine to work with                                                          |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| party_auth.method                     | pubkey           | Method to authenticate the participant when registering a user, supports pubkey/token                             |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| party_auth.enable_timestamp_check     | true             | When using pubkey authentication, whether to check the signed timestamp to avoid `replay attacks`_                |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+
| party_auth.validity_period            | 30s              | When enable timestamp check,  SCDB will check whether signed timestamp is within (now() - validity_period, now()) |
+---------------------------------------+------------------+-------------------------------------------------------------------------------------------------------------------+


.. _config_security_compromise_options:

Config for SecurityCompromise
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SCDB provides some security compromise options, which can be selectively enabled when the security risk is acceptable to speed up the overall operation.

1. reveal_group_mark:
default disable, if enabled, SCDB will expose grouping information(size of each group) when calculating group-by-aggregation, thereby avoiding the overhead caused by pre-shuffle.  ``risk``: group size will be leaked, which is equivalent to the result of count(*)

A typical config of security_compromise can be like:

.. code-block:: yaml

  security_compromise:
    reveal_group_mark: false


.. _config_storage_options:

Config for storage
^^^^^^^^^^^^^^^^^^
Database in SCDB is used to store the SCQL system data, such as CCL and user information, currently SCDB support MySQL/SQLite3. You can connect to a database by setting ``conn_str`` and ``type`` in the storage config.

type
  The database type, which can be set as mysql/sqlite. And MySQL is recommended, which has been fully tested.

conn_str
  MySQL string format, see `dsn-data-source-name <https://github.com/mattn/go-sqlite3#connection-string>`_ for more information.

    [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]


  MySQL string example:

    ``user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true``

  SQLite3 string format:

    more infos: https://github.com/mattn/go-sqlite3#connection-string.

  SQLite3 string example:

    ``scdb.db``

A typical config of storage can be like:

.. code-block:: yaml

  storage:
    type: mysql
    conn_str: user_name:pass_word@tcp(127.0.0.1:3306)/db_name?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m

.. note::
  To handle time.Time correctly, you need to include parseTime as a parameter. To fully support UTF-8 encoding, you need to change ``charset=utf8`` to ``charset=utf8mb4``.


Config for audit
^^^^^^^^^^^^^^^^

Audit log are used to record the SCDB activities during query execution. It has two types: basic audit log and detail audit log.

  The basic audit log are used to record the basic information of a query, such as the result and user information, while the detail audit log records more detailed information such as execution plan and CCL details. you can see `scdb_audit <https://github.com/secretflow/scql/blob/main/pkg/audit/audit.proto>`_ for more information.

When the audit log has reaches the maximum size ( set by ``audit_max_size``), SCDB will save the audit log as ``name-timestamp.ext``, where the `name` is the filename set in ``audit_log_file`` without the extension, `timestamp` is the time at which the log was rotated formatted with local time format of `2006-01-02T15-04-05.000`,
`ext` is the extension set in ``audit_log_file``.

  For example, if ``audit_log_file`` set as `audit/audit.log`, a backup created at 6:30pm on Nov 11 2016 would be saved to `./audit/audit-2016-11-04T18-30-00.000.log`


Password check
^^^^^^^^^^^^^^
``password_check`` serves to validate password strength. For ALTER USER, CREATE USER statements, if it's true, the password should be at least 16 characters which including a number, a lowercase letter, a uppercase letter and a special character.


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

Additionally, it is necessary to configure the SCQLEngine to work with SSL, please refer :ref:`Config for SSL in SCQLEngine <scqlengine-tls>`.


Config for SPU
^^^^^^^^^^^^^^
SCQL supports different mpc protocol powered by SPU, you can choose different mpc protocol by setting SPU runtime config. Protocol **SEMI2K** is suggested, which is fully tested and support multi parties. See `SPU runtime config <https://www.secretflow.org.cn/docs/spu/en/reference/runtime_config.html>`_ to get more information.

.. code-block:: yaml

  spu: |
  {
    "protocol": "SEMI2K",
    "field": "FM64"
  }


.. _config_broker_server_options:

BrokerServer configuration options
==================================

BrokerServer, like SCDB, uses yaml files to configure parameters, The majority of their configuration items are the same.

Example for BrokerServer
------------------------

.. code-block:: yaml

  intra_server:
    port: 8080
  inter_server:
    host: 0.0.0.0
    port: 8081
    protocol: https
    cert_file: ${your cert file path}
    key_file: ${your key file path}
  log_level: debug
  party_code: alice
  party_info_file: "/home/admin/configs/party_info.json"
  private_pem_path: "/home/admin/configs/private_key.pem"
  intra_host: http://broker_alice:8080
  engines: ["engine_alice:8003"]
  engine:
    timeout: 120s
    protocol: http
    content_type: application/json
  storage:
    type: mysql
    conn_str: "user_name:pass_word@tcp(127.0.0.1:3306)/db_name?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m


Config in BrokerServer
----------------------

+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
|                 Name                  |  Default  |                                                       Description                                                       |
+=======================================+===========+=========================================================================================================================+
| intra_server.host                     | 127.0.0.1 | The host where BrokerServer listens for IntraServer requests, default localhost for safety                              |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| intra_server.port                     | none      | The port on which BrokerServer listens for IntraServer requests                                                         |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| intra_server.protocol                 | http      | The transfer protocol of IntraServer, supports HTTP/HTTPS                                                               |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| intra_server.cert_file                | none      | Certificate file path for IntraServer to enable HTTPS, supports crt/pem type                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| intra_server.key_file                 | none      | Private key file path for IntraServer to enable HTTPS, supports key/pem type                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_server.host                     | none      | The host where BrokerServer listens for InterServer requests                                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_server.port                     | none      | The port on which BrokerServer listens for InterServer requests                                                         |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_server.protocol                 | http      | The transfer protocol of InterServer, supports HTTP/HTTPS                                                               |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_server.cert_file                | none      | Certificate file path for InterServer to enable HTTPS, supports crt/pem type                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_server.key_file                 | none      | Private key file path for InterServer to enable HTTPS, supports key/pem type                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| inter_timeout                         | 5s        | Timeout for requesting InterServe                                                                             |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| log_level                             | info      | The type and severity of a logged event                                                                                 |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| party_code                            | none      | Unique identifier used to identify the party                                                                            |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| party_info_file                       | none      | File path that stores information of each party, including party code, public key and InterServer's URL                 |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| private_pem_path                      | none      | Private key file path for party_code, which will be used to sign requests to other BrokerServers                        |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| intra_host                            | none      | The callback URL for the local SCQLEngine to notify BrokerServer                                                        |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| engines                               | none      | The URLs for local available SCQLEngines                                                                                |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| engine.timeout                        | none      | Timeout for BrokerServer to send message to SCQLEngine                                                                  |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| engine.protocol                       | http      | The transfer protocol of SCQLEngine, support http/https                                                                 |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| engine.content_type                   | none      | The original media type in post body from BrokerServer to SCQLEngine                                                    |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| security_compromise.reveal_group_mark | false     | Whether to reveal group_mark directly for group by                                                                      |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.type                          | none      | Database kind in BrokerServer, supports MYSQL/SQLite3, MYSQL is recommended, SQLite3 may have problems with concurrency |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.conn_str                      | none      | Used to connect to a database                                                                                           |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.max_idle_conns                | 1         | Maximum number of connections in idle connection pool                                                                   |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.max_open_conns                | 1         | Maximum number of open connections to the database                                                                      |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_idle_time            | -1s       | Maximum amount of time a connection may be idle                                                                         |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_lifetime             | -1s       | Maximum amount of time a connection may be reused                                                                       |
+---------------------------------------+-----------+-------------------------------------------------------------------------------------------------------------------------+

Config for ServerConfig
^^^^^^^^^^^^^^^^^^^^^^^
BrokerServer accept intra-domain requests through IntraServer, while accept requests between different BrokerServers through InterServer.

IntraServer is recommended to use localhost host or LAN address to avoid external attacks, while InterServer is recommended to enable HTTPS to improve security.


Reused Config
^^^^^^^^^^^^^

For more about SecurityCompromise, see :ref:`Config for SecurityCompromise <config_security_compromise_options>`

For more about Storage, see :ref:`Config for storage <config_storage_options>`


.. _engine_config_options:

SCQLEngine configuration options
================================
SCQLEngine uses Gflags to manage configurations when SCQLEngine set up.

Example for SCQLEngine
----------------------

.. code-block::

  # Config for Brpc server
  --listen_port=8003
  # Config for datasource
  --datasource_router=embed
  --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"${connection_str}"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}


Config in SCQLEngine
--------------------

+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
|                    Name                    |     Default      |                                        Description                                         |
+============================================+==================+============================================================================================+
| log_enable_console_logger                  | true             | Whether logging to stdout while logging to file                                            |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| log_dir                                    | logs             | The directory to save log file                                                             |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_audit_logger                        | true             | Whether to enable audit log                                                                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| audit_log_file                             | audit/audit.log  | The file to save basic information about a query                                           |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| audit_detail_file                          | audit/detail.log | The file to save more detailed information about a query                                   |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| audit_max_files                            | 180              | The maximum number of old audit log files to retain                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_protocol                       | `http:proto`     | The rpc protocol between engine and engine                                                 |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_connection_type                | pooled           | The rpc connection type between engine and engine                                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_timeout_ms                     | 300000           | The rpc timeout between engine and engine, unit: ms                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_max_retry                      | 3                | Rpc max retries(not including the first rpc) between engine and engine                     |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_enable_ssl_as_client           | true             | Whether enable ssl encryption when send message to another engine                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_enable_ssl_client_verification | false            | Whether enable certificate verification when send message to another engine                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| peer_engine_ssl_client_ca_certificate      | none             | The trusted CA file to verify certificate when send message to another engine              |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| link_recv_timeout_ms                       | 30000            | The max time that engine will wait for message come from another engine                    |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_protocol                              | `http:proto`     | The rpc protocol between engine and SCDB                                                   |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_connection_type                       | pooled           | The rpc connection type between engine and SCDB                                            |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_timeout_ms                            | 5000             | The rpc timeout between engine and SCDB, unit: ms                                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_max_retry                             | 3                | Rpc max retries(not including the first rpc) between engine and SCDB                       |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_enable_ssl_as_client                  | true             | Whether enable ssl encryption when send message to SCDB                                    |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_enable_ssl_client_verification        | false            | Whether enable certificate verification when send message to SCDB                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| scdb_ssl_client_ca_certificate             | none             | The trusted CA file to verify certificate when send message to SCDB                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| listen_port                                | 8003             | The listening port of engine service                                                       |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_builtin_service                     | false            | Whether enable brpc builtin service                                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| internal_port                              | 9527             | The listening port of brpc builtin services                                                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| idle_timeout_s                             | 30               | Idle connection close delay in seconds between the engine and SCDB, unit: s                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| server_enable_ssl                          | true             | Whether enable SSL when engine work as a server                                            |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| server_ssl_certificate                     | none             | Certificate file path to enable SSL when engine work as a server                           |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| server_ssl_private_key                     | none             | Private key file path to enable SSL when engine work as a server                           |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_client_authorization                | false            | Whether check requests' http header when engine work as a server                           |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| auth_credential                            | none             | Authorization credential used to check requests' http header                               |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_scdb_authorization                  | false            | Whether to authenticate the identity of SCDB                                               |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| engine_credential                          | none             | Credential used to authenticate SCDB                                                       |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| session_timeout_s                          | 1800             | Expiration duration of a session between engine and SCDB, unit: s                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| datasource_router                          | embed            | The datasource router type                                                                 |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| embed_router_conf                          | none             | Configuration for embed router in json format                                              |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| db_connection_info                         | none             | Connection string used to connect to mysql                                                 |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_he_schema_type_ou                   | false            | Whether to use OU to speed up HeSum, use ZPaillier by default for security, see: `heu/ou`_ |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_self_auth                           | true             | Whether enable self identity authentication                                                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| private_key_pem_path                       | none             | Path to private key pem file                                                               |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_peer_auth                           | true             | Whether enable peer parties identity authentication                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| authorized_profile_path                    | none             | Path to authorized profile, in json format                                                 |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+

.. _datasource_router:

Config for datasource
^^^^^^^^^^^^^^^^^^^^^
datasources(MySQL/SQLite3/PostgreSQL/CSVDB/ArrowSQL) are where the SCQLEngine gets its data from.

``datasource_router`` is design to support multi datasources, currently only supported: embed, which is initialized with ``embed_router_conf`` first, a json string like::

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

if ``embed_router_conf`` is empty, embed_router will try to initialized with ``db_connection_info``.

Embed router
""""""""""""
datasources in embed_router_conf contain information for connecting MySQL/SQLite3/PostgreSQL/CSVDB/ArrowSQL:

  id: unique id of datasource.

  name: custom description help to distinguish datasources.

  kind: datasource type, currently support MySQL/SQLite3/PostgreSQL/CSVDB/ArrowSQL.

  connection_str: string used to connect MySQL/SQLite3/PostgreSQL/CSVDB/ArrowSQL.

    MySQL Connection string format:
      <str> == <assignment> | <assignment> ';' <str>

      <assignment> == <name> '=' <value>

      <name> == 'host' | 'port' | 'user' | 'password' | 'db' | 'compress' | 'auto-reconnect' | 'reset' | 'fail-readonly'

      <value> == [~;]*

    MySQL Connection string e.g:
      ``db=${db};user=${user};password=${password};host=${host}``

    SQLite3 Connection string format:
      more infos: https://www.sqlite.org/c3ref/open.html

    SQLite3 Connection string e.g:
      ``file:data_test.db?mode=memory&cache=shared``

    PostgreSQL Connection string format:
      <str> == <assignment> | <assignment> ' ' <str>

      <assignment> == <name> '=' <value>

      <name> == 'host' | 'port' | 'user' | 'password' | 'dbname' | 'connect_timeout'

      <value> == [~;]*

    PostgreSQL Connection string e.g:
      ``db=${db};user=${user};password=${password};host=${host}``

    CSVDB Connection string format:
      Since connection_str is an object in another json object, the format is a converted json string corresponding to `CsvdbConf <https://github.com/secretflow/scql/tree/main/engine/datasource/csvdb_conf.proto>`_

    CSVDB Connection string e.g:
      "{\\\"db_name\\\":\\\"csvdb\\\",\\\"tables\\\":[{\\\"table_name\\\":\\\"staff\\\",\\\"data_path\\\":\\\"test.csv\\\",\\\"columns\\\":[{\\\"column_name\\\":\\\"id\\\",\\\"column_type\\\":\\\"1\\\"}]}]}"

    ArrowSQL Connection string format:
      grpc+<scheme>://host:port

      <scheme> == 'tcp' | 'tls'

    ArrowSQL Connection string e.g:
      ``grpc+tcp://127.0.0.1:6666``

      .. note::
        As a datasource embedded in SCQLEngine, ArrowSQL requires an additional gRPC server which provides the corresponding interface for executing an ad-hoc query in `Arrow Flight SQL <https://arrow.apache.org/docs/format/FlightSql.html>`_

Routing rules
"""""""""""""
embed_router's rules support wildcard ``*`` , when given a table in format: *database_name:table_name*,
embed_router will route to the corresponding datasource by

1. find the exact rules first, whose ``${db}:${table}`` equals to *database_name:table_name*;
2. try the database_name:\* rules;
3. try \*:table_name in the end.

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
If you want to enable SSL in SCQLEngine, add FLAGS as follows. Additionally, it may be necessary to configure SCDB work with TLS please refer :ref:`Config for TLS in SCDB <scdb-tls>`.

.. code-block::

  --server_enable_ssl=true
  --server_ssl_certificate=${file path of cert}
  --server_ssl_private_key=${file path of key}
  --peer_engine_enable_ssl_as_client=true
  --scdb_enable_ssl_as_client=true

Config for audit
^^^^^^^^^^^^^^^^
The audit log in SCQLEngine is used to record the SCQLEngine activities during the execution of tasks from SCDB. Just like the audit in SCDB, it also can be divided into two types: common audit log and detail audit log.

  The common audit is used to record some basic information about a task, while the detail audit is used to record more detailed information of the task. See `engine_audit <https://github.com/secretflow/scql/blob/main/engine/audit/audit.proto>`_ for more information

The log file is rotated in every 24:00:00 in local time, and the filename is generated in the format ``name-date.ext``, where `name` is the filename set in ``audit_log_file`` without the extension, `date` is the time at which the log was rotated formatted with local time format of `YYYY-MM-DD`,
`ext` is the extension set in ``audit_log_file``.

  For example, if you set ``audit_log_file`` as `audit/audit.log`, a backup created on Nov 11 2016 would be saved to `/audit/audit_2016-11-04.log`

Config for party authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For security, SCQLEngine enables party authentication by default. SCQLEngine will check it's public key in the SCDB request matches the local public key in ``private_key_pem_path``, and that the other participant's public key also matches the one in ``authorized_profile_path``.

.. _heu/ou: https://www.secretflow.org.cn/docs/heu/latest/zh-Hans/getting_started/algo_choice#ou-paillier

.. _replay attacks: https://en.wikipedia.org/wiki/Replay_attack
