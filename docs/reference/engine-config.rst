
.. _engine_config_options:

SCQLEngine Configuration
------------------------

SCQLEngine uses Gflags to manage configurations when SCQLEngine set up.

Example configuration for SCQLEngine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

  # Config for Brpc server
  --listen_port=8003
  # Config for datasource
  --datasource_router=embed
  --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"${connection_str}"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}


Configuration Options of SCQLEngine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SCQLEngine can cooperate with upper-layer modules such as SCDB and SCQLBroker according to the deployment mode. ``Driver`` is used in the configuration items to represent these upper-layer modules.

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
| driver_protocol                            | `http:proto`     | The rpc protocol between engine and Driver                                                 |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_connection_type                     | pooled           | The rpc connection type between engine and Driver                                          |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_timeout_ms                          | 5000             | The rpc timeout between engine and Driver, unit: ms                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_max_retry                           | 3                | Rpc max retries(not including the first rpc) between engine and Driver                     |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_enable_ssl_as_client                | true             | Whether enable ssl encryption when send message to Driver                                  |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_enable_ssl_client_verification      | false            | Whether enable certificate verification when send message to Driver                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| driver_ssl_client_ca_certificate           | none             | The trusted CA file to verify certificate when send message to Driver                      |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| listen_port                                | 8003             | The listening port of engine service                                                       |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| enable_builtin_service                     | false            | Whether enable brpc builtin service                                                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| internal_port                              | 9527             | The listening port of brpc builtin services                                                |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| idle_timeout_s                             | 30               | Idle connection close delay in seconds between the engine and Driver, unit: s              |
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
| enable_driver_authorization                | false            | Whether to authenticate the identity of Driver                                             |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| engine_credential                          | none             | Credential used to authenticate Driver                                                     |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| session_timeout_s                          | 1800             | Expiration duration of a session between engine and Driver, unit: s                        |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| datasource_router                          | embed            | The datasource router type, "embed" or "http"                                              |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| embed_router_conf                          | none             | Configuration for embed router in json format                                              |
+--------------------------------------------+------------------+--------------------------------------------------------------------------------------------+
| http_router_endpoint                       | none             | http datasource router endpoint, it is valid only when --datasource_router=http            |
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
SCQLEngine uses **Brpc** to communicate with Driver and other peer SCQLEngines, each SCQLEngine will start a Brpc service on *local-host:listen_port* to receive data from outside. If you want to enable Brpc builtin services, add FLAGS:

.. code-block::

  --enable_builtin_service=true
  --internal_port=9527


.. _scqlengine-tls:

Config for SSL
^^^^^^^^^^^^^^
If you want to enable SSL in SCQLEngine, add FLAGS as follows. Additionally, it may be necessary to configure the Driver to work with TLS, please refer :ref:`TLS in SCDB <scdb-tls>` or :ref:`TLS in SCQLBroker <broker-tls>`.

.. code-block::

  --server_enable_ssl=true
  --server_ssl_certificate=${file path of cert}
  --server_ssl_private_key=${file path of key}
  # set peer_engine_enable_ssl_as_client to true when peer SCQLEngine has https enabled
  --peer_engine_enable_ssl_as_client=true
  # set driver_enable_ssl_as_client to true when the Driver has https enabled (SCDB or SCQLBroker's IntraServer)
  --driver_enable_ssl_as_client=true

Config for audit
^^^^^^^^^^^^^^^^
The audit log in SCQLEngine is used to record the SCQLEngine activities during the execution of tasks from Driver. Just like the audit in Driver, it also can be divided into two types: common audit log and detail audit log.

  The common audit is used to record some basic information about a task, while the detail audit is used to record more detailed information of the task. See `engine_audit <https://github.com/secretflow/scql/blob/main/engine/audit/audit.proto>`_ for more information

The log file is rotated in every 24:00:00 in local time, and the filename is generated in the format ``name-date.ext``, where `name` is the filename set in ``audit_log_file`` without the extension, `date` is the time at which the log was rotated formatted with local time format of `YYYY-MM-DD`,
`ext` is the extension set in ``audit_log_file``.

  For example, if you set ``audit_log_file`` as `audit/audit.log`, a backup created on Nov 11 2016 would be saved to `/audit/audit_2016-11-04.log`

Config for party authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For security, SCQLEngine enables party authentication by default. SCQLEngine will check it's public key in the Driver request matches the local public key in ``private_key_pem_path``, and that the other participant's public key also matches the one in ``authorized_profile_path``.

.. _heu/ou: https://www.secretflow.org.cn/docs/heu/latest/zh-Hans/getting_started/algo_choice#ou-paillier

.. _replay attacks: https://en.wikipedia.org/wiki/Replay_attack
