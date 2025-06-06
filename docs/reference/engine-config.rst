
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

+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
|                      Name                       |       Default       |                                                               Description                                                               |
+=================================================+=====================+=========================================================================================================================================+
| log_enable_console_logger                       | true                | Whether logging to stdout while logging to file                                                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| log_enable_session_logger_separation            | false               | Whether output session-related logs to a dedicated file                                                                                 |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| log_dir                                         | logs                | The directory to save log file                                                                                                          |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| log_level                                       | info                | Log level, can be trace/debug/info/warning/error/critical/off                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_protocol                            | baidu_std           | The rpc protocol between engine and engine                                                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_connection_type                     | single              | The rpc connection type between engine and engine                                                                                       |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_load_balancer                       | none                | The rpc load balancer between engine and engine, can be rr or empty string                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_timeout_ms                          | 300000              | The rpc timeout between engine and engine, unit: ms                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_max_retry                           | 3                   | Rpc max retries(not including the first rpc) between engine and engine                                                                  |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_enable_ssl_as_client                | true                | Whether enable ssl encryption when send message to another engine                                                                       |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_enable_ssl_client_verification      | false               | Whether enable certificate verification when send message to another engine                                                             |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| peer_engine_ssl_client_ca_certificate           | none                | The trusted CA file to verify certificate when send message to another engine                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| link_recv_timeout_ms                            | 30000               | The max time that engine will wait for message come from another engine                                                                 |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| link_throttle_window_size                       | 16                  | Throttle window size for channel, set to limit the number of messages sent asynchronously to avoid network congestion, set 0 to disable |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| link_chunked_send_parallel_size                 | 1                   | Parallel size when send chunked value                                                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| http_max_payload_size                           | 1048576             | Max payload to decide whether to send value chunked, default 1MB                                                                        |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_protocol                                 | `http:proto`        | The rpc protocol between engine and Driver                                                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_connection_type                          | pooled              | The rpc connection type between engine and Driver                                                                                       |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_load_balancer                            | none                | The rpc load balancer between engine and Driver, can be rr or empty string                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_timeout_ms                               | 5000                | The rpc timeout between engine and Driver, unit: ms                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_max_retry                                | 3                   | Rpc max retries(not including the first rpc) between engine and Driver                                                                  |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_enable_ssl_as_client                     | true                | Whether enable ssl encryption when send message to Driver                                                                               |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_enable_ssl_client_verification           | false               | Whether enable certificate verification when send message to Driver                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| driver_ssl_client_ca_certificate                | none                | The trusted CA file to verify certificate when send message to Driver                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| listen_port                                     | 8003                | The listening port of engine service                                                                                                    |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_builtin_service                          | false               | Whether enable brpc builtin service                                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| internal_port                                   | 9527                | The listening port of brpc builtin services                                                                                             |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_separate_link_port                       | false               | Whether use a separate port for link service                                                                                            |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| link_port                                       | 8004                | Port for link service                                                                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| idle_timeout_s                                  | 30                  | Idle connection close delay in seconds between the engine and Driver, unit: s                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| server_enable_ssl                               | true                | Whether enable SSL when engine work as a server                                                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| server_ssl_certificate                          | none                | Certificate file path to enable SSL when engine work as a server                                                                        |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| server_ssl_private_key                          | none                | Private key file path to enable SSL when engine work as a server                                                                        |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_client_authorization                     | false               | Whether check requests' http header when engine work as a server                                                                        |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| auth_credential                                 | none                | Authorization credential used to check requests' http header                                                                            |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_driver_authorization                     | false               | Whether to authenticate the identity of Driver                                                                                          |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| engine_credential                               | none                | Credential used to authenticate Driver                                                                                                  |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| session_timeout_s                               | 1800                | Expiration duration of a session between engine and Driver, unit: s                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| spu_allowed_protocols                           | SEMI2K,ABY3,CHEETAH | SPU allowed protocols                                                                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| datasource_router                               | embed               | The datasource router type, "embed" or "http"                                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| embed_router_conf                               | none                | Configuration for embed router in json format                                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| http_router_endpoint                            | none                | http datasource router endpoint, it is valid only datasource_router is set to "http"                                                    |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| kuscia_datamesh_endpoint                        | datamesh:8071       | Kuscia datamesh grpc endpoint                                                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| kuscia_datamesh_client_key_path                 | none                | Kuscia datamesh client key file                                                                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| kuscia_datamesh_client_cert_path                | none                | Kuscia datamesh client cert file                                                                                                        |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| kuscia_datamesh_cacert_path                     | none                | Kuscia datamesh server cacert file                                                                                                      |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| db_connection_info                              | none                | Connection string used to connect to mysql                                                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_he_schema_type_ou                        | false               | Whether to use OU to speed up HeSum, use ZPaillier by default for security, see: `heu/ou`_                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_self_auth                                | true                | Whether enable self identity authentication                                                                                             |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| private_key_pem_path                            | none                | Path to private key pem file                                                                                                            |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_peer_auth                                | true                | Whether enable peer parties identity authentication                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| authorized_profile_path                         | none                | Path to authorized profile, in json format                                                                                              |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_psi_detail_logger                        | false               | Whether enable detail log                                                                                                               |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| psi_detail_logger_dir                           | logs/detail         | Detail log directory                                                                                                                    |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_restricted_read_path                     | true                | Whether restrict path for file to read                                                                                                  |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| restricted_read_path                            | ./data              | In where the file is allowed to read if enable restricted read path                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| csv_null_str                                    | NULL                | Specifies the string that represents a NULL value when reading csv                                                                      |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_restricted_write_path                    | true                | Whether restrict path for file to write                                                                                                 |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| restricted_write_path                           | ./data              | In where the file is allowed to write if enable restricted write path                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| null_string_to_write                            | NULL                | The string to write for NULL values                                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_endpoint                              | none                | The endpoint of output s3/minio/oss                                                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_access_key                            | none                | The access key id of output s3/minio/oss                                                                                                |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_secret_key                            | none                | The secret access key of output s3/minio/oss                                                                                            |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_enalbe_ssl                            | true                | Default enable ssl, if s3 server not enable ssl, set to false                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_ca_dir_path                           | /etc/ssl/certs/     | Directory where the certificates stored to verify s3 server                                                                             |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_s3_force_virtual_addressing              | true                | Default set to true to work with oss, for minio please set to false                                                                     |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_db_kind                                  | none                | The kind of output db, support mysql/sqlite/postgresql                                                                                  |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| output_db_connection_str                        | none                | The :ref:`connection string <connection_str>` to connect to output db                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| psi_curve_type                                  | 2                   | Curve type used in PSI, default 2: CURVE_FOURQ, for more see `psi curve type`_                                                          |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| unbalance_psi_ratio_threshold                   | 5                   | Mininum ratio (larger party's rows count / smaller's) to trigger unbalanced PSI                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| unbalance_psi_larger_party_rows_count_threshold | 81920               | Minimum rows count of the larger party to choose unbalanced PSI                                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| provider_batch_size                             | 8192                | Batch size used in PSI Provider                                                                                                         |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| detail_logger_sample_num                        | 0                   | Sample number for detail logger, 0 means print all, default 0                                                                           |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| max_chunk_size                                  | 134217728           | Max chunk size for spu value proto, default 128MB                                                                                       |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| enable_tensor_life_cycle_manage                 | true                | Whether tensor life cycle manage is enable/disable                                                                                      |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| arrow_client_disable_server_verification        | false               | Whether disable server verification for ArrowSQL adaptor                                                                                |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| arrow_cert_pem_path                             | none                | Certificate file path for server verification when arrow_client_disable_server_verification is false                                    |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| arrow_client_key_pem_path                       | none                | Private key file path for ArrowSQL client to work in mtls                                                                               |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| arrow_client_cert_pem_path                      | none                | Certificate file path for ArrowSQL client to work in mtls                                                                               |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| tmp_file_path                                   | /tmp                | The path for temporarily storing local data in streaming mode.                                                                          |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| streaming_row_num_threshold                     | 30000000            | Minimum row num to use streaming mode                                                                                                   |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| batch_row_num                                   | 10000000            | Max row num in one batch                                                                                                                |
+-------------------------------------------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------+

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

.. _connection_str:

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
      ``file:/path/to/data.db``

    PostgreSQL Connection string format:
      <str> == <assignment> | <assignment> ' ' <str>

      <assignment> == <name> '=' <value>

      <name> == 'host' | 'port' | 'user' | 'password' | 'dbname' | 'connect_timeout'

      <value> == [~;]*

    PostgreSQL Connection string e.g:
      ``dbname=${db} user=${user} password=${password} host=${host} port=${port}``

    CSVDB Connection string format:
      CSVDB support read csv from local and OSS/MinIO, since connection_str is an object in another json object, the format is a converted json string corresponding to `CsvdbConf <https://github.com/secretflow/scql/tree/main/engine/datasource/csvdb_conf.proto>`_

    CSVDB Connection string e.g:
      local csv: "{\\\"db_name\\\":\\\"csvdb\\\",\\\"tables\\\":[{\\\"table_name\\\":\\\"staff\\\",\\\"data_path\\\":\\\"test.csv\\\",\\\"columns\\\":[{\\\"column_name\\\":\\\"id\\\",\\\"column_type\\\":\\\"string\\\"}]}]}"

      OSS csv: "{\\\"db_name\\\":\\\"csvdb\\\",\\\"s3_conf\\\":{\\\"endpoint\\\":\\\"test_endpoint\\\",\\\"access_key_id\\\":\\\"test_id\\\",\\\"secret_access_key\\\":\\\"test_key\\\",\\\"virtualhost\\\": true },\\\"tables\\\":[{\\\"table_name\\\":\\\"staff\\\",\\\"data_path\\\":\\\"oss://test_bucket/test.csv\\\",\\\"columns\\\":[{\\\"column_name\\\":\\\"id\\\",\\\"column_type\\\":\\\"string\\\"}]}]}"

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

Config for party authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For security, SCQLEngine enables party authentication by default. SCQLEngine will check it's public key in the Driver request matches the local public key in ``private_key_pem_path``, and that the other participant's public key also matches the one in ``authorized_profile_path``.

.. _heu/ou: https://www.secretflow.org.cn/docs/heu/getting_started/algo_choice#ou-paillier

.. _psi curve type: https://www.secretflow.org.cn/en/docs/psi/main/reference/psi_config#curvetype

.. _replay attacks: https://en.wikipedia.org/wiki/Replay_attack
