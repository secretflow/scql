SCQL Centralized Deployment Configuration
=========================================

SCQL centralized deployment architecture consists of SCDB and SCQLEngine, and the configuration instructions include these two parts.

.. _scdb_config_options:

SCDB Configuration
------------------

SCDB uses YAML as configuration file format.

Example configuration for SCDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

  scdb_host: http://localhost:8080
  port: 8080
  protocol: http
  query_result_callback_timeout: 200ms
  session_expire_time: 1h
  session_expire_check_time: 100ms
  password_check: true
  log_level: info
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
          "field": "FM64"
        }
  party_auth:
    method: pubkey
    enable_timestamp_check: true
    validity_period: 1m


Configuration Options of SCDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
|                 Name                  |     Default      |                                                                   Description                                                                    |
+=======================================+==================+==================================================================================================================================================+
| scdb_host                             | none             | The callback URL for the SCQLEngine to notify SCDB                                                                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| port                                  | none             | The listening port of the SCDB server                                                                                                            |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| protocol                              | https            | The transfer protocol of SCDB server, supports HTTP/HTTPS                                                                                        |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| query_result_callback_timeout         | 200ms            | Timeout for SCDB to notify the query result                                                                                                      |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| session_expire_time                   | 48h              | The expiration duration of a session                                                                                                             |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| session_expire_check_time             | 1h               | The cleanup interval of expired session                                                                                                          |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| password_check                        | true             | Whether to validate password strength when create a user                                                                                         |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| log_level                             | info             | The type and severity of a logged event                                                                                                          |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| enable_audit_logger                   | true             | Whether to enable audit logger                                                                                                                   |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_log_file                  | audit/audit.log  | The file to save basic information about a query                                                                                                 |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_detail_file               | audit/detail.log | The file to save more detailed information about a query                                                                                         |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_size                  | 500              | The maximum size of the audit log file before it gets rotated, unit: MB                                                                          |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_backups               | 0                | The maximum number of old audit log files to retain, default 0 to retain all old log files                                                       |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_age_days              | 180              | The maximum number of days to retain old log files                                                                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| audit.audit_max_compress              | false            | Whether the rotated log files should be compressed                                                                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| security_compromise.reveal_group_mark | false            | Whether to reveal group_mark directly for group by                                                                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| tls.cert_file                         | none             | Certificate file path to enable TSL, supports crt/pem type                                                                                       |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| tls.key_file                          | none             | Private key file path to enable TSL, supports key/pem type                                                                                       |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.type                          | none             | Database kind in SCDB, supports MYSQL/SQLite3, MYSQL is recommended, SQLite3 may have problems with concurrency                                  |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.conn_str                      | none             | Used to connect to a database                                                                                                                    |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.max_idle_conns                | 1                | Maximum number of connections in idle connection pool                                                                                            |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.max_open_conns                | 1                | Maximum number of open connections to the database                                                                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_idle_time            | -1s              | Maximum amount of time a connection may be idle                                                                                                  |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_lifetime             | -1s              | Maximum amount of time a connection may be reused                                                                                                |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.mode                           | none             | Engine client mode, supports GRPC and HTTP/HTTPS. In GRPC mode, tls_cfg is required. In HTTP/HTTPS mode, protocol and content_type are necessary |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.tls.mode                       | none             | Tls mode for engine client: *NOTLS*/TLS/MTLS.                                                                                                    |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.tls.cert_file                  | none             | The path to the file containing informations about the certificate for engine client                                                             |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.tls.key_file                   | none             | The path to the file containing informations about the private key for engine client                                                             |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.tls.cacert_file                | none             | The path to the file containing informations about the ca certificate for engine client                                                          |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.timeout                        | none             | Timeout for SCDB to send message to engine                                                                                                       |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.protocol                       | https            | The transfer protocol of Engine, support http/https                                                                                              |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.content_type                   | none             | The original media type in post body from SCDB to engine                                                                                         |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.spu.protocol                   | none             | The mpc protocol for engine to work with                                                                                                         |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.spu.field                      | none             | A security parameter type for engine to work with                                                                                                |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| engine.spu.sigmoid_mode               | none             | The sigmoid approximation method for engine to work with                                                                                         |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| party_auth.method                     | pubkey           | Method to authenticate the participant when registering a user, supports pubkey/token                                                            |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| party_auth.enable_timestamp_check     | true             | When using pubkey authentication, whether to check the signed timestamp to avoid `replay attacks`_                                               |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+
| party_auth.validity_period            | 30s              | When enable timestamp check,  SCDB will check whether signed timestamp is within (now() - validity_period, now())                                |
+---------------------------------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+


.. _config_security_compromise_options:

Config for SecurityCompromise
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SCQL provides some security compromise options, which can be selectively enabled when the security risk is acceptable to speed up the overall operation.

1. **reveal_group_mark**: default disable, if enabled, SCQL will expose grouping information(size of each group) when calculating group-by-aggregation, thereby avoiding the overhead caused by pre-shuffle.  ``risk``: group size will be leaked, which is equivalent to the result of count(*)
2. **group_by_threshold**: minimum amount of data within a group that will be retained(effective only for the group by syntax, and not for distinct, etc.)

A typical config of security_compromise can be like:

.. code-block:: yaml

    security_compromise:
      reveal_group_mark: false
      group_by_threshold: 4


.. _config_storage_options:

Config for storage
^^^^^^^^^^^^^^^^^^
Database is used to store the SCQL system data, such as CCL and user information, currently support MySQL/SQLite3. You can connect to a database by setting ``conn_str`` and ``type`` in the storage config.

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

.. note::

  Self-signed CA files may not be trusted by default, please refer to `Trouble shooting <https://github.com/secretflow/scql/tree/main/test-tools#trouble-shooting>`_ for help.

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




.. include:: /reference/engine-config.rst
