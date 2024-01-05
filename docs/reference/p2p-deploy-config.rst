SCQL P2P Deployment Configuration
=================================

SCQL P2P deployment architecture consists of SCQLBroker and SCQLEngine, and the configuration instructions include these two parts.


.. _config_broker_server_options:

SCQLBroker Configuration
------------------------

SCQLBroker uses YAML as configuration file format.

Example configuration for SCQLBroker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

  intra_server:
    port: 8080
  inter_server:
    host: 0.0.0.0
    port: 8081
    protocol: https
    cert_file: ${your cert file path}
    key_file: ${your key file path}
  log_level: info
  party_code: alice
  session_expire_time: 24h
  session_expire_check_time: 1m
  party_info_file: "/home/admin/configs/party_info.json"
  private_pem_path: "/home/admin/configs/private_key.pem"
  intra_host: http://broker_alice:8080
  engine:
    timeout: 120s
    protocol: http
    content_type: application/json
    uris:
      - for_peer: alice_for_peer:8003
        for_self: alice_for_self:8003
  storage:
    type: mysql
    conn_str: "user_name:pass_word@tcp(127.0.0.1:3306)/db_name?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m


Configuration Options of SCQLBroker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
|                 Name                  |  Default  |                                                       Description                                                        |
+=======================================+===========+==========================================================================================================================+
| intra_server.host                     | 127.0.0.1 | The host where SCQLBroker listens for IntraServer requests, default localhost for safety                                 |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| intra_server.port                     | none      | The port on which SCQLBroker listens for IntraServer requests                                                            |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| intra_server.protocol                 | http      | The transfer protocol of IntraServer, supports HTTP/HTTPS                                                                |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| intra_server.cert_file                | none      | Certificate file path for IntraServer to enable HTTPS, supports crt/pem type                                             |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| intra_server.key_file                 | none      | Private key file path for IntraServer to enable HTTPS, supports key/pem type                                             |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_server.host                     | none      | The host where SCQLBroker listens for InterServer requests                                                               |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_server.port                     | none      | The port on which SCQLBroker listens for InterServer requests                                                            |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_server.protocol                 | http      | The transfer protocol of InterServer, supports HTTP/HTTPS                                                                |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_server.cert_file                | none      | Certificate file path for InterServer to enable HTTPS, supports crt/pem type                                             |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_server.key_file                 | none      | Private key file path for InterServer to enable HTTPS, supports key/pem type                                             |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| inter_timeout                         | 5s        | Timeout for requesting InterServe                                                                                        |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| log_level                             | info      | The type and severity of a logged event                                                                                  |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| party_code                            | none      | Unique identifier used to identify the party                                                                             |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| party_info_file                       | none      | File path that stores information of each party, including party code, public key and InterServer's URL                  |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| private_pem_path                      | none      | Private key file path for party_code, which will be used to sign requests to other SCQLBrokers                           |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| intra_host                            | none      | The callback URL for the local SCQLEngine to notify SCQLBroker                                                           |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| engine.timeout                        | none      | Timeout for SCQLBroker to send message to SCQLEngine                                                                     |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| engine.protocol                       | http      | The transfer protocol of SCQLEngine, support http/https                                                                  |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| engine.content_type                   | none      | The original media type in post body from SCQLBroker to SCQLEngine                                                       |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| engine.uris                           | none      | The URIs for local SCQLEngines, which using **for_peer** to serve peer engines and **for_self** to serve the SCQLBroker, |
|                                       |           | if **for_self** is empty, **for_peer** is used instead                                                                   |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| security_compromise.reveal_group_mark | false     | Whether to reveal group_mark directly for group by                                                                       |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.type                          | none      | Database kind in SCQLBroker, supports MYSQL/SQLite3, MYSQL is recommended, SQLite3 may have problems with concurrency    |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.conn_str                      | none      | Used to connect to a database                                                                                            |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.max_idle_conns                | 1         | Maximum number of connections in idle connection pool                                                                    |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.max_open_conns                | 1         | Maximum number of open connections to the database                                                                       |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_idle_time            | -1s       | Maximum amount of time a connection may be idle                                                                          |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| storage.conn_max_lifetime             | -1s       | Maximum amount of time a connection may be reused                                                                        |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| session_expire_time                   | 24h       | Maximum lifespan of a job                                                                                                |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+
| session_expire_check_time             | 1m        | The interval checking whether session is timeout                                                                         |
+---------------------------------------+-----------+--------------------------------------------------------------------------------------------------------------------------+


Config for ServerConfig
^^^^^^^^^^^^^^^^^^^^^^^
SCQLBroker accept intra-domain requests through IntraServer, while accept requests between different SCQLBrokers through InterServer.

IntraServer is recommended to use localhost host or LAN address to avoid external attacks, while InterServer is recommended to enable HTTPS to improve security.

.. _broker-tls:

Please refer to the following configuration to enable HTTPS for InterServer: (similar to IntraServer)

.. code-block:: yaml

  inter_server:
    host: 0.0.0.0
    port: 8081
    protocol: https
    cert_file: ${your cert file path}
    key_file: ${your key file path}

.. note::

  Self-signed CA files may not be trusted by default, please refer to `Trouble shooting <https://github.com/secretflow/scql/tree/main/test-tools#trouble-shooting>`_ for help.

  Please change the endpoints in **party_info.json** from http to https.

  We have enabled HTTPS in the `p2p examples <https://github.com/secretflow/scql/blob/main/examples/p2p-tutorial/README.md>`_, and the initialization process of related configurations may provide some help.

For SCQLEngine to work with SSL, please refer :ref:`Config for SSL in SCQLEngine <scqlengine-tls>`.

Reused Config
^^^^^^^^^^^^^

For more about SecurityCompromise, see :ref:`Config for SecurityCompromise <config_security_compromise_options>`

For more about Storage, see :ref:`Config for storage <config_storage_options>`



.. include:: /reference/engine-config.rst
