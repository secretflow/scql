==============
P2P Deployment
==============

This document describes how to deploy a SCQL system with docker in P2P mode and use brokerctl to query. It is basically the same as the :doc:`/intro/p2p-tutorial`, but deployed on multi machines.

Before start this doc, we assume that the reader has some experience using the docker-compose utility. If you are new to Docker Compose, please consider reviewing the `official Docker Compose overview <https://docs.docker.com/compose/>`_, or checking out the `Getting Started guide <https://docs.docker.com/compose/gettingstarted/>`_.

Deployment Diagram
==================

The deployment diagram of the SCQL system that we plan to deploy is shown as the following figure, it involves parties named ``Alice`` and ``Bob``. We use two machines to simulate different parties.

.. image:: /imgs/p2p_deploy.png

.. note::
    1. The SCQLBrokers are served through the HTTP protocol. It is recommended to use HTTPS instead in production environments.


Step 1: Deployment for Alice
============================

Here we present how to deploy components for party Alice.

1.1 Create a Workspace
-----------------------

.. code-block:: bash

    mkdir scql-p2p
    cd scql-p2p

1.2 Prepare Meta Data and Source Data
-------------------------------------

To simplify, We use a mysql container to store the SCQLBroker's meta data and SCQLEngine's source data. However, if you prefer, you can use your preferred database service or store both types of data separately.

The source data can be stored in a file called ``alice_init.sql`` with content like `alice_init.sql <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial/mysql/initdb/alice_init.sql>`_. For Bob, please use `bob_init.sql <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial/mysql/initdb/bob_init.sql>`_ instead.

The meta data can be stored in ``broker_init_alice.sql`` with content like `broker_init_alice.sql <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial/mysql/initdb/broker_init_alice.sql>`_. For Bob, please use `broker_init_bob.sql <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial/mysql/initdb/broker_init_bob.sql>`_ instead.

These files can also be obtained via the command-line with either curl, wget or another similar tool.

.. code-block:: bash

  # For Bob, please use command: wget raw.githubusercontent.com/secretflow/scql/main/examples/p2p-tutorial/mysql/initdb/bob_init.sql
  wget raw.githubusercontent.com/secretflow/scql/main/examples/p2p-tutorial/mysql/initdb/alice_init.sql
  # For Bob, please use command: wget raw.githubusercontent.com/secretflow/scql/main/examples/p2p-tutorial/mysql/initdb/broker_init_bob.sql
  wget raw.githubusercontent.com/secretflow/scql/main/examples/p2p-tutorial/mysql/initdb/broker_init_alice.sql


1.3 Set SCQLBroker Config
---------------------------

Create a file called ``config.yml`` in your workspace and paste the following code in:

.. code-block:: yaml

  intra_server:
    host: 0.0.0.0
    port: 8080
  inter_server:
    port: 8081
  log_level: debug
  party_code: alice
  session_expire_time: 24h
  session_expire_check_time: 1m
  party_info_file: "/home/admin/configs/party_info.json"
  private_key_path: "/home/admin/configs/ed25519key.pem"
  intra_host: broker:8080
  engine:
    timeout: 120s
    protocol: http
    content_type: application/json
    uris:
      - for_peer: __ENGINE_URL__
        for_self: engine:8003
  storage:
    type: mysql
    conn_str: "root:__MYSQL_ROOT_PASSWORD__@tcp(mysql:3306)/brokeralice?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_idle_time: 2m
    conn_max_lifetime: 5m

.. _replace_p2p_password:
.. note::

  For Bob, the **party_code** should be ``bob``, and the ``brokeralice`` in **conn_str** should be replaced by ``brokerbob``.

  The ``__ENGINE_URL__`` should be replaced by **machine host/ip + engine published port**, like: 30.30.30.30:8003.

  The ``__MYSQL_ROOT_PASSWORD__`` should be replaced with the password set by the corresponding party, and please replace this placeholder in the same way for subsequent files.

See :ref:`SCQLBroker configuration options <config_broker_server_options>` for more.


1.4 Set SCQLEngine Config
-------------------------

Create a file called ``gflags.conf`` in your workspace and paste the following code in:

.. code-block:: bash

  --listen_port=8003
  --datasource_router=embed
  --enable_driver_authorization=false
  --server_enable_ssl=false
  --driver_enable_ssl_as_client=false
  --peer_engine_enable_ssl_as_client=false
  --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"db=alice;user=root;password=__MYSQL_ROOT_PASSWORD__;host=mysql;auto-reconnect=true"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}
  # party authentication flags
  --enable_self_auth=false
  --enable_peer_auth=false

.. note::

  The ``connection_str`` specified in ``embed_router_conf`` is utilized to connect database named **alice** as set in `1.2 Prepare Meta Data and Source Data`_, For Bob it should be set to connect database named **bob**.

  Please remember to replace ``__MYSQL_ROOT_PASSWORD__`` with the same password :ref:`as before <replace_p2p_password>`

See :ref:`Engine configuration options <engine_config_options>` for more config information


1.5 Create docker-compose file
------------------------------

Create a file called ``docker-compose.yaml`` in your workspace and paste the following code in:

.. code-block:: yaml

  version: '3.8'
  services:
    broker:
      image: secretflow/scql:latest
      command:
        - /home/admin/bin/broker
        - -config=/home/admin/configs/config.yml
      restart: always
      ports:
        - __INTRA_PORT__:8080
        - __INTER_PORT__:8081
      volumes:
        - ./config.yml:/home/admin/configs/config.yml
        - ./party_info.json:/home/admin/configs/party_info.json
        - ./ed25519key.pem:/home/admin/configs/ed25519key.pem
    engine:
      cap_add:
        - NET_ADMIN
      command:
        - /home/admin/bin/scqlengine
        - --flagfile=/home/admin/engine/conf/gflags.conf
      image: secretflow/scql:latest
      ports:
        - __ENGINE_PORT__:8003
      volumes:
        - ./gflags.conf:/home/admin/engine/conf/gflags.conf
    mysql:
      image: mysql:latest
      environment:
        - MYSQL_ROOT_PASSWORD=__MYSQL_ROOT_PASSWORD__
        - TZ=Asia/Shanghai
      healthcheck:
        retries: 10
        test:
          - CMD
          - mysqladmin
          - ping
          - -h
          - mysql
        timeout: 20s
      expose:
        - "3306"
      restart: always
      volumes:
        - ./alice_init.sql:/docker-entrypoint-initdb.d/alice_init.sql
        - ./broker_init_alice.sql:/docker-entrypoint-initdb.d/broker_init_alice.sql


.. note::

  ``__INTRA_PORT__``, ``__INTER_PORT__`` and ``__ENGINE_PORT__``  are published ports on the host machine, you should replace them with accessible port numbers, in particular, the ``__ENGINE_PORT__`` should be the same port in :ref:`__ENGINE_URL__ <replace_p2p_password>`. In this case, we have designated them as ``8080``, ``8081`` and ``8003``

  Please remember to replace ``__MYSQL_ROOT_PASSWORD__`` with the same password :ref:`as before <replace_p2p_password>`

  Container *mysql* are initialized by ``alice_init.sql`` and ``broker_init_alice.sql`` as set in `1.2 Prepare Meta Data and Source Data`_ , please change to ``bob_init.sql`` and ``broker_init_bob.sql`` for Bob
  
  If you use your own database service, container *mysql* can be deleted


1.6 Prepare Party Auth Files
----------------------------

Parties are identified by private-public key pairs, so we need to generate these files.

Create a file called ``party_info.json`` in your workspace and paste the following code in:

.. code-block:: json

  {
    "participants": [
      {
        "party_code": "alice",
        "endpoint": "__ALICE_BROKER_URL__",
        "pubkey": "__ALICE_PUBLIC_KEY__"
      },
      {
        "party_code": "bob",
        "endpoint": "__BOB_BROKER_URL__",
        "pubkey": "__BOB_PUBLIC_KEY__"
      }
    ]
  }

.. note::
  ``__ALICE_BROKER_URL__`` should be replaced by ``Alice machine host/ip + Alice __INTER_PORT__``, like: http://30.30.30.30:8081, do the same for ``__BOB_BROKER_URL__``.


Create other files:

.. code-block:: bash

  # generate private key
  openssl genpkey -algorithm ed25519 -out ed25519key.pem
  # get public key corresponding to the private key, the output can be used to replace the __ALICE_PUBLIC_KEY__ in party_info.json
  # for engine Bob,  the output can be used to replace the __BOB_PUBLIC_KEY__ in party_info.json
  openssl pkey -in ed25519key.pem  -pubout -outform DER | base64


Then you need to replace ``__XXX_PUBLIC_KEY__`` in party_info.json with corresponding public keys.


1.6 Start Services
------------------

The file your workspace should be as follows:

.. code-block:: bash

  └── scql-p2p
    ├── alice_init.sql
    ├── broker_init_alice.sql
    ├── config.yml
    ├── docker-compose.yaml
    ├── ed25519key.pem
    ├── gflags.conf
    └── party_info.json

Then you can start services by running docker compose up

.. code-block:: bash

  # If you install docker with Compose V1, please use `docker-compose` instead of `docker compose`
  $ docker compose -f docker-compose.yaml up -d

  Network scql-p2p_default     Created
  Container scql-p2p-engine-1  Started
  Container scql-p2p-broker-1  Started
  Container scql-p2p-mysql-1   Started

You can use docker logs to check whether services works well

.. code-block:: bash

  $ docker logs -f scql-p2p-engine-1

  [info] [main.cc:main:297] Started engine rpc server success, listen on: 0.0.0.0:8003

  $ docker logs -f scql-p2p-broker-1

  INFO main.go:157 Starting to serve request on :8081 with http...
  INFO main.go:157 Starting to serve request on :8080 with http...


Step 2: Deployment for Bob
============================

It is basically the same as `Step 1: Deployment for Alice`_, but some characters and files related to ``alice`` need to be replaced with ``bob``.


Step 3: SCQL Test
=================

Here we use brokerctl to submit a query to SCQLBroker for testing, you can also submit queries directly to SCQLBroker by sending a POST request.


3.1 Build brokerctl
-------------------

.. code-block:: bash

    # Grab a copy of scql:
    git clone git@github.com:secretflow/scql.git
    cd scql

    # build scdbclient from source
    # requirements:
    #   go version >= 1.22
    go build -o brokerctl cmd/brokerctl/main.go

    # try brokerctl
    ./brokerctl --help

3.2 Submit Query
----------------

You can start to use brokerctl to submit requests to SCQLBroker and fetch the results back. it's similar to what you can do in :doc:`/intro/p2p-tutorial`.


.. code-block:: bash

    # create project demo in alice
    ./brokerctl create project --project-id "demo" --host __ALICE_INTRA_URL__
    # check project's information
    ./brokerctl get project --host __ALICE_INTRA_URL__
    [fetch]
    +-----------+---------+---------+----------------------------------+
    | ProjectId | Creator | Members |               Conf               |
    +-----------+---------+---------+----------------------------------+
    | demo      | alice   | [alice] | {                                |
    |           |         |         |   "protocol":  "SEMI2K",         |
    |           |         |         |   "field":  "FM64"               |
    |           |         |         | }                                |
    +-----------+---------+---------+----------------------------------+
    ...

.. note::

  You need to replace ``__ALICE_INTRA_URL__`` or ``__BOB_INTRA_URL__`` with the actual IntraServer address, like:  http://30.30.30.30:8080.