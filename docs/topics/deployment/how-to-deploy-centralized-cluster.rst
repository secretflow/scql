===================================
Deployment with Trusted Third Party
===================================

This document describes how to deploy a SCQL system in centralized mode with docker, and use scdbclient to query, it's basically same with :doc:`/intro/tutorial`, but deployed in multi-machine.

Before start this doc, we assume that the reader has some experience using the docker-compose utility. If you are new to Docker Compose, please consider reviewing the `official Docker Compose overview <https://docs.docker.com/compose/>`_, or checking out the `Getting Started guide <https://docs.docker.com/compose/gettingstarted/>`_.

Deployment Diagram
==================

The deployment diagram of the SCQL system that we plan to deploy is shown as the following figure, it involves a total of three party, including a trusted third-party and two data owner parties named ``Alice`` and ``Bob``. We use three machines to simulate different parties.

.. image:: /imgs/deployment.png

.. note::
    1. The SCDB is served through the HTTP protocol. It is recommended to use HTTPS instead in production environments. Please check :ref:`TLS Configuration <scdb-tls>` for details.

Step 1: Deploy SCQLEngine
==========================

Here we present how to deploy engine in party Alice, it's same with party Bob.

1.1 Create a Workspace
-----------------------

.. code-block:: bash

    mkdir engine
    cd engine

1.2 Set Dataset
---------------

The database in Engine is use to storage the origin data. In this document we will use MySQL as the database service, and simulate a dataset. However, if you prefer, you can use your preferred database service.

To simulate a dataset, you need create a sql init file named alice_init.sql in your workspace, which corresponds to `alice_init.sql <https://github.com/secretflow/scql/tree/main/examples/scdb-tutorial/mysql/initdb/alice_init.sql>`_. For Bob, please use `bob_init.sql <https://github.com/secretflow/scql/tree/main/examples/scdb-tutorial/mysql/initdb/bob_init.sql>`_ instead.

This file can also be obtained via the command-line with either curl, wget or another similar tool.

.. code-block:: bash

  wget raw.githubusercontent.com/secretflow/scql/main/examples/scdb-tutorial/mysql/initdb/alice_init.sql


1.3 Set Engine Config
---------------------

Create a file called ``gflags.conf`` in your workspace and paste the following code in:

.. code-block:: bash

    --listen_port=8080
    --datasource_router=embed
    --enable_driver_authorization=false
    --server_enable_ssl=false
    --driver_enable_ssl_as_client=false
    --peer_engine_enable_ssl_as_client=false
    --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"db=alice;user=root;password=__MYSQL_ROOT_PASSWORD__;host=mysql;auto-reconnect=true"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}
    # party authentication flags
    --enable_self_auth=true
    --enable_peer_auth=true
    --private_key_pem_path=/home/admin/engine/conf/ed25519key.pem
    --authorized_profile_path=/home/admin/engine/conf/authorized_profile.json

See :ref:`Engine configuration options <engine_config_options>` for more config information

.. _replace_root_password:
.. note::

  The ``connection_str`` specified in ``embed_router_conf`` is utilized to connect database named alice as set in `1.2 Set Dataset`_, For Bob it should be set to connect database named bob.

  The ``__MYSQL_ROOT_PASSWORD__`` should be replaced with the password set by the corresponding party, and please replace this placeholder in the same way for subsequent files.


1.4 Create docker-compose file
------------------------------

Create a file called ``docker-compose.yaml`` in your workspace and paste the following code in:

.. code-block:: yaml

  version: "3.8"
  services:
    engine:
      cap_add:
        - NET_ADMIN
      command:
        - /home/admin/bin/scqlengine
        - --flagfile=/home/admin/engine/conf/gflags.conf
      restart: always
      image: secretflow/scql:latest
      ports:
        - __ALICE_PORT__:8080
      volumes:
        - ./gflags.conf:/home/admin/engine/conf/gflags.conf
        - ./ed25519key.pem:/home/admin/engine/conf/ed25519key.pem
        - ./authorized_profile.json:/home/admin/engine/conf/authorized_profile.json
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

.. note::

  ``__ALICE_PORT__``  is the published port on the host machine which is used for engine service to listen on, you need to replace it with an accessible port number. In this case, we have designated it as ``8080``

  Container *mysql* is init by ``alice_init.sql`` as set in `1.2 Set Dataset`_ , it should be ``bob_init.sql`` for Bob. If you use your own database service, container *mysql* can be deleted

  Please remember to replace ``__MYSQL_ROOT_PASSWORD__`` with the same password :ref:`as before <replace_root_password>`


1.5 Prepare Party Auth Files
----------------------------

The party authentication flags are enabled in ``gflags.conf``, so we need to generate private key and authorized_profile.

.. code-block:: bash

  # generate private key
  openssl genpkey -algorithm ed25519 -out ed25519key.pem
  # get public key corresponding to the private key, the output can be used to replace the __ALICE_PUBLIC_KEY__ in engine Bob's authorized_profile.json
  # for engine Bob,  the output can be used to replace the __BOB_PUBLIC_KEY__ in engine Alice's authorized_profile.json
  openssl pkey -in ed25519key.pem  -pubout -outform DER | base64
  # download authorized profile
  # for engine Bob, use command: wget https://raw.githubusercontent.com/secretflow/scql/main/examples/scdb-tutorial/engine/bob/conf/authorized_profile.json
  wget https://raw.githubusercontent.com/secretflow/scql/main/examples/scdb-tutorial/engine/alice/conf/authorized_profile.json


Then you need to replace ``__BOB_PUBLIC_KEY__`` in authorized_profile.json with Bob's public key. For engine Bob, please replace the ``__ALICE_PUBLIC_KEY__``


1.6 Start Engine Service
------------------------

The file your workspace should be as follows:

.. code-block:: bash

  └── engine
    ├── alice_init.sql
    ├── authorized_profile.json
    ├── docker-compose.yaml
    ├── ed25519key.pem
    └── gflags.conf

Then you can start engine service by running docker compose up

.. code-block:: bash

  # If you install docker with Compose V1, please use `docker-compose` instead of `docker compose`
  $ docker compose -f docker-compose.yaml up -d

  Creating network "engine_default" with the default driver
  Creating engine_engine_1 ... done
  Creating engine_mysql_1  ... done

You can use docker logs to check whether engine works well

.. code-block:: bash

  $ docker logs -f engine_engine_1

  [info] [main.cc:main:297] Started engine rpc server success, listen on: 0.0.0.0:8080

SCQLEngine is listening on ``8080``.


Step 2: Deploy SCDB
===================

This chapter will demonstrate how to deploy SCDB in a Third-Party

2.1 Create a Workspace
----------------------

.. code-block:: bash

  mkdir scdb
  cd scdb

2.2 Set SCDB Config
--------------------

Create a file called ``config.yml`` in your workspace and paste the following code in:

.. code-block:: yaml

  scdb_host: scdb:8080
  port: 8080
  protocol: http
  query_result_callback_timeout: 3m
  session_expire_time: 24h
  session_expire_check_time: 1m
  log_level: debug
  storage:
    type: mysql
    conn_str: "root:__MYSQL_ROOT_PASSWORD__@tcp(mysql:3306)/scdb?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
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

See :ref:`SCDB configuration options <scdb_config_options>` for more config information

.. note::

  ``conn_str`` is utilized to connect database named scdb which will be deployed in next step, if you prefer, you can also use your own database service.

  Please remember to replace ``__MYSQL_ROOT_PASSWORD__`` with the same password  :ref:`as before <replace_root_password>`


2.4 Create docker-compose file
------------------------------

Create a file called ``docker-compose.yaml`` in your workspace and paste the following code in:

.. code-block:: yaml

  version: "3.8"
  services:
    scdb:
      image: secretflow/scql:latest
      environment:
        - SCQL_ROOT_PASSWORD=root
      command:
        - /home/admin/bin/scdbserver
        - -config=/home/admin/configs/config.yml
      restart: always
      ports:
        - __SCDB_PORT__:8080
      volumes:
        - ./config.yml:/home/admin/configs/config.yml
    mysql:
      image: mysql:latest
      environment:
        - MYSQL_ROOT_PASSWORD=__MYSQL_ROOT_PASSWORD__
        - MYSQL_DATABASE=scdb
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

.. note::

  ``__SCDB_PORT__`` is the published port on the host machine which is used for scdb service to listen on, you need to replace it with an accessible port number. Here, it's set as 8080

  Please remember to replace ``__MYSQL_ROOT_PASSWORD__`` with the same password  :ref:`as before <replace_root_password>`

2.5 Start SCDB Service
----------------------

The file your workspace should be as follows:

.. code-block:: bash

  └── scdb
    ├── scdb_init.sql
    ├── config.yml
    └── docker-compose.yaml

Then you can start engine service by running docker compose up

.. code-block:: bash

  # If you install docker with Compose V1, please use `docker-compose` instead of `docker compose`
  $ docker compose -f docker-compose.yaml up -d

  Creating scdb_mysql_1 ... done
  Creating scdb_scdb_1  ... done

You can use docker logs to check whether engine works well

.. code-block:: bash

  $ docker logs -f scdb_scdb_1

  INFO main.go:122 Starting to serve request with http...

SCDB is listening on ``8080``, waiting for connection


Step 3: SCQL Test
=================

Here we use scdbclient to submit a query to SCDB for testing, you can also submit queries directly to SCDB by sending a POST request. This step can be completed on any machine that has access to the SCDB ip address.

You can read `How to use SCDBClient <https://github.com/secretflow/scql/tree/main/cmd/scdbclient/README.md>`_ for more information about scdbclient.

3.1 Build scdbclient
--------------------

.. code-block:: bash

    # Grab a copy of scql:
    git clone git@github.com:secretflow/scql.git
    cd scql

    # build scdbclient from source
    # requirements:
    #   go version >= 1.22
    go build -o scdbclient cmd/scdbclient/main.go

    # try scdbclient
    ./scdbclient --help

3.2 Set Client Config
---------------------

Create a json file named as ``users.json`` as follows:

.. code-block:: json

  {
    "alice": {
      "UserName": "alice",
      "Password": "some_password"
    },
    "bob": {
      "UserName": "bob",
      "Password": "another_password"
    },
    "root": {
      "UserName": "root",
      "Password": "root"
    }
  }


.. note::

    The ``root`` user is the admin user of SCDB which is init when scdb container set up, ``alice`` and ``bob`` are the user belong to party Alice and Bob,

    The user information for ``alice`` and ``bob`` should be same with the user information you will create.


3.3 Submit Query
----------------

You can start to use scdbclient to submit queries to SCDBServer and fetch the query results back. it's same as what you can do in :doc:`/intro/tutorial`

.. code-block:: bash

    # use scdbclient to connect to scdbserver
    ./scdbclient prompt --host=__SCDB_URL__ --usersConfFileName=users.json --sync
    > switch root
    # create our first db demo
    root> create database demo
    [fetch] OK for DDL/DML
    root> show databases;
    [fetch]
    1 rows in set: (2.945772ms)
    +----------+
    | Database |
    +----------+
    | demo     |
    +----------+
    ...

.. note::
    ``__SCDB_URL__`` is the url (eg:http://127.0.0.1:8080) where scdb service is listen on, you need to replace it with scdb service url.

    Because the SCQLEngine listening URL of quickstart is different from that of distributed deployment, if you encounter an error similar to the following, please use the ``alter user`` query to specify the correct listening URL, like: 30.30.30.30:8080

      [fetch]err: Code: 300, message:Post "http://engine_alice:8080/SCQLEngineService/RunExecutionPlan": dial tcp engine_alice:8080: connect: connection refused
