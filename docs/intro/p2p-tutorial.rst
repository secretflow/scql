Quickstart Tutorial
===================

In this tutorial, you will deploy the SCQL system on a single machine, then simulate the project settings for joint data analysis between two parties, and finally initiate a joint analysis query.


Deployment Architecture Description
-----------------------------------

SCQL supports two deployment architectures: centralized and P2P.

* Centralized: The centralized deployment architecture relies on a trusted third party to deploy the coordination service SCDB. Each data participant only needs to deploy a computing engine named SCQLEngine.
* P2P: The P2P deployment architecture does not require a trusted third party. Each data participant needs to deploy an SCQLEngine and an SCQLBroker service.

This tutorial uses the P2P deployment architecture. If you would like to experience the centralized deployment architecture, you can go :doc:`here </intro/tutorial>`.


Prerequisites
-------------

Clone repo
^^^^^^^^^^

.. code-block:: bash

    git clone git@github.com:secretflow/scql.git

    # all the following operations will be executed in the scql directory
    cd scql


Build brokerctl
^^^^^^^^^^^^^^^

``brokerctl`` is a command-line tool for SCQLBroker, we would use it to configure project and submit queries to the SCQLBrokers.

.. code-block:: bash

    # build brokerctl from source
    # requirements:
    #   go version >= 1.22
    go build -o brokerctl cmd/brokerctl/main.go

    # try brokerctl
    ./brokerctl --help


Generate PrivateKey and Exchange PublicKey
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    bash examples/p2p-tutorial/setup.sh

.. note::
   The setup.sh script will generate private key for each party, and exchange public key with peers.

Start SCQL Service
------------------

You could start SCQL service via `docker-compose <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial>`_, it would deploy and start services as shown in the following figure, it contains two SCQLBrokers and SCQLEngines for party ``alice``, ``bob``.

.. image:: /imgs/p2p_deploy.png
    :alt: docker-compose deployment for quickstart example


.. note::
    To demonstrate SCQL, we conducted the following simplified operations:

    1. The SCQLBrokers and SCQLEngines use the same database server but are separated by distinct database names.
    2. The SCQLBrokers are served through the HTTP protocol. However, for production environments, it is recommended to use HTTPS instead. Please check :ref:`TLS Configuration <broker-tls>` for details.
    3. The brokerctl can send requests to both SCQLBrokers, but in production environments, the SCQLBroker should use the local port and only listen to local requests for security.


.. code-block:: bash

    # startup docker-compose
    # If you install docker with Compose V1, please use `docker-compose` instead of `docker compose`
    (cd examples/p2p-tutorial && docker compose up -d)

SCQLBroker for alice is listening on ``http://localhost:8081`` while bob is on ``http://localhost:8082`` you could send requests to them via brokerctl.

.. note::
    Please checkout `examples/p2p-tutorial/README.md <https://github.com/secretflow/scql/tree/main/examples/p2p-tutorial/README.md>`_ troubleshooting section for help if you encounter any problems.



Create project, invite party to join
------------------------------------

.. code-block:: bash

    # create project demo in alice
    # NOTE: we specify the project-id to simplify the description, generally you should make sure the id is unique or ignore this flag and use the automatically generated one
    ./brokerctl create project --project-id "demo" --host http://localhost:8081
    # check project's information
    ./brokerctl get project --host http://localhost:8081
    [fetch]
    +-----------+---------+---------+----------------------------------+
    | ProjectId | Creator | Members |               Conf               |
    +-----------+---------+---------+----------------------------------+
    | demo      | alice   | [alice] | {                                |
    |           |         |         |   "protocol":  "SEMI2K",         |
    |           |         |         |   "field":  "FM64"               |
    |           |         |         | }                                |
    +-----------+---------+---------+----------------------------------+

    # alice invite bob to join the project
    ./brokerctl invite bob --project-id "demo" --host http://localhost:8081
    # bob check invitation list
    ./brokerctl get invitation --host http://localhost:8082
    [fetch]
    +--------------+---------+---------+---------+-----------+---------+---------+----------------------------------+
    | InvitationId | Status  | Inviter | Invitee | ProjectId | Creator | Members |               Conf               |
    +--------------+---------+---------+---------+-----------+---------+---------+----------------------------------+
    |            1 | Pending | alice   | bob     | demo      | alice   | [alice] | {                                |
    |              |         |         |         |           |         |         |   "protocol":  "SEMI2K",         |
    |              |         |         |         |           |         |         |   "field":  "FM64"               |
    |              |         |         |         |           |         |         | }                                |
    +--------------+---------+---------+---------+-----------+---------+---------+----------------------------------+

    # bob decide to join the project with invitation-id 1
    ./brokerctl process invitation 1 --response "accept" --project-id "demo" --host http://localhost:8082
    # check the project, its members should contain alice and bob
    ./brokerctl get project --host http://localhost:8081
    [fetch]
    +-----------+---------+-------------+----------------------------------+
    | ProjectId | Creator |   Members   |               Conf               |
    +-----------+---------+-------------+----------------------------------+
    | demo      | alice   | [alice bob] | {                                |
    |           |         |             |   "protocol":  "SEMI2K",         |
    |           |         |             |   "field":  "FM64"               |
    |           |         |             | }                                |
    +-----------+---------+-------------+----------------------------------+


Create tables
-------------

.. code-block:: bash

    # create table for alice
    ./brokerctl create table ta --project-id "demo" --columns "ID string, credit_rank int, income int, age int" --ref-table alice.user_credit --db-type mysql --host http://localhost:8081
    # check the table ta
    ./brokerctl get table ta --host http://localhost:8081 --project-id "demo"
    [fetch]
    TableName: ta, Owner: alice, RefTable: alice.user_credit, DBType: mysql
    Columns:
    +-------------+----------+
    | ColumnName  | DataType |
    +-------------+----------+
    | age         | int      |
    | credit_rank | int      |
    | ID          | string   |
    | income      | int      |
    +-------------+----------+

    # create table for bob
    ./brokerctl create table tb --project-id "demo" --columns "ID string, order_amount double, is_active int" --ref-table bob.user_stats --db-type mysql --host http://localhost:8082
    # check the table tb
    ./brokerctl get table tb --host http://localhost:8082 --project-id "demo"
    [fetch]
    TableName: tb, Owner: bob, RefTable: bob.user_stats, DBType: mysql
    Columns:
    +--------------+----------+
    |  ColumnName  | DataType |
    +--------------+----------+
    | ID           | string   |
    | is_active    | int      |
    | order_amount | double   |
    +--------------+----------+


Grant CCL
---------

.. code-block:: bash

    # alice set CCL for table ta
    ./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name ID --host http://localhost:8081
    ./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:8081
    ./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name income --host http://localhost:8081
    ./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name age --host http://localhost:8081

    ./brokerctl grant bob PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name ta --column-name ID --host http://localhost:8081
    ./brokerctl grant bob PLAINTEXT_AFTER_GROUP_BY --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:8081
    ./brokerctl grant bob PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name ta --column-name income --host http://localhost:8081
    ./brokerctl grant bob PLAINTEXT_AFTER_COMPARE --project-id "demo" --table-name ta --column-name age --host http://localhost:8081
    # bob set ccl for table tb
    ./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name ID --host http://localhost:8082
    ./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:8082
    ./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8082

    ./brokerctl grant alice PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name tb --column-name ID --host http://localhost:8082
    ./brokerctl grant alice PLAINTEXT_AFTER_COMPARE --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8082
    ./brokerctl grant alice PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:8082
    
    # show grants for alice
    # NOTE: you can add flag tables to specify table like: --tables ta
    ./brokerctl get ccl  --project-id "demo" --parties alice --host http://localhost:8081
    [fetch]
    +-----------+-----------+--------------+---------------------------+
    | PartyCode | TableName |  ColumnName  |        Constraint         |
    +-----------+-----------+--------------+---------------------------+
    | alice     | ta        | age          | PLAINTEXT                 |
    | alice     | ta        | credit_rank  | PLAINTEXT                 |
    | alice     | ta        | ID           | PLAINTEXT                 |
    | alice     | ta        | income       | PLAINTEXT                 |
    | alice     | tb        | ID           | PLAINTEXT_AFTER_JOIN      |
    | alice     | tb        | is_active    | PLAINTEXT_AFTER_COMPARE   |
    | alice     | tb        | order_amount | PLAINTEXT_AFTER_AGGREGATE |
    +-----------+-----------+--------------+---------------------------+
    # show grants for bob
    ./brokerctl get ccl  --project-id "demo" --parties bob --host http://localhost:8081
    [fetch]
    +-----------+-----------+--------------+---------------------------+
    | PartyCode | TableName |  ColumnName  |        Constraint         |
    +-----------+-----------+--------------+---------------------------+
    | bob       | ta        | age          | PLAINTEXT_AFTER_COMPARE   |
    | bob       | ta        | credit_rank  | PLAINTEXT_AFTER_GROUP_BY  |
    | bob       | ta        | ID           | PLAINTEXT_AFTER_JOIN      |
    | bob       | ta        | income       | PLAINTEXT_AFTER_AGGREGATE |
    | bob       | tb        | ID           | PLAINTEXT                 |
    | bob       | tb        | is_active    | PLAINTEXT                 |
    | bob       | tb        | order_amount | PLAINTEXT                 |
    +-----------+-----------+--------------+---------------------------+


Do query
--------


.. code-block:: bash

    ./brokerctl run "SELECT ta.credit_rank, COUNT(*) as cnt, AVG(ta.income) as avg_income, AVG(tb.order_amount) as avg_amount FROM ta INNER JOIN tb ON ta.ID = tb.ID WHERE ta.age >= 20 AND ta.age <= 30 AND tb.is_active=1 GROUP BY ta.credit_rank;"  --project-id "demo" --host http://localhost:8081 --timeout 3
    [fetch]
    2 rows in set: (1.221304389s)
    +-------------+-----+-------------------+-------------------+
    | credit_rank | cnt |    avg_income     |    avg_amount     |
    +-------------+-----+-------------------+-------------------+
    |           5 |   6 | 18069.77597427368 | 7743.392951965332 |
    |           6 |   4 | 336016.8590965271 | 5499.404067993164 |
    +-------------+-----+-------------------+-------------------+

