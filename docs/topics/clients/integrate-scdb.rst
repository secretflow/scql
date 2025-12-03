How to Integrate SCDB
=====================

Overview
--------

As shown in FIG, SCQL System needs to work with **Client**:

* Client: user interface designed for query submission and result retrieval.

.. image:: /imgs/scql_system.png

Therefore the platform should support Client to integrate SCQL System.

Specifically, the Client interact with SCDB through Query API.

Query API
----------
For SQL query, SCDB support services:

* Submit: async API, just submit SQL query and return, server listen on ``${SCDBHost}/public/submit_query``
* Fetch: async API, try to get the result of a SQL query, server listen on ``${SCDBHost}/public/fetch_result``
* SubmitAndGet: sync API, submit query and wait to get the query result, server listen on ``${SCDBHost}/public/submit_and_get``

Please refer to :doc:`/reference/scdb-api` for details.

.. note::
  *  Client can choose to support either async or sync API according to business scenarios:

      If the SQL query task might take too much time, it is recommended to use the async API, otherwise use the sync API for simplicity.


In a word, the custom Client should construct HTTP request for user's SQL, post to SCDB and parse the response from SCDB.

Dry run mode
------------

SCDB provides the "dry_run" flag solely for syntax and CCL validation purposes,
without actually triggering execution. This mode possesses the following characteristics:

* This only applies to the synchronous interface "SubmitAndGet". If the asynchronous "Submit" carries "dry_run=true", it will return "BAD_REQUEST".
* Only DQL statements are supported; DDL/DCL will be executed normally, even if `dry_run=true`.
* The return value is consistent with that of a regular synchronous query, but `status.message` will contain "dry run success" when the verification passes

A typical request is as follows:

.. code-block:: json

   {
     "user": {
       "user": {
         "account_system_type": "NATIVE_USER",
         "native_user": {
           "name": "alice",
           "password": "alice123"
         }
       }
     },
     "query": "select column1_1 from test.table_1",
     "dry_run": true
   }

The client can first send a dry-run request for the critical DQL, and after confirming its success,
remove the 'dry_run' flag to execute the formal query, in order to avoid long execution time or potential CCL violations.

Explain statement
-----------------

SCDB also supports MySQL-style ``EXPLAIN`` statements to inspect the execution graph of a DQL query. The behaviour is summarized below:

* Only the synchronous ``SubmitAndGet`` API is supported; calling ``Submit`` with EXPLAIN will return ``BAD_REQUEST``.
* The target statement must be DQL (``SELECT``/``UNION``). ``EXPLAIN ANALYZE`` is not supported yet.
* Use ``FORMAT = 'dot'`` for clarity. When ``FORMAT`` is omitted, SCDB still returns a Graphviz DOT string. Other formats are rejected.
* The response contains a single column named ``execution_graph_dot``. Save the string to a ``.dot`` file for visualization via Graphviz.

Example:

.. code-block:: SQL

   -- With explicit FORMAT:
   EXPLAIN FORMAT = 'dot'
   SELECT column1_1 FROM test.table_1;

   -- Without FORMAT (defaults to dot):
   EXPLAIN SELECT column1_1 FROM test.table_1;


SQL Syntax
----------

SCDB provides a SQL-like user interface, which is compatible with most MySQL syntax.

.. note::
  For DQL syntax, please check :doc:`/reference/lang/manual`


.. _create_user_stm:

CREATE/DROP/ALTER USER Statement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    CREATE USER [IF NOT EXISTS] user PARTY_CODE party_code IDENTIFIED BY 'auth_string' [with_opt]

    with_opt:
      WITH auth_opt [endpoint_opt]

    auth_opt:
      (TOKEN token_str) | (message signature pub_key)

    endpoint_opt:
      ENDPOINT endpoint (, endpoint)+


    DROP USER [IF EXISTS] user

    ALTER USER [IF EXISTS] user IDENTIFIED BY 'new_auth_string'

    ALTER USER [IF EXISTS] user WITH ENDPOINT endpoint (, endpoint)+


Examples:

.. code-block:: SQL

    -- create an user named alice,
    -- with password `alice123`,
    -- belongs to party `party_alice` with public key `MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA=`.
    -- the create user statement includes a timestamp message (in RFC 3339 formats), and signed by the party's private key.
    CREATE USER `alice` PARTY_CODE 'party_alice' IDENTIFIED BY 'alice123' WITH '2023-08-23T15:46:16.096262218+08:00' 'DK/V80pV8bsWkXwgyRBrca7P2V2O03nC1pEldnJF+1dUnnL2NoRGKhAjSMv0ubuflT4yUmoIPRzwOi/bOsf2BQ==' 'MCowBQYDK2VwAyEAqhfJVWZX32aVh00fUqfrbrGkwboi8ZpTpybLQ4rbxoA=';

    -- drop user alice
    DROP USER alice;

    -- change user alice password
    ALTER USER alice IDENTIFIED BY "new_password";
    -- change user engine endpoint
    ALTER USER alice WITH ENDPOINT 'engine-alice-host:port'

.. _create_database_stm:

CREATE/DROP DATABASE Statement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    CREATE DATABASE [IF NOT EXISTS] database;

    DROP DATABASE [IF EXISTS] database;


Examples:

.. code-block:: SQL

    -- create db `db_test`
    CREATE DATABASE db_test;

    -- drop db `db_test`
    DROP DATABASE db_test;


.. _create_table:

CREATE/DROP TABLE Statement
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    CREATE TABLE [IF NOT EXISTS] tbl_name (
        column_name data_type,
        column_name data_type,
        ...
    ) REF_TABLE=TableName DB_TYPE='db_type'

    DROP TABLE [IF EXISTS] tbl_name

.. note::
    Create table here means mapping 'tbl_name' to the existing physic table, which is specified by ``REF_TABLE`` option and located on the query issuer party.
    In SCQL, user runs queries on virtual table, which helps simplify privilege control and usage.

Examples:

.. code-block:: SQL

    -- create a table `ta` in database `db_test`
    -- the new table should have the same table schema with the table `db1.tbl_1` specified by
    -- REF_TABLE option,  and the new table is a MySQL table.
    CREATE TABLE db_test.ta (
        id string,
        col1 int64,
        col2 float,
        col3 double
    ) REF_TABLE=db1.tbl_1 DB_TYPE='mysql';

    DROP TABLE db_test.ta;

.. _scql_grant_revoke:

GRANT/REVOKE Statement
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    GRANT
    extend_priv_type [(column_list)]
      [, extend_priv_type[(column_list)]] ...
    ON priv_level
    TO user

    REVOKE [IF EXISTS]
    extend_priv_type [(column_list)]
      [, extend_priv_type[(column_list)]] ...
    ON priv_level
    FROM user

    extend_priv_type:
        priv_type | SELECT [CCL level]



Examples:

.. code-block:: SQL

    -- GRANT privileges
    GRANT CREATE VIEW on db.* to alice;
    GRANT SHOW, DESCRIBE ON db.* TO 'someuser'@'somehost';
    -- GRANT CCL
    GRANT SELECT PLAINTEXT_AFTER_JOIN(column) on db.table to user;
    -- REVOKE CCL
    REVOKE SELECT PLAINTEXT_AFTER_AGGREGATE(column) ON db.table FROM user;



SHOW/DESCRIBE Statement
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    SHOW DATABASES;

    SHOW TABLES;

    SHOW GRANTS on <db_name> FOR <user>;

    DESCRIBE <db_name>.<table_name>;
