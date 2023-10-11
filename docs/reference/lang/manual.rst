SCQL Language Manual
====================


.. _scql_data_types:

SCQL Data Types
---------------

SCQL supports frequently-used data types, as illustrated in the following table.

+---------------+------------------------------+--------------------------------+
|   Data Type   |            Alias             |          Description           |
+===============+==============================+================================+
| ``integer``   | ``int``, ``long``, ``int64`` |                                |
+---------------+------------------------------+--------------------------------+
| ``float``     | ``float32``                  |                                |
+---------------+------------------------------+--------------------------------+
| ``double``    | ``float64``                  |                                |
+---------------+------------------------------+--------------------------------+
| ``string``    | ``str``                      |                                |
+---------------+------------------------------+--------------------------------+
| ``datetime``  |                              | not supported yet, coming soon |
+---------------+------------------------------+--------------------------------+
| ``timestamp`` |                              | not supported yet, coming soon |
+---------------+------------------------------+--------------------------------+


.. _scql_statements:

SCQL Statements
---------------

SCQL is compatible with most MySQL syntax, which makes it easy to use. For syntax differences between SCQL and MySQL, please read :doc:`/reference/lang/mysql-compatibility`.

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

    ALTER USER [IF EXISTS] user WITH ENDPOINT endpint (, endpoint)+


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


SELECT Statement
^^^^^^^^^^^^^^^^

.. code-block:: SQL

    SELECT [DISTINCT] select_expr [, select_expr] ...
    [FROM table_reference]
    [WHERE where_condition]
    [GROUP BY column]

    select_expr:
        col_reference [AS alias]

    col_reference:
        column
    | agg_function(column)

    column:
        *
    | db_name.tbl_name.col_name field_as_name_opt
    | alias.col_name field_as_name_opt
    | expression field_as_name_opt

    field_as_name_opt:
        ""
    | field_as_name

    field_as_name:
        identifier
    | "AS" identifier

    table_reference:
        table_factor
    | join_table
    | union_table

    table_factor:
        db_name.tbl_name [[AS] alias]

    join_table:
        table_reference [INNER] JOIN table_factor [join_specification]

    union_table:
        select_expr
        | UNION [ALL] union_table

    join_specification:
        ON search_condition

    expression:
        expression "SUPPORTED_OP" expression
        | "NOT" expression
        | predicate_expr

    predicate_expr:
        column InOrNotOp '(' expression_list ')'
        | column InOrNotOp sub_select
        | column

    sub_select:
        '(' select_stmt ')'


Functions and Operators
-----------------------

.. todo:: this part is not ready, please check later
