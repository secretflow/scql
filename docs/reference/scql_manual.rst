SCQL Manual
===========


.. _scql_data_types:

SCQL Data Types
---------------

SCQL supports frequently-used data types, as illustrated in the following table.

+---------------+------------------------------+-------------------------------------------------------+
|   Data Type   |            Alias             |                      Description                      |
+===============+==============================+=======================================================+
| ``integer``   | ``int``, ``long``, ``int64`` |                                                       |
+---------------+------------------------------+-------------------------------------------------------+
| ``float``     | ``double``                   | both storage and computation are done in type float32 |
+---------------+------------------------------+-------------------------------------------------------+
| ``string``    | ``str``                      |                                                       |
+---------------+------------------------------+-------------------------------------------------------+
| ``datetime``  |                              | not supported yet, coming soon                        |
+---------------+------------------------------+-------------------------------------------------------+
| ``timestamp`` |                              | not supported yet, coming soon                        |
+---------------+------------------------------+-------------------------------------------------------+

 

SCQL Statements
---------------

SCQL is compatible with most MySQL syntax, which makes it easy to use. For syntax differences between SCQL and MySQL, please read :doc:`mysql_compatibility`.

CREATE/DROP USER Statement
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: SQL

    CREATE USER [IF NOT EXISTS] user party_code_option IDENTIFIED BY 'auth_string'

    party_code_option:
        PARTY_CODE party_code
    
    DROP USER [IF EXISTS] user


Examples:

.. code-block:: SQL
    
    -- create an user named alice, 
    -- with password `alice123`, 
    -- belongs to party `party_alice`
    CREATE USER alice PARTY_CODE "party_alice" IDENTIFIED BY "alice123";

    -- drop user alice
    DROP USER alice;


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

    CREATE TABLE [IF NOT EXISTS] tbl_name TID [=] value

    DROP TABLE [IF EXISTS] tbl_name

.. note::
    Create table here means mapping 'tbl_name' to the existing physic table, which is specified by 'TID' and located on the relevant parties..
    In SCQL, user runs queries on virtual table, which helps simplify privilege control and usage.

Examples:

.. code-block:: SQL

    -- create a table `ta` in database `db_test`,
    -- the new table references the table with TID = `table-id` in GRM,
    -- SCQL would fetch the table schema from GRM with given `TID`.
    CREATE TABLE db_test.ta TID="table-id";

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


**Limitation:**

* ``JOIN`` only supports 

    * ``INNER JOIN``

Functions and Operators
-----------------------

.. todo:: this part is not ready, please check later