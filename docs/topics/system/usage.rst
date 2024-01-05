How to Use SCQL
===============

The flowchart shows how to use SCQL system, which contains two stages: prepare and query. In the prepare stage, the system is set up, and the necessary tables are created and initialized. In the query stage, queries are submitted, processed, and results are returned.

.. note::
   All requests from user to SCDB can be submitted through :ref:`synchronous api<sync_api>` or :ref:`asynchronous api<async_api>`.

Workflow
---------
.. image:: /imgs/how_to_use_scql.png
    :alt: how_to_use_scql

Participants
------------

* Alice: the user in party alice
* Bob: the user in party bob
* SCDB: the SCDB server

.. note::
   Before the start of the project, Alice and Bob need to obtain the root user's username and password from the admin.

Prepare stage
-------------

1.0 create database
    Alice or Bob creates a new database using the root user account through :ref:`CREATE DATABASE Statement <create_database_stm>`, which can be considered a new project, SCQL supports running multiple projects simultaneously.

2.0 create and grant user
    Alice creates user account "Alice" in SCDB using the root user account by :ref:`CREATE USER Statement <create_user_stm>`, and grants access to the relevant database by :ref:`GRANT Statement <scql_grant_revoke>`

3.0 create and grant user
    Bob creates user account "Bob" in SCDB using the root user account by :ref:`CREATE USER Statement <create_user_stm>`, and grants access to the relevant database by :ref:`GRANT Statement <scql_grant_revoke>`

4.0 create table
    Alice creates table in SCDB using the account "Alice" by :ref:`CREATE TABLE Statement <create_table>`.

5.0 create table
    Bob creates table in SCDB using the account "Bob" by :ref:`CREATE TABLE Statement <create_table>`.

6.0 grant CCL
    Alice grants CCL about the table created by Alice for Alice and Bob in SCDB using the account "Alice", see :doc:`/topics/ccl/intro` for more information.

7.0 grant CCL
    Bob grants CCL about the table created by Bob for Alice and Bob in SCDB using the account "Bob", see :doc:`/topics/ccl/intro` for more information.

Execute stage
-------------

8.0 submit a DQL
    Alice submits a DQL to SCDB through :ref:`public/submit_query <submit_query>` or :ref:`public/submit_and_get <submit_and_get>` using the account "Alice".

9.0 process query
    SCDB will check ccl and execute query with engines.

10.0 return result
    SCDB return the query result to Alice.
