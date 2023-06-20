=======================
Global Resource Manager
=======================

.. _grm:

Overview
========

Global Resource Manager (GRM) is used to manage global data in a secure collaborative system. The global data managed by GRM includes information about parties, table schemas, SCQLEngine endpoints, and etc.

Why GRM?
--------

The SCQL system is only responsible for secure collaborative analytics, it does not own or manage data.

SCQL needs to know the following information when executing a query.

1. The schema information of the tables involved in the query includes detailed information such as the table columns and the data source type.
2. Data owner party node metadata, such as SCQLEngine endpoints.
3. Party identity management.


API
===

The GRM service is an HTTP service and can be implemented in any language like C++/Java/Go/Python/..., where request and response are in JSON format(corresponding to their `Protocol Buffer definition <https://github.com/secretflow/scql/blob/main/api/grm.proto>`_).

The GRM service will be called by `client <https://github.com/secretflow/scql/blob/main/pkg/grm/stdgrm/standard_grm.go>`_.


/VerifyTableOwnership
---------------------

When creating table, SCQL needs to verify whether the user holding the token owns the table specified by TID

Request
^^^^^^^

+-------+--------+----------+------------------------------------------------------------------------------------------------------------------+
| Field |  Type  | Required |                                                   Description                                                    |
+=======+========+==========+==================================================================================================================+
| tid   | string | Y        | Table identifier provided by user when create table, read :ref:`Create table<create_table>` for more information |
+-------+--------+----------+------------------------------------------------------------------------------------------------------------------+
| token | string | Y        | The token used to authenticate the user                                                                          |
+-------+--------+----------+------------------------------------------------------------------------------------------------------------------+

Response
^^^^^^^^

+----------+---------+----------+--------------------------------------+
|  Field   |  Type   | Required |             Description              |
+==========+=========+==========+======================================+
| status   | Status_ | Y        | Status of response                   |
+----------+---------+----------+--------------------------------------+
| is_owner | bool    | Y        | True: user is the owner of the table |
+----------+---------+----------+--------------------------------------+

Status
""""""

+---------+-------------------+----------+----------------------------------------------+
|  Field  |       Type        | Required |                 Description                  |
+=========+===================+==========+==============================================+
| code    | int32             | Y        | The status code, 0 means success             |
+---------+-------------------+----------+----------------------------------------------+
| message | string            | N        | Message for recording the error information. |
+---------+-------------------+----------+----------------------------------------------+
| details | protobuf.Any list | N        | A list of messages for error details         |
+---------+-------------------+----------+----------------------------------------------+



Example
^^^^^^^

Request

.. code-block:: javascript

    {
        "tid": "some_tid",
        "token": "some_token"
    }

Response

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
        "is_owner": true
    }


/GetTableMeta
-------------

During creating table, after ensuring the ownership, SCQL needs to Get table schema from GRM service.

Request
^^^^^^^

+---------------+--------+----------+-----------------------------------------+
|     Field     |  Type  | Required |               Description               |
+===============+========+==========+=========================================+
| tid           | string | Y        | Unique table identifier                 |
+---------------+--------+----------+-----------------------------------------+
| request_party | string | Y        | The party code of request issuer        |
+---------------+--------+----------+-----------------------------------------+
| token         | string | Y        | The token used to authenticate the user |
+---------------+--------+----------+-----------------------------------------+

Response
^^^^^^^^

+--------+--------------+----------+-------------------------+
| Field  |     Type     | Required |       Description       |
+========+==============+==========+=========================+
| status | Status_      | Y        | The status of response  |
+--------+--------------+----------+-------------------------+
| schema | TableSchema_ | Y        | The schema of the table |
+--------+--------------+----------+-------------------------+

TableSchema
"""""""""""

+------------+------------------+----------+----------------------------------------------------+
|   Field    |       Type       | Required |                    Description                     |
+============+==================+==========+====================================================+
| db_name    | string           | Y        | The name of the database that the table belongs to |
+------------+------------------+----------+----------------------------------------------------+
| table_name | string           | Y        | The name of the table                              |
+------------+------------------+----------+----------------------------------------------------+
| columns    | ColumnDesc_ list | Y        | The column information in the table                |
+------------+------------------+----------+----------------------------------------------------+

ColumnDesc
**********

+-------------+--------+----------+-------------------------------+
|    Field    |  Type  | Required |          Description          |
+=============+========+==========+===============================+
| name        | string | Y        | The column name               |
+-------------+--------+----------+-------------------------------+
| type        | string | Y        | The type of column value      |
+-------------+--------+----------+-------------------------------+
| description | string | N        | The description of the column |
+-------------+--------+----------+-------------------------------+

Example
^^^^^^^

request

.. code-block:: javascript

    {
        "tid": "1"
        "request_party": "some_party",
        "token": "some_token",
    }

response

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
        "schema" {
            "db_name": "some_da_name",
            "table_name": "some_table_name"
            "columns": [
                {
                    "name": "col1",
                    "type": "long"
                },
                {
                    "name": "col2",
                    "type": "string"
                }
            ]
        }
    }


/GetEngines
-----------

During executing the DQL submitted by the user holding the token, SCQL needs to get the SCQLEngine information of the relevant parties.

Request
^^^^^^^

+-------------+-------------+----------+---------------------------------------------------+
|    Field    |    Type     | Required |                    Description                    |
+=============+=============+==========+===================================================+
| party_codes | string list | Y        | Parties whose SCQLEngine info need to be obtained |
+-------------+-------------+----------+---------------------------------------------------+
| token       | string      | Y        | Token used to authenticate the user               |
+-------------+-------------+----------+---------------------------------------------------+

Response
^^^^^^^^

+--------------+------------------+----------+---------------------------------------------------------------------+
|    Field     |       Type       | Required |                             Description                             |
+==============+==================+==========+=====================================================================+
| status       | Status_          | Y        | The status of response                                              |
+--------------+------------------+----------+---------------------------------------------------------------------+
| engine_infos | EngineInfo_ list | Y        | engine_infos[i] is SCQLEngine info for party request.party_codes[i] |
+--------------+------------------+----------+---------------------------------------------------------------------+

EngineInfo
""""""""""

+------------+-------------+----------+-----------------------------------------------------+
|   Field    |    Type     | Required |                     Description                     |
+============+=============+==========+=====================================================+
| endpoints  | string list | Y        | The url of SCQLEngine                               |
+------------+-------------+----------+-----------------------------------------------------+
| credential | string list | Y        | Credential used for SCQLEngine to authenticate SCDB |
+------------+-------------+----------+-----------------------------------------------------+

Example
^^^^^^^

Request

.. code-block:: javascript

    {
        "party_codes": ["party1", "party2"],
        "token": "some_token"
    }

Response

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
       "engine_infos": [
            {
                "endpoints": ["party1_url"],
                "credential": ["party1_credential"]
            },
            {
                "endpoints": ["party2_url"],
                "credential": ["party2_credential"]
            }
       ]
    }