========
SCQL API
========

SCQL supports two types of APIs: synchronous and asynchronous. The synchronous interface is suitable for executing fast queries, such as DDL, DCL, and simple DQL. 
Meanwhile, the asynchronous interface is recommended when the query may take a long time to run.

.. _async_api:

Asynchronous
============

.. _submit_query:

public/submit_query
-------------------

Submit the query (DDL/DCL/DQL) to SCQL, SCQL will return a session ID immediately which can be used to fetch result, and processes the query in the background.

Request
^^^^^^^

+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| Field                      | Type            | Required | Description                                                                                           |
+============================+=================+==========+=======================================================================================================+
| header                     | RequestHeader_  | N        | Common request header                                                                                 |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| user                       | SCDBCredential_ | Y        | User information                                                                                      |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| query                      | string          | Y        | SCQL query to be run                                                                                  |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| .. _callback_url:          |                 |          |                                                                                                       |
|                            | string          | N        | callback URL to report query result. See :ref:`Asynchronous send result<async_send_result>` for more  |
| query_result_callback_url  |                 |          |                                                                                                       |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| biz_request_id             | string          | N        | Biz request id which often be unique per biz action                                                   |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+
| db_name                    | string          | N        | Current database name                                                                                 |
+----------------------------+-----------------+----------+-------------------------------------------------------------------------------------------------------+

Response 
^^^^^^^^

+-----------------+---------+-----------------------------------------+
|      Field      |  Type   |               Description               |
+=================+=========+=========================================+
| status          | Status_ | See `Status code`_ for more information |
+-----------------+---------+-----------------------------------------+
| .. _session_id: |         |                                         |
|                 | string  | Unique ID of a session                  |
| scdb_session_id |         |                                         |
+-----------------+---------+-----------------------------------------+


Example
^^^^^^^^

If you want execute `show databases`, the request and response message should be structured as follows:

* request

.. code-block:: javascript

    {
        "user": {
            "user": {
                "account_system_type": "NATIVE_USER",
                "native_user": { "name": "someuser", "password": "somepassword" }
            }
        },
        "query": "show databases;",
        "biz_request_id": "1234"
    }

* response

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
        "scdb_session_id": "some_session_id"
    }

.. _fetch_result:

public/fetch_result
---------------------

Fetch result of the query submitted via the "submit_query" method before, if the query is still running, It will return `NOT_READY` status code.

Request
^^^^^^^

+-----------------+-----------------+----------+-------------------------------------------------------------------+
| Field           | Type            | Required | Description                                                       |
+=================+=================+==========+===================================================================+
| header          | RequestHeader_  | N        | Common request header                                             |
+-----------------+-----------------+----------+-------------------------------------------------------------------+
| user            | SCDBCredential_ | Y        | User information                                                  |
+-----------------+-----------------+----------+-------------------------------------------------------------------+
| scdb_session_id | string          | Y        | Given by scdb when submit the query before, same with session_id_ |
+-----------------+-----------------+----------+-------------------------------------------------------------------+

Response
^^^^^^^^

+-------------------+--------------------+-----------------------------------------+
| Field             | Type               | Description                             |
+===================+====================+=========================================+
| status            | Status_            | See `Status code`_ for more information |
+-------------------+--------------------+-----------------------------------------+
| out_columns       | Tensor_ list       | Query result                            |
+-------------------+--------------------+-----------------------------------------+
| scdb_session_id   | string             | Same with session_id_                   |
+-------------------+--------------------+-----------------------------------------+
| affected_rows     | int64              | The num of rows affected                |
+-------------------+--------------------+-----------------------------------------+

Example
^^^^^^^^

If you want to get the result of the query `show databases`, you can send a request like this.

.. code-block:: javascript

    {
        "user": {
            "user": {
                "account_system_type": "NATIVE_USER",
                "native_user": { "name": "some_user", "password": "some_password" }
            }
        },
        "scdb_session_id": "some_session_id"
    }


If succeed, a response will be received with status code 0 like this:

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
        "out_columns": [
            {
                "name": "Database",
                "shape": {
                    "dim": [
                        {
                            "dim_value": "1"
                        },
                        {
                            "dim_value": "1"
                        }
                    ]
                },
                "elem_type": "STRING",
                "option": "VALUE",
                "annotation": null,
                "int32_data":[],
                "int64_data":[],
                "float_data":[],
                "double_data":[],
                "bool_data":[],
                "string_data": ["scdb"]
            }
        ],
        "scdb_session_id": "some_session_id",
        "affected_rows": "0"
    }

If result is not ready, the response can be show as follows:

.. code-block:: javascript

    {
        "status": {
            "code": 104,
            "message": "result not ready, please retry later",
            "details": []
        },
        "out_columns": [],
        "scdb_session_id": "some_session_id",
        "affected_rows": "0"
    }

.. _async_send_result:

Asynchronous send result
------------------------

Automatically send the result to the user by post the following message when the result is available. To accomplish this, :ref:`query_result_callback_url <callback_url>` should be set.

+-------------------+--------------------+----------+-------------------------------------------------------------------+
| Field             | Type               | Required | Description                                                       |
+===================+====================+==========+===================================================================+
| status            | Status_            | Y        | See `Status code`_ for more information                           |
+-------------------+--------------------+----------+-------------------------------------------------------------------+
| out_columns       | Tensor_ list       | Y        | Query result, See Tensor_ for more information                    |
+-------------------+--------------------+----------+-------------------------------------------------------------------+
| scdb_session_id   | string             | Y        | Given by scdb when submit the query before, same with session_id_ |
+-------------------+--------------------+----------+-------------------------------------------------------------------+
| affected_rows     | int64              | Y        | The num of rows affected                                          |
+-------------------+--------------------+----------+-------------------------------------------------------------------+

.. _sync_api:

Synchronous
===========

.. _submit_and_get:

public/submit_and_get
---------------------

Submit a query to SCQL, SCQL will wait for all tasks to complete before returning the result to the use. 

Request
^^^^^^^

+----------------+-----------------+----------+-----------------------------------------------------+
| Field          | Type            | Required | Description                                         |
+================+=================+==========+=====================================================+
| header         | RequestHeader_  | N        | Common request header                               |
+----------------+-----------------+----------+-----------------------------------------------------+
| user           | SCDBCredential_ | Y        | User information                                    |
+----------------+-----------------+----------+-----------------------------------------------------+
| query          | string          | Y        | SCQL query to be run                                |
+----------------+-----------------+----------+-----------------------------------------------------+
| biz_request_id | string          | N        | Biz request id which often be unique per biz action |
+----------------+-----------------+----------+-----------------------------------------------------+
| db_name        | string          | Y        | Current database name                               |
+----------------+-----------------+----------+-----------------------------------------------------+

Response
^^^^^^^^

+-----------------+--------------+--------------------------------------------------+
| Field           | Type         | Description                                      |
+=================+==============+==================================================+
| status          | Status_      | See `Status code`_ for more information          |
+-----------------+--------------+--------------------------------------------------+
| out_columns     | Tensor_ list | Query result, See `Tensor`_ for more information |
+-----------------+--------------+--------------------------------------------------+
| scdb_session_id | string       | SCDB session id                                  |
+-----------------+--------------+--------------------------------------------------+
| affected_rows   | int64        | The num of rows affected                         |
+-----------------+--------------+--------------------------------------------------+


Example
^^^^^^^^

If you want submit a query `show databases`, you can send a request as follows:

.. code-block:: javascript

    {
        "user": {
            "user": {
                "account_system_type": "NATIVE_USER",
                "native_user": { "name": "someuser", "password": "somepassword" }
            }
        },
        "query": "show databases;",
        "biz_request_id": "1234",
        "db_name": "scdb"
    }

If successful, a response will be received like this:

.. code-block:: javascript

    {
        "status": {
            "code": 0,
            "message": "",
            "details": []
        },
        "out_columns": [
            {
                "name": "Database",
                "shape": {
                    "dim": [
                        {
                            "dim_value": "1"
                        },
                        {
                            "dim_value": "1"
                        }
                    ]
                },
                "elem_type": "STRING",
                "option": "VALUE",
                "annotation": null,
                "int32_data":[],
                "int64_data":[],
                "float_data":[],
                "double_data":[],
                "bool_data":[],
                "string_data": ["scdb"]
            }
        ],
        "scdb_session_id": "some_session_id",
        "affected_rows": "0"
    }

Message Structure
=================

RequestHeader
-------------

+----------------+---------------------+----------+--------------------------------------------------+
| Field          | Type                | Required | Description                                      |
+================+=====================+==========+==================================================+
| custom_headers | map<string, string> | Y        | Custom headers used to record custom information |
+----------------+---------------------+----------+--------------------------------------------------+


.. _scdb_credential:

SCDBCredential
--------------

+-----------+--------+----------+----------------------------------------------+
| Field     | Type   | Required | Description                                  |
+===========+========+==========+==============================================+
| user      | User_  | Y        | User information, contains password and name |
+-----------+--------+----------+----------------------------------------------+

User
^^^^

+---------------------+--------------------+----------+-------------------------+
| Field               | Type               | Required | Description             |
+=====================+====================+==========+=========================+
| account_system_type | AccountSystemType_ | Y        | Account Type            |
+---------------------+--------------------+----------+-------------------------+
| native_user         | NativeUser_        | Y        | Native user information |
+---------------------+--------------------+----------+-------------------------+

NativeUser
""""""""""

+----------+--------+----------+------------------+
| Field    | Type   | Required | Description      |
+==========+========+==========+==================+
| name     | string | Y        | name of user     |
+----------+--------+----------+------------------+
| password | string | Y        | password of user |
+----------+--------+----------+------------------+

Tensor
------

+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| Field       | Type               | Required | Description                                                                          |
+=============+====================+==========+======================================================================================+
| name        | string             | Y        | Tensor name                                                                          |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| shape       | TensorShape_       | Y        | It's normally [M] (a vector with M elements)                                         |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| elem_type   | PrimitiveDataType_ | Y        | The data type of the value data in tensor                                            |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| option      | TensorOptions_     | Y        | Tensor options                                                                       |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| annotation  | TensorAnnotation_  | N        | Carries physical status information, It MUST be there if the <option> is "Reference" |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| int32_data  | int32 list         | N        | The value data in tensor, for int8, int16, int32 data types                          |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| int64_data  | int64 list         | N        | The value data in tensor, for int64 and timestamp data types                         |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| float_data  | float list         | N        | The value data in tensor, for float32 data type                                      |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| double_data | double list        | N        | The value data in tensor, for float64 data type                                      |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| bool_data   | bool list          | N        | The value data in tensor, for bool data type                                         |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+
| string_data | string list        | N        | The value data in tensor, for string and datetime data types                         |
+-------------+--------------------+----------+--------------------------------------------------------------------------------------+

TensorShape
^^^^^^^^^^^

+-------+-----------------------------+----------+-------------+
| Field | Type                        | Required | Description |
+=======+=============================+==========+=============+
| dim   | TensorShape_Dimension_ list | Y        |             |
+-------+-----------------------------+----------+-------------+

TensorShape_Dimension
"""""""""""""""""""""

TensorShape_Dimension could be dim_value or dim_param.

+-----------+--------+
| Field     | Type   |
+===========+========+
| dim_value | int64  |
+-----------+--------+
| dim_param | string |
+-----------+--------+

TensorAnnotation
^^^^^^^^^^^^^^^^

+--------+---------------+----------+------------------+
| Field  | Type          | Required | Description      |
+========+===============+==========+==================+
| status | TensorStatus_ | Y        | Status of tensor |
+--------+---------------+----------+------------------+


Status
------

+----------+-------------------+----------+--------------------------------------------------------------------------+
| Field    | Type              | Required | Description                                                              |
+==========+===================+==========+==========================================================================+
| code     | int32             | Y        | The status code, see `Status code`_ for more information                 |
+----------+-------------------+----------+--------------------------------------------------------------------------+
| Messages | string            | N        | Message for recording the error information                              |
+----------+-------------------+----------+--------------------------------------------------------------------------+
| details  | protobuf.Any list | N        | A list of messages that carry the additional supplementary error details |
+----------+-------------------+----------+--------------------------------------------------------------------------+


Enum Values
===========

AccountSystemType
-----------------

+-------------+--------+----------------------+
| Name        | Number | Description          |
+=============+========+======================+
| UNKNOWN     | 0      | Unknown account type |
+-------------+--------+----------------------+
| NATIVE_USER | 1      | Native user type     |
+-------------+--------+----------------------+


PrimitiveDataType
-----------------

+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| Name                        | Number | Description                                                                                  |
+=============================+========+==============================================================================================+
| PrimitiveDataType_UNDEFINED | 0      | undefined data type                                                                          |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| INT8                        | 1      | the 8-bit signed integer type                                                                |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| INT16                       | 2      | the 16-bit signed integer type                                                               |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| INT32                       | 3      | the 32-bit signed integer type                                                               |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| INT64                       | 4      | the 64-bit signed integer type                                                               |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| FLOAT32                     | 5      | the 32-bit binary floating point type                                                        |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| FLOAT64                     | 6      | the 64-bit binary floating point type                                                        |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| BOOL                        | 7      | the bool data type                                                                           |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| STRING                      | 8      | the string data type                                                                         |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| DATETIME                    | 9      | see `datetime in mysql <https://dev.mysql.com/doc/refman/8.0/en/datetime.html>`_ to get more |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+
| TIMESTAMP                   | 10     | seconds since '1970-01-01 00:00:00' UTC                                                      |
+-----------------------------+--------+----------------------------------------------------------------------------------------------+

TensorOptions
-------------

+-----------+--------+---------------------------------+
| Name      | Number | Description                     |
+===========+========+=================================+
| VALUE     | 0      | A tensor with data              |
+-----------+--------+---------------------------------+
| REFERENCE | 1      | A tensor with reference (URI)   |
+-----------+--------+---------------------------------+
| VARIABLE  | 2      | A tensor variable (declaration) |
+-----------+--------+---------------------------------+

TensorStatus
------------

+----------------------+--------+----------------------------------------------------------------------+
| Name                 | Number | Description                                                          |
+======================+========+======================================================================+
| TENSORSTATUS_UNKNOWN | 0      | Unknown                                                              |
+----------------------+--------+----------------------------------------------------------------------+
| TENSORSTATUS_PRIVATE | 1      | Private                                                              |
+----------------------+--------+----------------------------------------------------------------------+
| TENSORSTATUS_SECRET  | 2      | Secret, usually in the form of secret sharing                        |
+----------------------+--------+----------------------------------------------------------------------+
| TENSORSTATUS_CIPHER  | 3      | Ciphertext, usually in the form of homomorphic encryption ciphertext |
+----------------------+--------+----------------------------------------------------------------------+
| TENSORSTATUS_PUBLIC  | 4      | Public                                                               |
+----------------------+--------+----------------------------------------------------------------------+

Status code
===========

+------------+-------------------------------------+---------------------------------------------------+
| Error code | Status Code                         | Description                                       |
+============+=====================================+===================================================+
| 0          | Code_OK                             | Success                                           |
+------------+-------------------------------------+---------------------------------------------------+
| 100        | Code_BAD_REQUEST                    | Invalid request body                              |
+------------+-------------------------------------+---------------------------------------------------+
| 101        | Code_UNAUTHENTICATED                | User authentication failed                        |
+------------+-------------------------------------+---------------------------------------------------+
| 102        | Code_SQL_PARSE_ERROR                | Invalid SCQL statement                            |
+------------+-------------------------------------+---------------------------------------------------+
| 103        | Code_INVALID_ARGUMENT               | Invalid parameter in Request                      |
+------------+-------------------------------------+---------------------------------------------------+
| 104        | Code_NOT_READY                      | Result not ready                                  |
+------------+-------------------------------------+---------------------------------------------------+
| 131        | Code_DDL_PERMISSION_DENIED          | User does not have permission to execute the DDL  |
+------------+-------------------------------------+---------------------------------------------------+
| 140        | Code_NOT_FOUND                      | General not found error                           |
+------------+-------------------------------------+---------------------------------------------------+
| 141        | Code_SESSION_NOT_FOUND              | SCDB session not found                            |
+------------+-------------------------------------+---------------------------------------------------+
| 160        | Code_CCL_CHECK_FAILED               | Query CCL check failed                            |
+------------+-------------------------------------+---------------------------------------------------+
| 201        | Code_STORAGE_ERROR                  | SCDB DB error                                     |
+------------+-------------------------------------+---------------------------------------------------+
| 300        | Code_INTERNAL                       | Server Internal Error                             |
+------------+-------------------------------------+---------------------------------------------------+
| 320        | Code_UNKNOWN_ENGINE_ERROR           | Unknown error occurs in Engine                    |
+------------+-------------------------------------+---------------------------------------------------+
| 332        | Code_ENGINE_RUNSQL_ERROR            | Unknown error occurs in Engine during RunSQL      |
+------------+-------------------------------------+---------------------------------------------------+
| 340        | Code_NOT_SUPPORTED                  | Feature not supported                             |
+------------+-------------------------------------+---------------------------------------------------+

