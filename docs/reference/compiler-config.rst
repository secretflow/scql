.. _compiler_config_options:

SCQL Compiler Configuration
============================

The SCQL compiler translates SQL queries into secure execution plans. This document describes the configuration options for the compiler API.


CompileSQLRequest
-----------------

The main request message for compiling SQL queries.

- **query** (*required*): The SQL query string to compile
- **db** (*optional*): Database name
- **issuer** (*required*): The party code of the query issuer (e.g., ``{"code": "alice"}``)
- **catalog** (*required*): Database catalog metadata describing tables and columns, see :ref:`catalog_config`
- **compile_opts** (*required*): Compilation options, see :ref:`compile_options`
- **issue_time** (*required*): Query issue timestamp, used for functions like ``NOW()``
- **security_config** (*required*): Security configuration, see :ref:`security_config`
- **additional_info** (*required*): Specifies additional information in response, see :ref:`additional_info_spec`


.. _compile_options:

CompileOptions
--------------

Compilation options that control the execution strategy.

- **spu_conf** (*required*): SPU runtime configuration (protocol, field, etc.), see `SPU documentation`_
- **batched** (*required*): Whether to run in streaming mode
- **psi_algorithm_type** (*optional*): PSI algorithm type: ``UNSPECIFIED`` (auto), ``ECDH_PSI``, ``KKRT_PSI``, ``RR22``

**SPU Configuration Example:**

.. code-block:: json

    {
      "spu_conf": {
        "protocol": "SEMI2K",
        "field": "FM64"
      },
      "batched": false
    }

**Supported SPU Protocols:**

- ``SEMI2K``: Semi-honest 2PC/MPC (supports N parties)
- ``CHEETAH``: Semi-honest 2PC (2 parties only)
- ``ABY3``: Semi-honest 3PC (3 parties only)

**Supported Fields:**

- ``FM32``: 32-bit finite field
- ``FM64``: 64-bit finite field
- ``FM128``: 128-bit finite field


.. _security_config:

CompilerSecurityConfig
----------------------

Security configuration that controls data visibility and security relaxations.

- **global_relaxation** (*required*): Global security relaxation settings, see :ref:`global_security_relaxation`
- **column_relaxation_list** (*optional*): Per-column security relaxations, see :ref:`column_security_relaxation`
- **reverse_inference_conf** (*required*): Reverse inference detection config, see :ref:`reverse_inference_config`
- **column_visibility_list** (*optional*): User-specified column visibility, see :ref:`column_visibility`
- **result_security_conf** (*optional*): Result-level security config, see :ref:`result_security_config`


.. _global_security_relaxation:

GlobalSecurityRelaxation
~~~~~~~~~~~~~~~~~~~~~~~~

Global security relaxation that applies to all data.

- **reveal_group_count** (*optional*): Allow group counts to be visible in plaintext
- **reveal_group_mark** (*optional*): Allow group marks to be visible in plaintext
- **reveal_key_after_join** (*optional*): Allow join keys to be visible in plaintext after join (intersection only). When enabled, the compiler will prefer PSI join over secret join for better performance
- **reveal_filter_mask** (*optional*): Allow filter mask to be used in plaintext for subsequent computations

**Example:**

.. code-block:: json

    {
      "global_relaxation": {
        "reveal_group_count": false,
        "reveal_group_mark": false,
        "reveal_key_after_join": true,
        "reveal_filter_mask": false
      }
    }


.. _column_security_relaxation:

ColumnSecurityRelaxation
~~~~~~~~~~~~~~~~~~~~~~~~

Per-column security relaxation for fine-grained control.

- **database** (*required*): Database name
- **table** (*required*): Table name
- **column** (*required*): Column name
- **reveal_key_after_join** (*optional*): Allow join keys to be visible in plaintext after join (intersection only). When enabled, the compiler will prefer PSI join over secret join for better performance
- **reveal_filter_mask** (*optional*): Allow filter mask to be used in plaintext for subsequent computations

.. note::
    Column-level settings use logical OR with global settings. If global ``reveal_key_after_join`` is ``true`` and column-level is ``false``, the result is ``true``. If global is ``false`` and column-level is ``true``, the result is ``true``. Only when both are ``false`` is the final result ``false``. The same logic applies to ``reveal_filter_mask``.

**Example:**

.. code-block:: json

    {
      "column_relaxation_list": [
        {
          "database": "alice",
          "table": "user_credit",
          "column": "ID",
          "reveal_key_after_join": true
        }
      ]
    }


.. _reverse_inference_config:

ReverseInferenceConfig
~~~~~~~~~~~~~~~~~~~~~~

Configuration for reverse inference.

- **enable_reverse_inference** (*required*): Infer intermediate visibility from result visibility to enable plaintext compute

When enabled, the compiler will infer the visibility of intermediate computation steps based on the final result visibility, increasing the possibility of plaintext computation and improving performance.

**Example:**

.. code-block:: json

    {
      "reverse_inference_conf": {
        "enable_reverse_inference": true
      }
    }


.. _column_visibility:

ColumnVisibility
~~~~~~~~~~~~~~~~

Specifies which parties can see specific columns.

- **database** (*required*): Database name
- **table** (*required*): Table name
- **column** (*required*): Column name
- **visible_parties** (*required*): List of party codes that can see this column during computation (in addition to the owner)

By default, only the column owner can see the data. Use this to grant visibility to other parties. Adding column visibility will make the execution graph prefer plaintext operators, improving execution efficiency.

**Example:**

.. code-block:: json

    {
      "column_visibility_list": [
        {
          "database": "alice",
          "table": "user_credit",
          "column": "age",
          "visible_parties": [
            {"code": "bob"}
          ]
        }
      ]
    }


.. _result_security_config:

ResultSecurityConfig
~~~~~~~~~~~~~~~~~~~~

Security configuration that affects query results.

- **groupby_threshold** (*optional*): Minimum number of rows in a group for GROUP BY results (default: 4)

Groups with fewer rows than this threshold will be filtered out to prevent inference attacks.

**Example:**

.. code-block:: json

    {
      "result_security_conf": {
        "groupby_threshold": 4
      }
    }


.. _additional_info_spec:

AdditionalInfoSpec
------------------

Specifies what additional information should be included in the response.

- **need_operator_graph** (*optional*): Whether to return the operator graph for debugging (default: ``false``)


.. _catalog_config:

Catalog Configuration
---------------------

The catalog defines the database schema. See ``api/interpreter.proto`` for the complete definition.

**Basic Structure:**

.. code-block:: json

    {
      "catalog": {
        "tables": [
          {
            "table_name": "user_credit",
            "columns": [
              {
                "name": "ID",
                "type": "string",
                "ordinal_position": 1
              },
              {
                "name": "age",
                "type": "int",
                "ordinal_position": 2
              }
            ],
            "ref_table": "alice.user_credit",
            "db_type": "mysql",
            "owner": {"code": "alice"}
          }
        ]
      }
    }

**Supported Column Types:**

- ``int``, ``long``, ``string``, ``float``, ``double``
- ``datetime``, ``timestamp``

**Supported Database Types:**

- ``mysql``, ``postgres``, ``sqlite``
- ``csvdb`` (for CSV files)
- ``arrowsql`` (for Arrow Flight servers)

See Also
--------

- :doc:`/reference/engine-config` - Engine configuration reference
- :doc:`/intro/opencore-quickstart` - Quick start guide
- `SPU Documentation <https://github.com/secretflow/spu>`_ - SPU protocol details

.. _SPU documentation: https://github.com/secretflow/spu
