SCQL OpenCore Quickstart
========================

This guide shows how to use SCQL's native compiler + engine architecture.

Architecture Overview
---------------------

SCQL consists of two components:

- **Compiler**: Translates SQL queries into secure execution plans
- **Engine**: Executes plans using MPC protocols

Workflow:

1. Compile SQL to execution plan
2. Send plan to engine nodes
3. Get query results

Quick Start
-----------

Setup
^^^^^

.. code-block:: bash

    git clone https://github.com/secretflow/scql.git
    cd scql

    # Start engines
    cd examples/tutorial
    bash setup.sh
    bash project_bootstrap.sh

Run Example
^^^^^^^^^^^

.. code-block:: bash

    # From examples/tutorial
    ./opencore-demo --config example_config.json

This compiles a SQL query and executes it across Alice and Bob's engines.

**Output:**

.. code-block:: text

    INFO[0000] Step 1: Compiling SQL to execution plan...
    INFO[0000] Compilation successful. SubGraphs: 2
    INFO[0000] Step 2: Executing plan on engine...
    INFO[0000] Calling engine for party bob at localhost:8005...
    INFO[0000] Calling engine for party alice at localhost:8003...
    INFO[0000] Party alice execution succeeded
    INFO[0000] Party bob execution succeeded
    INFO[0000] Step 3: Query results:
    [fetch]
    17 rows in set: (0s)
    +--------+-------------+-----+-----------------+-----------+
    |   ID   | credit_rank | age |  order_amount   | is_active |
    +--------+-------------+-----+-----------------+-----------+
    | id0011 |           5 |  28 | 9816.2001953125 |         1 |
    | id0019 |           6 |  25 | 9816.2001953125 |         1 |
    | id0014 |           5 |  28 | 9816.2001953125 |         1 |
    | id0001 |           6 |  20 |            3598 |         1 |
    | id0006 |           5 |  25 |            3598 |         1 |
    | id0010 |           5 |  25 |             322 |         0 |
    | id0009 |           6 |  30 |            3598 |         1 |
    | id0002 |           5 |  19 |             100 |         0 |
    | id0005 |           6 |  30 |          4985.5 |         1 |
    | id0016 |           5 |  26 | 9816.2001953125 |         1 |
    | id0017 |           5 |  27 |            3598 |         1 |
    | id0013 |           5 |  25 |             322 |         0 |
    | id0008 |           6 |  50 | 9816.2001953125 |         1 |
    | id0003 |           6 |  32 |            2549 |         1 |
    | id0007 |           6 |  28 |             322 |         0 |
    | id0012 |           6 |  50 |            3598 |         1 |
    | id0020 |           5 |  28 | 9816.2001953125 |         1 |
    +--------+-------------+-----+-----------------+-----------+

Configuration
-------------

This is the configuration for ``examples/opencore-demo``, containing the minimal configuration required when integrating compiler + engine. For more tutorial information, please refer to the ``examples/tutorial`` directory.

**Minimal Example:**

.. code-block:: examples/tutorial/example_config.json

    {
        "sql": "SELECT a.ID, a.credit_rank, a.age, b.order_amount, b.is_active FROM user_credit a JOIN user_stats b ON a.ID = b.ID WHERE a.age > 18",
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
                            "name": "credit_rank",
                            "type": "int",
                            "ordinal_position": 2
                        },
                        {
                            "name": "income",
                            "type": "int",
                            "ordinal_position": 3
                        },
                        {
                            "name": "age",
                            "type": "int",
                            "ordinal_position": 4
                        }
                    ],
                    "ref_table": "alice.user_credit",
                    "db_type": "mysql",
                    "owner": {
                        "code": "alice"
                    }
                },
                {
                    "table_name": "user_stats",
                    "columns": [
                        {
                            "name": "ID",
                            "type": "string",
                            "ordinal_position": 1
                        },
                        {
                            "name": "order_amount",
                            "type": "float",
                            "ordinal_position": 2
                        },
                        {
                            "name": "is_active",
                            "type": "int",
                            "ordinal_position": 3
                        }
                    ],
                    "ref_table": "bob.user_stats",
                    "db_type": "mysql",
                    "owner": {
                        "code": "bob"
                    }
                }
            ]
        },
        "security_conf": {
            "reverse_inference_conf": {
                "enable_reverse_inference": false
            }
        },
        "engine_endpoints": {
            "alice": "localhost:8003",
            "bob": "localhost:8005"
        },
        "engine_link_endpoints": {
            "alice": "tutorial-engine_alice-1:8004",
            "bob": "tutorial-engine_bob-1:8004"
        },
        "engine_client_type": "GRPC",
        "engine_timeout": 300,
        "tls_ca_cert": "./tls/root-ca.crt",
        "issuer": "alice",
        "compile_opts": {
            "spu_conf": {
                "protocol": "SEMI2K",
                "field": "FM64"
            },
            "batched": false
        }
    }


Integration
-----------

Basic Usage
^^^^^^^^^^^

.. code-block:: go

    import (
        "github.com/secretflow/scql/pkg/interpreter/compiler"
        "github.com/secretflow/scql/pkg/executor"
    )

    ...

    // 1. Compile SQL
    req := &v1.CompileSQLRequest{
        Query:          sql,
        Issuer:         &pb.PartyId{Code: "alice"},
        Catalog:        catalog,
        CompileOpts:    compileOpts,
        SecurityConfig: securityConfig,
    }
    plan, err := compiler.Compile(ctx, req)

    // 2. Execute on engines
    client, _ := executor.NewEngineClient(
		clientType,
		timeout,
		tlsConfig,
		"application/json",
		"",
	)
    ...
    result, err := client.RunExecutionPlan(
        engineEndpoint,
        "", // no credential
        execReq,
    )

For complete code, see ``examples/opencore-demo/main.go``.

Next Steps
----------

- Compiler configuration: :doc:`/reference/compiler-config`
- Engine configuration: :doc:`/reference/engine-config`
- Implementation status: :doc:`/reference/implementation-status`
- Examples: ``examples/opencore-demo/`` and ``examples/tutorial/``
