Hive Data Source Support
========================

SCQL supports Apache Hive as a data source, allowing participants to keep their data in Hive tables and run privacy-preserving queries through SCQL without migrating data to MySQL.

Architecture
------------

SCQL connects to Hive through an Arrow Flight SQL middleware layer:

.. code-block:: text

    SCDB/Broker ──> SCQLEngine ──(Arrow Flight SQL)──> Java Flight SQL Server ──(JDBC)──> HiveServer2

The Java Arrow Flight SQL server (``run_java_arrow_server.sh`` + ``java-flight-sql-server/``) acts as a protocol adapter:

1. Receives SQL queries from SCQLEngine via gRPC (Arrow Flight SQL protocol)
2. Forwards queries to HiveServer2 via JDBC
3. Converts results to Apache Arrow format and streams back

This keeps the Hive-specific protocol adaptation in the middleware layer and stays aligned with Arrow's Java Flight SQL implementation.

Prerequisites
-------------

- Apache Hive with HiveServer2 running (tested with Hive 3.1.3)
- Java JDK 8+
- Hadoop (tested with 3.3.6, Local Mode is sufficient for testing)
- Maven 3.9+ (or allow ``run_java_arrow_server.sh`` to auto-download it)

Setup Guide
-----------

Step 1: Start Hive
^^^^^^^^^^^^^^^^^^^

Ensure HiveServer2 and Hive Metastore are running:

.. code-block:: bash

    # Start Metastore (port 9083)
    nohup hive --service metastore &

    # Start HiveServer2 (port 10000)
    nohup hive --service hiveserver2 &

Step 2: Initialize Test Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    cd examples/tutorial/hive
    bash init_hive_test_data.sh

This creates sample tables in Hive:

- ``alice.user_credit`` (ID, credit_rank, income, age) - 19 rows
- ``bob.user_stats`` (ID, order_amount, is_active) - 19 rows

Step 3: Start Arrow Flight SQL Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each participant runs their own Java Arrow Flight SQL server:

.. code-block:: bash

    # Alice's Flight Server (port 8815)
    # 默认会连接到与 party 同名的 Hive database, 即 alice
    bash examples/tutorial/hive/run_java_arrow_server.sh \
        --party alice --port 8815 \
        --backend hive --hive-host localhost --hive-port 10000

    # Bob's Flight Server (port 8816)
    # 默认会连接到与 party 同名的 Hive database, 即 bob
    bash examples/tutorial/hive/run_java_arrow_server.sh \
        --party bob --port 8816 \
        --backend hive --hive-host localhost --hive-port 10000

Or use the convenience script:

.. code-block:: bash

    SCQL_FLIGHT_BACKEND=hive bash examples/tutorial/hive/start_arrow_servers.sh

If you need a custom database name instead of the party name, set ``SCQL_HIVE_DATABASE``
or pass ``--hive-database`` explicitly.

Step 4: Configure SCQLEngine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``ARROWSQL`` as the datasource kind in the engine's gflags configuration. Example (``gflags_hive.conf``):

.. code-block::

    --listen_port=8003
    --datasource_router=embed
    --embed_router_conf={"datasources":[{"id":"ds001","name":"hive db","kind":"ARROWSQL","connection_str":"grpc://localhost:8815"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]}
    --enable_self_auth=false
    --enable_peer_auth=false

Key points:

- ``kind`` must be ``ARROWSQL`` (not ``HIVE``). The engine connects to the Arrow Flight SQL server, not directly to Hive.
- ``connection_str`` uses the ``grpc://`` scheme pointing to the Flight Server port.

Step 5: Register Tables in SCQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating virtual tables in SCQL, use ``DB_TYPE='hive'`` to enable Hive SQL dialect conversion:

.. code-block:: sql

    -- Create database
    CREATE DATABASE IF NOT EXISTS hive_test;

    -- Register table with Hive dialect (run as alice)
    CREATE TABLE hive_test.user_credit (
        ID STRING, credit_rank INT, income INT, age INT
    ) REF_TABLE=alice.user_credit DB_TYPE='hive';

    -- Grant CCL permissions
    GRANT SELECT PLAINTEXT(ID, credit_rank, income, age) ON hive_test.user_credit TO alice;
    GRANT SELECT PLAINTEXT(ID) ON hive_test.user_credit TO bob;

.. note::

    Tables must be created by the user who owns the data (i.e., Alice creates Alice's tables). The ``DB_TYPE='hive'`` flag tells SCQL to apply Hive SQL dialect conversion when generating queries for this table.

Step 6: Run Queries
^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    -- Single-party query
    SELECT credit_rank, COUNT(*) as cnt
    FROM hive_test.user_credit
    GROUP BY credit_rank;

    -- Cross-party query with privacy protection
    SELECT a.credit_rank, COUNT(*) as cnt
    FROM hive_test.user_credit a
    JOIN hive_test.user_stats b ON a.ID = b.ID
    WHERE b.is_active = 1
    GROUP BY a.credit_rank;

SQL Dialect Conversion
-----------------------

When ``DB_TYPE='hive'`` is set, SCQL automatically converts MySQL-style SQL to Hive-compatible SQL. The following conversions are applied:

Functions
^^^^^^^^^

- ``IFNULL(a, b)`` → ``nvl(a, b)``
- ``NOW()`` → ``current_timestamp()``
- ``CURDATE()`` → ``current_date()``
- ``TRUNCATE(x, d)`` → ``trunc(x, d)``
- ``SUBSTRING(s, pos, len)`` → ``substr(s, pos, len)``
- ``REPLACE(s, from, to)`` → ``replace(s, from, to)`` (Hive 3.x native)

Type Casting
^^^^^^^^^^^^

- ``CAST(x AS VARCHAR)`` → ``CAST(x AS STRING)``
- ``CAST(x AS SIGNED)`` → ``CAST(x AS BIGINT)``
- ``CAST(x AS DATETIME)`` → ``CAST(x AS TIMESTAMP)``

Other Conversions
^^^^^^^^^^^^^^^^^

- Comparison operators are wrapped in parentheses (Hive requirement)
- ``DIV`` integer division is handled
- ``MOD`` is converted to ``%`` operator

For the full list of supported functions and conversions, see the ``HiveDialect`` implementation in ``pkg/parser/format/format_dialect.go``.

Performance Characteristics
----------------------------

Based on testing with HiveServer2 in Hadoop Local Mode:

- Arrow Flight SQL middleware overhead: implementation-dependent, typically small compared with Hive query latency
- Hive query latency: ~200ms (warm connection) to ~900ms (cold connection)
- SCQL end-to-end single-party query: ~1s
- SCQL end-to-end cross-party query: ~2s
- HiveServer2 throughput: ~1 QPS (serial processing)

.. note::

    HiveServer2 processes queries serially. This is acceptable for SCQL's query pattern, where queries are submitted sequentially. For higher throughput requirements, consider using Spark Thrift Server as the Hive-compatible backend.

Known Limitations
-----------------

1. ``SUM(boolean)`` is not supported by Hive. Use ``SUM(CAST(col AS INT))`` instead.
2. ``DATE_FORMAT`` and ``STR_TO_DATE``: MySQL format specifiers (``%Y-%m-%d %H:%i:%S``) are automatically translated to Hive's Java SimpleDateFormat patterns (``yyyy-MM-dd HH:mm:ss``).
3. ``ADDDATE`` / ``SUBDATE`` are automatically converted to Hive's ``DATE_ADD`` / ``DATE_SUB``. Note: only DAY intervals are supported (Hive limitation).
4. ``GROUP_CONCAT`` needs to be converted to ``concat_ws(sep, collect_list(col))`` in Hive. This is not yet automated.
5. Hive supports ``LIMIT count OFFSET offset`` syntax (not MySQL's ``LIMIT offset, count``).
6. ``CAST(x AS UNSIGNED ...)`` is not supported by Hive. Use ``CAST(x AS BIGINT)`` instead.
7. ``ANY_VALUE`` is not supported by Hive.
8. Some date/time expressions still need extra validation, especially when Hive/JDBC returns ``DATETIME`` / ``TIMESTAMP`` values as strings.

Testing
-------

Run the official regression test suite against Hive:

.. code-block:: bash

    python3 regtest_hive.py

This runs 68 queries from ``single_party.json`` against Hive tables. Current pass rate: **61/68 (89.7%)**.

Run Hive dialect unit tests:

.. code-block:: bash

    go test -v ./pkg/planner/core/... -run "TestHiveDialectConversion" -timeout 120s

55 dialect conversion test cases, all passing.
