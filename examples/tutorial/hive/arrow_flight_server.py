#!/usr/bin/env python3
"""
Arrow Flight SQL Server
Supports two backend modes:
1. DuckDB mode (default) - for local testing with in-memory database
2. Hive mode - connects to a real HiveServer2

This server implements core Arrow Flight SQL protocol features:
- GetFlightInfo: handles SQL query requests (parses CommandStatementQuery protobuf)
- DoGet: returns query results

Usage:
    # DuckDB mode (for testing)
    python3 arrow_flight_server.py --party alice --port 8815

    # Hive mode (real Hive backend)
    python3 arrow_flight_server.py --port 8815 --backend hive \
        --hive-host hive.example.com --hive-port 10000 \
        --hive-user hive --hive-database default

Dependencies:
    pip install pyarrow duckdb
    # Hive mode additionally requires:
    pip install pyhive thrift thrift-sasl
"""

import argparse
import pyarrow as pa
import pyarrow.flight as flight


# === Hive SQL Dialect Converter ===
class HiveDialectConverter:
    """Converts MySQL/standard SQL to Hive-compatible SQL.

    Handles:
    - Trailing semicolons (Hive rejects them)
    - Database/party prefixes (strip when already connected to right DB)
    - IFNULL(a, b) -> NVL(a, b)
    - NOW() -> CURRENT_TIMESTAMP
    - CAST(x AS SIGNED/UNSIGNED) -> CAST(x AS BIGINT)
    - CAST(x AS VARCHAR/CHAR) -> CAST(x AS STRING)
    """

    def __init__(self, party: str = "", database: str = ""):
        import re
        self.party = party
        self.database = database
        prefixes = {"alice", "bob", "default", "hive_demo"}
        if party:
            prefixes.add(party.lower())
        if database:
            prefixes.add(database.lower())
        self._prefix_pattern = re.compile(
            r'\b(?:' + '|'.join(re.escape(p) for p in prefixes) + r')\.',
            re.IGNORECASE
        )

    def convert(self, query: str) -> str:
        """Apply all dialect conversions to a SQL query."""
        import re
        # Strip trailing semicolons
        query = query.rstrip().rstrip(';').rstrip()
        # Strip database/party prefixes
        query = self._prefix_pattern.sub('', query)
        # IFNULL -> NVL (consistent with Go HiveDialect)
        query = re.sub(r'\bIFNULL\s*\(', 'NVL(', query, flags=re.IGNORECASE)
        # NOW() -> CURRENT_TIMESTAMP
        query = re.sub(r'\bNOW\s*\(\s*\)', 'CURRENT_TIMESTAMP', query, flags=re.IGNORECASE)
        # CAST types: SIGNED/UNSIGNED -> BIGINT
        query = re.sub(
            r'\bCAST\s*\((.+?)\s+AS\s+(?:SIGNED|UNSIGNED)(?:\s+INTEGER)?\s*\)',
            r'CAST(\1 AS BIGINT)', query, flags=re.IGNORECASE)
        # CAST types: VARCHAR/CHAR -> STRING
        query = re.sub(
            r'\bCAST\s*\((.+?)\s+AS\s+(?:VARCHAR|CHAR)(?:\s*\(\s*\d+\s*\))?\s*\)',
            r'CAST(\1 AS STRING)', query, flags=re.IGNORECASE)
        return query


# =============================================================================
# Database backend abstraction
# =============================================================================

class DatabaseBackend:
    """Abstract database backend interface."""

    def execute(self, query: str) -> pa.Table:
        """Execute SQL and return an Arrow Table."""
        raise NotImplementedError

    def close(self):
        """Close the connection."""
        pass


class DuckDBBackend(DatabaseBackend):
    """DuckDB backend - for local testing."""

    def __init__(self, party: str = None, init_data: bool = True):
        import duckdb
        self.conn = duckdb.connect(":memory:")
        self.party = party
        if init_data and party:
            self._init_test_data()

    def _init_test_data(self):
        """Initialize test data matching SCQL tutorial examples."""
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS "default"')
        self.conn.execute('SET search_path TO "default"')

        if self.party == "alice":
            self.conn.execute('''
                CREATE TABLE "default".user_credit (
                    ID VARCHAR PRIMARY KEY,
                    credit_rank INTEGER,
                    income INTEGER,
                    age INTEGER
                )
            ''')
            self.conn.execute('''
                INSERT INTO "default".user_credit VALUES
                    ('id0001', 6, 100000, 20),
                    ('id0002', 5, 90000, 19),
                    ('id0003', 6, 89700, 32),
                    ('id0005', 6, 607000, 30),
                    ('id0006', 5, 30070, 25),
                    ('id0007', 6, 12070, 28),
                    ('id0008', 6, 200800, 50),
                    ('id0009', 6, 607000, 30),
                    ('id0010', 5, 30070, 25),
                    ('id0011', 5, 12070, 28),
                    ('id0012', 6, 200800, 50),
                    ('id0013', 5, 30070, 25),
                    ('id0014', 5, 12070, 28),
                    ('id0015', 6, 200800, 18),
                    ('id0016', 5, 30070, 26),
                    ('id0017', 5, 12070, 27),
                    ('id0018', 6, 200800, 16),
                    ('id0019', 6, 30070, 25),
                    ('id0020', 5, 12070, 28)
            ''')
            print(f"[DuckDB] Initialized Alice user_credit table (19 rows)")

        elif self.party == "bob":
            self.conn.execute('''
                CREATE TABLE "default".user_stats (
                    ID VARCHAR PRIMARY KEY,
                    order_amount INTEGER,
                    is_active INTEGER
                )
            ''')
            self.conn.execute('''
                INSERT INTO "default".user_stats VALUES
                    ('id0001', 5000, 1),
                    ('id0002', 3000, 1),
                    ('id0003', 8000, 0),
                    ('id0005', 12000, 1),
                    ('id0006', 1500, 1),
                    ('id0007', 2500, 0),
                    ('id0008', 9500, 1),
                    ('id0009', 7000, 1),
                    ('id0010', 500, 0),
                    ('id0011', 3500, 1),
                    ('id0012', 15000, 1),
                    ('id0013', 2000, 0),
                    ('id0014', 4500, 1),
                    ('id0015', 6500, 1),
                    ('id0016', 1000, 0),
                    ('id0017', 8500, 1),
                    ('id0018', 11000, 1),
                    ('id0019', 3200, 1),
                    ('id0020', 7500, 0)
            ''')
            print(f"[DuckDB] Initialized Bob user_stats table (19 rows)")

    def execute(self, query: str) -> pa.Table:
        return self.conn.execute(query).fetch_arrow_table()

    def close(self):
        self.conn.close()


class HiveBackend(DatabaseBackend):
    """Hive backend - connects to a real HiveServer2."""

    def __init__(self, host: str, port: int = 10000, username: str = None,
                 password: str = None, database: str = "default",
                 auth: str = "NONE"):
        """
        Initialize Hive connection.

        Args:
            host: HiveServer2 hostname
            port: HiveServer2 port (default 10000)
            username: Username
            password: Password (for LDAP auth)
            database: Default database
            auth: Auth method (NONE, LDAP, KERBEROS)
        """
        try:
            from pyhive import hive
        except ImportError:
            raise ImportError(
                "Hive backend requires pyhive: pip install pyhive thrift thrift-sasl"
            )

        self.host = host
        self.port = port
        self.database = database

        conn_kwargs = {
            "host": host,
            "port": port,
            "database": database,
        }

        if username:
            conn_kwargs["username"] = username
        if auth and auth != "NONE":
            conn_kwargs["auth"] = auth
        if password and auth == "LDAP":
            conn_kwargs["password"] = password

        print(f"[Hive] Connecting to {host}:{port}/{database} (auth={auth})")
        self.conn = hive.connect(**conn_kwargs)
        self.cursor = self.conn.cursor()
        print(f"[Hive] Connected successfully")

    def execute(self, query: str) -> pa.Table:
        """Execute Hive SQL and return an Arrow Table."""
        print(f"[Hive] Executing: {query[:100]}...")

        self.cursor.execute(query)

        # Get column info
        columns = [desc[0] for desc in self.cursor.description]
        col_types = [desc[1] for desc in self.cursor.description]

        # Fetch all data
        rows = self.cursor.fetchall()

        # Convert to Arrow Table
        if not rows:
            fields = [pa.field(name, self._hive_type_to_arrow(t))
                      for name, t in zip(columns, col_types)]
            schema = pa.schema(fields)
            return pa.table({name: [] for name in columns}, schema=schema)

        # Organize by columns
        col_data = {name: [] for name in columns}
        for row in rows:
            for i, value in enumerate(row):
                col_data[columns[i]].append(value)

        arrays = {}
        for name, data in col_data.items():
            arrays[name] = pa.array(data)

        return pa.table(arrays)

    def _hive_type_to_arrow(self, hive_type: str) -> pa.DataType:
        """Map Hive types to Arrow types."""
        hive_type = hive_type.upper()
        type_map = {
            "STRING": pa.string(),
            "VARCHAR": pa.string(),
            "CHAR": pa.string(),
            "INT": pa.int32(),
            "INTEGER": pa.int32(),
            "BIGINT": pa.int64(),
            "SMALLINT": pa.int16(),
            "TINYINT": pa.int8(),
            "FLOAT": pa.float32(),
            "DOUBLE": pa.float64(),
            "DECIMAL": pa.decimal128(38, 18),
            "BOOLEAN": pa.bool_(),
            "BINARY": pa.binary(),
            "TIMESTAMP": pa.timestamp("us"),
            "DATE": pa.date32(),
        }
        return type_map.get(hive_type, pa.string())

    def close(self):
        self.cursor.close()
        self.conn.close()
        print("[Hive] Connection closed")


def create_backend(args) -> DatabaseBackend:
    """Create database backend based on command-line arguments."""
    if args.backend == "hive":
        if not args.hive_host:
            raise ValueError("Hive mode requires --hive-host")
        return HiveBackend(
            host=args.hive_host,
            port=args.hive_port,
            username=args.hive_user,
            password=args.hive_password,
            database=args.hive_database,
            auth=args.hive_auth,
        )
    else:
        return DuckDBBackend(party=args.party, init_data=True)


# =============================================================================
# Protobuf parsing (using google.protobuf library)
# =============================================================================

def parse_flight_sql_command(data: bytes) -> str:
    """
    Parse Arrow Flight SQL CommandStatementQuery protobuf message.
    Uses google.protobuf to unwrap Any wrapper if present,
    then extracts the query string from field 1.
    """
    if not data:
        return ""

    try:
        from google.protobuf import descriptor_pb2
        from google.protobuf.any_pb2 import Any as AnyProto
        from google.protobuf.descriptor_pool import DescriptorPool
        from google.protobuf.message_factory import GetMessageClass

        if b"type.googleapis.com" in data:
            any_msg = AnyProto()
            any_msg.ParseFromString(data)
            data = any_msg.value

        file_desc_proto = descriptor_pb2.FileDescriptorProto(
            name="flight_sql.proto",
            package="arrow.flight.protocol.sql",
            message_type=[descriptor_pb2.DescriptorProto(
                name="CommandStatementQuery",
                field=[
                    descriptor_pb2.FieldDescriptorProto(
                        name="query", number=1, type=9, label=1,
                    ),
                    descriptor_pb2.FieldDescriptorProto(
                        name="transaction_id", number=2, type=9, label=1,
                    ),
                ],
            )],
        )
        pool = DescriptorPool()
        pool.Add(file_desc_proto)
        desc = pool.FindMessageTypeByName(
            "arrow.flight.protocol.sql.CommandStatementQuery"
        )
        CmdClass = GetMessageClass(desc)
        msg = CmdClass()
        msg.ParseFromString(data)
        return msg.query

    except Exception as e:
        print(f"[warning] protobuf parse failed, falling back to raw decode: {e}")
        return data.decode("utf-8", errors="replace")


# =============================================================================
# Arrow Flight SQL Server
# =============================================================================

class FlightSqlServer(flight.FlightServerBase):
    """
    Arrow Flight SQL server implementation.
    Supports DuckDB (testing) and Hive (production) backends.
    """

    def __init__(self, backend: DatabaseBackend, host="0.0.0.0", port=8815,
                 party="unknown"):
        location = f"grpc://0.0.0.0:{port}"
        super().__init__(location)
        self.backend = backend
        self.party = party
        self._port = port
        self._host = host
        self._queries = {}  # ticket_id -> query
        self._ticket_counter = 0
        print(f"[{party}] Arrow Flight SQL server started on port {port}")

    def _preprocess_query(self, query: str) -> str:
        """
        Preprocess SQL query for the target backend.

        Uses HiveDialectConverter for Hive backend (full dialect conversion).
        For DuckDB: strips party name prefixes and semicolons.
        """
        import re

        if isinstance(self.backend, HiveBackend):
            if not hasattr(self, '_dialect'):
                self._dialect = HiveDialectConverter(
                    party=self.party, database=self.backend.database
                )
            return self._dialect.convert(query)
        else:
            # DuckDB: strip semicolons and party name prefixes
            query = query.rstrip().rstrip(';').rstrip()
            query = re.sub(r'\b(?:alice|bob|default|hive_demo)\.', '', query, flags=re.IGNORECASE)
            return query

    def _generate_ticket(self, query: str) -> bytes:
        """Generate a unique ticket ID."""
        self._ticket_counter += 1
        ticket_id = f"{self.party}_{self._ticket_counter}"
        self._queries[ticket_id] = query
        return ticket_id.encode("utf-8")

    def get_flight_info(self, context, descriptor):
        """
        Handle GetFlightInfo requests.
        Arrow Flight SQL clients send SQL queries through this method.
        """
        if descriptor.descriptor_type == flight.DescriptorType.CMD:
            query = parse_flight_sql_command(descriptor.command)
        elif descriptor.descriptor_type == flight.DescriptorType.PATH:
            table_name = "/".join(
                p.decode() if isinstance(p, bytes) else p
                for p in descriptor.path
            )
            query = f"SELECT * FROM {table_name}"
        else:
            raise flight.FlightUnavailableError("Unsupported descriptor type")

        query = self._preprocess_query(query)
        print(f"[{self.party}] GetFlightInfo - Query: {query[:100]}...")

        try:
            result = self.backend.execute(query)
            schema = result.schema
            num_rows = result.num_rows

            ticket_bytes = self._generate_ticket(query)
            ticket = flight.Ticket(ticket_bytes)

            location = flight.Location.for_grpc_tcp("localhost", self._port)
            endpoint = flight.FlightEndpoint(ticket, [location])

            info = flight.FlightInfo(
                schema,
                descriptor,
                [endpoint],
                num_rows,
                -1
            )

            print(f"[{self.party}] FlightInfo - rows: {num_rows}, columns: {len(schema)}")
            return info

        except Exception as e:
            print(f"[{self.party}] Query error: {e}")
            raise flight.FlightServerError(f"Query execution failed: {e}")

    def do_get(self, context, ticket):
        """Handle DoGet requests, returning query results."""
        ticket_data = ticket.ticket.decode("utf-8")

        if ticket_data in self._queries:
            query = self._queries.pop(ticket_data)
        else:
            query = ticket_data

        query = self._preprocess_query(query)
        print(f"[{self.party}] DoGet - Query: {query[:100]}...")

        try:
            result = self.backend.execute(query)
            print(f"[{self.party}] Returning {result.num_rows} rows, {result.num_columns} columns")
            return flight.RecordBatchStream(result)
        except Exception as e:
            print(f"[{self.party}] Query error: {e}")
            raise flight.FlightServerError(f"Query execution failed: {e}")

    def do_action(self, context, action):
        """Handle Action requests."""
        action_type = action.type
        print(f"[{self.party}] Action: {action_type}")

        if action_type == "healthcheck":
            yield flight.Result(b"ok")
        else:
            yield flight.Result(b"")

    def list_actions(self, context):
        """List supported actions."""
        return [("healthcheck", "Health check")]

    def shutdown(self):
        """Shut down the server and close backend connections."""
        self.backend.close()
        super().shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Arrow Flight SQL Server - supports DuckDB (testing) and Hive (production) backends",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # DuckDB mode (local testing)
  python3 arrow_flight_server.py --party alice --port 8815

  # Hive mode (real Hive backend)
  python3 arrow_flight_server.py --port 8815 --backend hive \\
      --hive-host hive.example.com --hive-port 10000 \\
      --hive-user hive --hive-database default
        """
    )

    # Basic arguments
    parser.add_argument("--party", type=str, default="alice",
                        help="Party name (used for logging)")
    parser.add_argument("--port", type=int, default=8815,
                        help="Arrow Flight server port (default: 8815)")
    parser.add_argument("--host", type=str, default="0.0.0.0",
                        help="Listen address (default: 0.0.0.0)")

    # Backend selection
    parser.add_argument("--backend", type=str, default="duckdb",
                        choices=["duckdb", "hive"],
                        help="Database backend (default: duckdb)")

    # Hive connection arguments
    hive_group = parser.add_argument_group("Hive connection arguments")
    hive_group.add_argument("--hive-host", type=str,
                            help="HiveServer2 hostname")
    hive_group.add_argument("--hive-port", type=int, default=10000,
                            help="HiveServer2 port (default: 10000)")
    hive_group.add_argument("--hive-user", type=str,
                            help="Hive username")
    hive_group.add_argument("--hive-password", type=str,
                            help="Hive password (for LDAP auth)")
    hive_group.add_argument("--hive-database", type=str, default="default",
                            help="Hive database (default: default)")
    hive_group.add_argument("--hive-auth", type=str, default="NONE",
                            choices=["NONE", "LDAP", "KERBEROS"],
                            help="Hive auth method (default: NONE)")

    args = parser.parse_args()

    # Create backend
    print("=" * 60)
    print("Arrow Flight SQL Server")
    print("=" * 60)

    try:
        backend = create_backend(args)
    except Exception as e:
        print(f"[ERROR] Failed to create backend: {e}")
        return 1

    # Create and start server
    server = FlightSqlServer(
        backend=backend,
        host=args.host,
        port=args.port,
        party=args.party
    )

    print(f"Backend: {args.backend.upper()}")
    if args.backend == "hive":
        print(f"Hive: {args.hive_host}:{args.hive_port}/{args.hive_database}")
    print(f"Listen: grpc://{args.host}:{args.port}")
    print("-" * 60)
    print("Press Ctrl+C to stop the server")
    print()

    try:
        server.serve()
    except KeyboardInterrupt:
        print(f"\n[{args.party}] Shutting down server...")
        server.shutdown()
        print(f"[{args.party}] Server stopped")

    return 0


if __name__ == "__main__":
    exit(main())
