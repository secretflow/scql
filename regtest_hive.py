#!/usr/bin/env python3
"""
Run the SCQL single-party regtest against Hive tables.

The script uses checked-in regtest metadata from the current repo checkout,
creates Hive tables with generated CSV data, registers them in SCDB with
DB_TYPE='hive', then runs cmd/regtest/testdata/single_party.json.
"""

import json
import os
import random
import re
import subprocess
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent
TABLE_META_FILES = {
    "alice": REPO_ROOT / "pkg/util/mock/testdata/generated_table_alice.json",
    "bob": REPO_ROOT / "pkg/util/mock/testdata/generated_table_bob.json",
}
QUERY_FILE = REPO_ROOT / "cmd/regtest/testdata/single_party.json"
LOG_PATH = Path(os.environ.get("SCQL_HIVE_REGTEST_LOG", "/tmp/regtest_results.txt"))
SCDB_URL = os.environ.get("SCQL_SCDB_URL", "http://localhost:8080")
SCDB_PASS = os.environ.get("SCQL_ROOT_PASSWORD", "")
ROOT_PASSWORD_FILE = os.environ.get("SCQL_ROOT_PASSWORD_FILE", "")
BEELINE_BIN = os.environ.get("BEELINE_BIN", "beeline")
HIVE_JDBC_URL = os.environ.get("HIVE_JDBC_URL", "jdbc:hive2://localhost:10000")
HIVE_USER = os.environ.get("HIVE_USER", "root")

if not SCDB_PASS and ROOT_PASSWORD_FILE:
    SCDB_PASS = Path(ROOT_PASSWORD_FILE).read_text().strip()

for home_var in ("HIVE_HOME", "HADOOP_HOME"):
    home = os.environ.get(home_var)
    if home:
        os.environ["PATH"] = f"{home}/bin:" + os.environ["PATH"]

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
out = LOG_PATH.open("w")


def log(msg):
    print(msg, flush=True)
    out.write(msg + "\n")
    out.flush()


def load_table_meta(party):
    return json.loads(TABLE_META_FILES[party].read_text())


def hive_exec(sql, timeout=120):
    result = subprocess.run(
        [BEELINE_BIN, "-u", HIVE_JDBC_URL, "-n", HIVE_USER, "--silent=true", "-e", sql],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.returncode, result.stdout, result.stderr


TYPE_MAP = {
    "int": "INT",
    "float": "DOUBLE",
    "string": "STRING",
    "datetime": "STRING",
    "timestamp": "STRING",
}


def create_tables():
    log("=== Step 1: Creating tables in Hive ===")
    for party in ("alice", "bob"):
        data = load_table_meta(party)
        for table_name, table_info in data.items():
            db = table_info["db_name"]
            cols = table_info["columns"]
            hive_table = f"{db}.{party}_{table_name}"
            col_defs = ", ".join(
                f"{col['column_name']} {TYPE_MAP.get(col['dtype'], 'STRING')}" for col in cols
            )
            drop_sql = f"DROP TABLE IF EXISTS {hive_table}"
            create_sql = (
                f"CREATE TABLE {hive_table} ({col_defs}) "
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
            )
            rc, _, err = hive_exec(f"{drop_sql}; {create_sql}")
            if rc == 0:
                log(f"  Created {hive_table} ({len(cols)} cols)")
            else:
                log(f"  FAILED {hive_table}: {err[:100]}")


def generate_test_data():
    log("\n=== Step 2: Generating test data ===")
    random.seed(42)
    row_count = 20
    for party in ("alice", "bob"):
        data = load_table_meta(party)
        for table_name, table_info in data.items():
            db = table_info["db_name"]
            cols = table_info["columns"]
            hive_table = f"{db}.{party}_{table_name}"
            csv_path = Path(f"/tmp/data_{party}_{table_name}.csv")
            with csv_path.open("w") as f:
                for _ in range(row_count):
                    values = []
                    for col in cols:
                        dtype = col["dtype"]
                        if dtype == "int":
                            values.append(str(random.randint(-10, 20)))
                        elif dtype == "float":
                            values.append(f"{random.uniform(-10, 20):.4f}")
                        elif dtype == "string":
                            if random.random() < 0.15:
                                values.append("")
                            else:
                                values.append(random.choice(["abc", "def", "ghi", "xyz", " hello ", "test"]))
                        elif dtype in ("datetime", "timestamp"):
                            year = random.randint(2019, 2025)
                            month = random.randint(1, 12)
                            day = random.randint(1, 28)
                            hour = random.randint(0, 23)
                            minute = random.randint(0, 59)
                            second = random.randint(0, 59)
                            values.append(f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}")
                        else:
                            values.append("unknown")
                    f.write(",".join(values) + "\n")
            load_sql = f"LOAD DATA LOCAL INPATH '{csv_path}' OVERWRITE INTO TABLE {hive_table}"
            rc, _, err = hive_exec(load_sql)
            if rc == 0:
                log(f"  Loaded {row_count} rows into {hive_table}")
            else:
                log(f"  FAILED loading {hive_table}: {err[:100]}")


def scql_admin(sql, user="root", password=None):
    if password is None:
        password = SCDB_PASS
    response = requests.post(
        f"{SCDB_URL}/public/submit_query",
        json={
            "user": {
                "user": {
                    "account_system_type": "NATIVE_USER",
                    "native_user": {"name": user, "password": password},
                }
            },
            "query": sql,
        },
        timeout=10,
    ).json()
    code = response.get("status", {}).get("code", -1)
    if code != 0:
        return code, response.get("status", {}).get("message", "")
    session_id = response.get("scdb_session_id", "")
    for _ in range(60):
        fetch = requests.post(
            f"{SCDB_URL}/public/fetch_result",
            json={
                "user": {
                    "user": {
                        "account_system_type": "NATIVE_USER",
                        "native_user": {"name": user, "password": password},
                    }
                },
                "scdb_session_id": session_id,
            },
            timeout=10,
        ).json()
        fetch_code = fetch.get("status", {}).get("code", -1)
        if fetch_code == 0:
            return 0, "OK"
        if fetch_code in (104, 300):
            time.sleep(0.5)
            continue
        return fetch_code, fetch.get("status", {}).get("message", "")
    return -1, "timeout"


def register_tables():
    log("\n=== Step 3: Registering tables in SCQL ===")
    code, msg = scql_admin("CREATE DATABASE IF NOT EXISTS regtest")
    log(f"  Create DB regtest: code={code} {msg[:50]}")

    scql_type_map = {
        "int": "INT",
        "float": "DOUBLE",
        "string": "STRING",
        "datetime": "DATETIME",
        "timestamp": "TIMESTAMP",
    }

    for party, user, password in (("alice", "alice", "alice123"), ("bob", "bob", "bob123")):
        data = load_table_meta(party)
        for table_name, table_info in data.items():
            db = table_info["db_name"]
            cols = table_info["columns"]
            scql_table = f"regtest.{party}_{table_name}"
            col_defs = ", ".join(
                f"{col['column_name']} {scql_type_map.get(col['dtype'], 'STRING')}" for col in cols
            )
            scql_admin(f"DROP TABLE IF EXISTS {scql_table}", user=user, password=password)
            create_sql = (
                f"CREATE TABLE {scql_table} ({col_defs}) "
                f"REF_TABLE={db}.{party}_{table_name} DB_TYPE='hive'"
            )
            code, msg = scql_admin(create_sql, user=user, password=password)
            if code == 0:
                log(f"  Registered {scql_table} (owner={user})")
            else:
                log(f"  FAILED {scql_table}: code={code} {msg[:80]}")

    log("\n  Granting CCL...")
    for party, user, password in (("alice", "alice", "alice123"), ("bob", "bob", "bob123")):
        for table_suffix in ("tbl_0", "tbl_1", "tbl_2"):
            table = f"regtest.{party}_{table_suffix}"
            code, _ = scql_admin(
                f"GRANT SELECT PLAINTEXT ON {table} TO {party}", user=user, password=password
            )
            log(f"  GRANT {table} to {party}: code={code}")


def scql_query(sql, user="alice", password="alice123"):
    start = time.time()
    try:
        response = requests.post(
            f"{SCDB_URL}/public/submit_query",
            json={
                "user": {
                    "user": {
                        "account_system_type": "NATIVE_USER",
                        "native_user": {"name": user, "password": password},
                    }
                },
                "query": sql,
            },
            timeout=30,
        ).json()
    except Exception as exc:
        return time.time() - start, -999, 0, str(exc)

    code = response.get("status", {}).get("code", -1)
    if code != 0:
        return time.time() - start, code, 0, response.get("status", {}).get("message", "")

    session_id = response.get("scdb_session_id", "")
    for _ in range(120):
        try:
            fetch = requests.post(
                f"{SCDB_URL}/public/fetch_result",
                json={
                    "user": {
                        "user": {
                            "account_system_type": "NATIVE_USER",
                            "native_user": {"name": user, "password": password},
                        }
                    },
                    "scdb_session_id": session_id,
                },
                timeout=30,
            ).json()
        except Exception:
            time.sleep(1)
            continue

        fetch_code = fetch.get("status", {}).get("code", -1)
        if fetch_code == 0:
            rows = 0
            for col in fetch.get("out_columns", []):
                for dtype in (
                    "string_data",
                    "int32_data",
                    "int64_data",
                    "float_data",
                    "double_data",
                    "bool_data",
                ):
                    if col.get(dtype):
                        rows = max(rows, len(col[dtype]))
            return time.time() - start, 0, rows, ""
        if fetch_code in (104, 300):
            time.sleep(1)
            continue
        return time.time() - start, fetch_code, 0, fetch.get("status", {}).get("message", "")

    return time.time() - start, -1, 0, "timeout waiting for result"


def run_queries():
    log("\n=== Step 4: Running single_party.json queries ===")
    queries = json.loads(QUERY_FILE.read_text())["queries"]
    passed = 0
    failed = 0
    results = []

    for index, query in enumerate(queries, start=1):
        name = query.get("name", f"query_{index}")
        sql = query.get("query", "")
        sql = re.sub(r"\b(alice_tbl_\d+)", r"regtest.\1", sql)
        sql = re.sub(r"\b(bob_tbl_\d+)", r"regtest.\1", sql)
        if "bob_tbl" in sql and "alice_tbl" not in sql:
            user, password = "bob", "bob123"
        else:
            user, password = "alice", "alice123"
        elapsed, code, rows, msg = scql_query(sql, user=user, password=password)
        status = "PASS" if code == 0 else "FAIL"
        if code == 0:
            passed += 1
        else:
            failed += 1
        results.append((status, name, code, elapsed, rows, msg))
        log(f"  [{status}] {index:2d}. {name}: code={code} {elapsed:.1f}s {rows} rows")
        if code != 0:
            log(f"       error: {msg[:120]}")

    log("\n=== SUMMARY ===")
    log(f"Total: {len(queries)}, Passed: {passed}, Failed: {failed}")
    log(f"Pass rate: {passed}/{len(queries)} = {passed / len(queries) * 100:.1f}%")
    if failed > 0:
        log("\nFailed queries:")
        for status, name, code, elapsed, rows, msg in results:
            if status == "FAIL":
                log(f"  - {name} (code={code}): {msg[:100]}")


if __name__ == "__main__":
    log("=== SCQL Official Regtest on Hive ===")
    log(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    create_tables()
    generate_test_data()
    register_tables()
    run_queries()
    log("\n=== ALL DONE ===")
    out.close()
