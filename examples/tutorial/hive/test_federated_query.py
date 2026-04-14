#!/usr/bin/env python3
"""
SCQL federated query test script for Hive backend.
Tests basic connectivity and cross-party queries.
"""

import os
import requests
import json
import time

SCDB_URL = os.environ.get("SCDB_URL", "http://localhost:8080")
ROOT_PASSWORD = os.environ.get("SCQL_ROOT_PASSWORD", "")

def execute_sql(sql, user="root", password=ROOT_PASSWORD):
    """Execute an SCQL query via SCDB API."""
    url = f"{SCDB_URL}/public/submit_query"
    payload = {
        "user": {"user": {"account_system_type": "NATIVE_USER", "native_user": {"name": user, "password": password}}},
        "query": sql
    }
    try:
        response = requests.post(url, json=payload, timeout=60)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def fetch_result(session_id, user="root", password=ROOT_PASSWORD):
    """Fetch query results by session ID."""
    url = f"{SCDB_URL}/public/fetch_result"
    payload = {
        "user": {"user": {"account_system_type": "NATIVE_USER", "native_user": {"name": user, "password": password}}},
        "session_id": session_id
    }
    try:
        response = requests.post(url, json=payload, timeout=60)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def setup_parties():
    """Set up participants (alice and bob)."""
    print("=== Setting up participants ===")

    alice_pw = os.environ.get("SCQL_ALICE_PASSWORD", "alice123")
    bob_pw = os.environ.get("SCQL_BOB_PASSWORD", "bob123")

    result = execute_sql(f"CREATE USER alice IDENTIFIED BY '{alice_pw}'")
    print(f"Create Alice user: {result}")

    result = execute_sql(f"CREATE USER bob IDENTIFIED BY '{bob_pw}'")
    print(f"Create Bob user: {result}")

    result = execute_sql("CREATE DATABASE IF NOT EXISTS hive_test")
    print(f"Create database: {result}")

def setup_tables():
    """Register table metadata in SCQL."""
    print("\n=== Setting up table metadata ===")

    result = execute_sql("""
        CREATE TABLE hive_test.user_credit (
            ID STRING,
            credit_rank INT,
            income INT,
            age INT
        ) REF_TABLE=user_credit DB_TYPE='hive' OWNER='alice' PARTY='alice'
    """)
    print(f"Create Alice table: {result}")

    result = execute_sql("""
        CREATE TABLE hive_test.user_stats (
            ID STRING,
            order_amount INT,
            is_active INT
        ) REF_TABLE=user_stats DB_TYPE='hive' OWNER='bob' PARTY='bob'
    """)
    print(f"Create Bob table: {result}")

def grant_permissions():
    """Grant cross-party permissions."""
    print("\n=== Granting permissions ===")

    alice_pw = os.environ.get("SCQL_ALICE_PASSWORD", "alice123")
    bob_pw = os.environ.get("SCQL_BOB_PASSWORD", "bob123")

    result = execute_sql("GRANT SELECT ON hive_test.user_credit TO bob", user="alice", password=alice_pw)
    print(f"Alice grants to Bob: {result}")

    result = execute_sql("GRANT SELECT ON hive_test.user_stats TO alice", user="bob", password=bob_pw)
    print(f"Bob grants to Alice: {result}")

def run_federated_query():
    """Run a cross-party federated query."""
    print("\n=== Running federated query ===")

    query = """
        SELECT
            a.ID,
            a.credit_rank,
            a.income,
            b.order_amount,
            b.is_active
        FROM hive_test.user_credit a
        JOIN hive_test.user_stats b ON a.ID = b.ID
        WHERE a.age >= 20 AND b.is_active = 1
        LIMIT 10
    """

    print(f"Query: {query}")
    result = execute_sql(query)
    print(f"Submit result: {json.dumps(result, indent=2)}")

    if "session_id" in result:
        print("\nWaiting for query results...")
        time.sleep(5)

        fetch = fetch_result(result["session_id"])
        print(f"Query result: {json.dumps(fetch, indent=2)}")

def test_basic_connectivity():
    """Test SCDB server connectivity."""
    print("=== Testing SCDB connectivity ===")
    try:
        response = requests.get(f"{SCDB_URL}/public/submit_query", timeout=5)
        print(f"SCDB server status: running (HTTP {response.status_code})")
        return True
    except Exception as e:
        print(f"SCDB server connection failed: {e}")
        return False

def main():
    if not ROOT_PASSWORD:
        print("ERROR: Set SCQL_ROOT_PASSWORD environment variable before running.")
        print("  export SCQL_ROOT_PASSWORD='your_scdb_root_password'")
        return

    print("=" * 60)
    print("SCQL Federated Query Test - Hive Backend")
    print("=" * 60)

    if not test_basic_connectivity():
        print("Please start the SCDB server first.")
        return

    print("\n=== Testing SCDB API ===")
    result = execute_sql("SHOW DATABASES")
    print(f"SHOW DATABASES: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    main()
