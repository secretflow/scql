#!/bin/bash
set -euo pipefail
# 启动 Alice 和 Bob 的 Java Arrow Flight SQL 测试服务器

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/run_java_arrow_server.sh"
BACKEND="${SCQL_FLIGHT_BACKEND:-duckdb}"
HIVE_HOST="${SCQL_HIVE_HOST:-localhost}"
HIVE_PORT="${SCQL_HIVE_PORT:-10000}"
HIVE_USER="${SCQL_HIVE_USER:-}"
HIVE_PASSWORD="${SCQL_HIVE_PASSWORD:-}"
HIVE_DATABASE="${SCQL_HIVE_DATABASE:-}"
HIVE_AUTH="${SCQL_HIVE_AUTH:-NONE}"

build_backend_args() {
  BACKEND_ARGS=(--backend "$BACKEND")
  if [ "$BACKEND" = "hive" ]; then
    BACKEND_ARGS+=(--hive-host "$HIVE_HOST" --hive-port "$HIVE_PORT" --hive-auth "$HIVE_AUTH")
    if [ -n "$HIVE_DATABASE" ]; then
      BACKEND_ARGS+=(--hive-database "$HIVE_DATABASE")
    fi
    if [ -n "$HIVE_USER" ]; then
      BACKEND_ARGS+=(--hive-user "$HIVE_USER")
    fi
    if [ -n "$HIVE_PASSWORD" ]; then
      BACKEND_ARGS+=(--hive-password "$HIVE_PASSWORD")
    fi
  fi
}

echo "=== 启动 Java Arrow Flight SQL 测试服务器 ==="

# 停止之前可能运行的服务器
pkill -f "scql-hive-flight-sql-server.jar" 2>/dev/null || true
sleep 1

# 先统一构建一次，避免 Alice/Bob 并发触发 Maven 下载/打包
if [ ! -f "$SCRIPT_DIR/java-flight-sql-server/target/scql-hive-flight-sql-server.jar" ] || [ "${SCQL_FORCE_REBUILD:-0}" = "1" ]; then
  echo "预构建 Java Arrow Flight SQL server..."
  SCQL_FORCE_REBUILD="${SCQL_FORCE_REBUILD:-0}" "$RUNNER" --help >/dev/null
fi

build_backend_args

# 启动 Alice 服务器 (端口 8815)
echo "启动 Alice Arrow Flight SQL 服务器 (端口 8815, backend=$BACKEND)..."
"$RUNNER" --party alice --port 8815 --advertise-host localhost "${BACKEND_ARGS[@]}" &
ALICE_PID=$!
echo "Alice PID: $ALICE_PID"

# 启动 Bob 服务器 (端口 8816)
echo "启动 Bob Arrow Flight SQL 服务器 (端口 8816, backend=$BACKEND)..."
"$RUNNER" --party bob --port 8816 --advertise-host localhost "${BACKEND_ARGS[@]}" &
BOB_PID=$!
echo "Bob PID: $BOB_PID"

sleep 4

echo ""
echo "=== Arrow Flight SQL 服务器已启动 ==="
echo "Alice: grpc://localhost:8815"
echo "Bob:   grpc://localhost:8816"
echo ""
echo "停止服务器: pkill -f scql-hive-flight-sql-server.jar"
echo ""

wait

