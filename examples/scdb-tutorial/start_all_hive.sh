#!/bin/bash
# SCQL 本地启动脚本 - Hive 后端版本
# 使用 Arrow Flight SQL 服务器模拟 Hive 数据源

PROJECT_ROOT="/root/autodl-tmp/scql"
TUTORIAL_DIR="/root/autodl-tmp/scql/examples/scdb-tutorial"
HIVE_DIR="$TUTORIAL_DIR/hive"
BIN_DIR="$PROJECT_ROOT/bin"

echo "========================================="
echo "启动 SCQL 服务 (Hive 后端)"
echo "========================================="

# 检查二进制文件
if [ ! -f "$BIN_DIR/scqlengine" ] || [ ! -f "$BIN_DIR/scdbserver" ]; then
    echo "错误: 找不到编译好的二进制文件"
    echo "请先运行: make binary"
    exit 1
fi

# 检查 Python 依赖
if ! python3 -c "import pyarrow.flight, duckdb" 2>/dev/null; then
    echo "警告: 缺少 Python 依赖，请安装:"
    echo "  pip install pyarrow duckdb"
    echo ""
fi

# 创建日志目录
mkdir -p "$TUTORIAL_DIR/logs"

# 停止可能已运行的 Arrow Flight 服务器
pkill -f "arrow_flight_server.py" 2>/dev/null || true
sleep 1

# 启动 Arrow Flight SQL 服务器 (模拟 Hive)
echo "启动 Arrow Flight SQL 服务器 (模拟 Hive)..."

echo "  启动 Alice Arrow Flight (端口 8815)..."
nohup python3 "$HIVE_DIR/arrow_flight_server.py" --party alice --port 8815 \
    > "$TUTORIAL_DIR/logs/alice_flight.log" 2>&1 &
ALICE_FLIGHT_PID=$!

echo "  启动 Bob Arrow Flight (端口 8816)..."
nohup python3 "$HIVE_DIR/arrow_flight_server.py" --party bob --port 8816 \
    > "$TUTORIAL_DIR/logs/bob_flight.log" 2>&1 &
BOB_FLIGHT_PID=$!

# 等待 Arrow Flight 服务器启动
sleep 2

# 禁用代理（避免 gRPC 连接被代理拦截）
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

# 启动 Alice Engine (使用 Hive 配置)
echo "启动 Alice Engine (端口 8003, Hive 后端)..."
nohup "$BIN_DIR/scqlengine" \
    --flagfile="$TUTORIAL_DIR/engine/alice/conf/gflags_hive.conf" \
    > "$TUTORIAL_DIR/logs/alice_engine.log" 2>&1 &
ALICE_PID=$!
echo "Alice Engine PID: $ALICE_PID"

# 启动 Bob Engine (使用 Hive 配置)
echo "启动 Bob Engine (端口 8004, Hive 后端)..."
nohup "$BIN_DIR/scqlengine" \
    --flagfile="$TUTORIAL_DIR/engine/bob/conf/gflags_hive.conf" \
    > "$TUTORIAL_DIR/logs/bob_engine.log" 2>&1 &
BOB_PID=$!
echo "Bob Engine PID: $BOB_PID"

# 等待 Engines 启动
sleep 2

# 删除旧的 SQLite 数据库（如果存在），以便重新初始化
rm -f "$TUTORIAL_DIR/scdb/scdb_hive.db"

# 启动 SCDB Server（设置 root 密码为 "root"）
echo "启动 SCDB Server (端口 8080)..."
export SCQL_ROOT_PASSWORD="root"
nohup "$BIN_DIR/scdbserver" \
    -config="$TUTORIAL_DIR/scdb/conf/config_hive.yml" \
    > "$TUTORIAL_DIR/logs/scdb_server.log" 2>&1 &
SCDB_PID=$!
echo "SCDB Server PID: $SCDB_PID"

echo ""
echo "========================================="
echo "所有服务已启动 (Hive 后端)！"
echo "========================================="
echo ""
echo "Arrow Flight SQL (模拟 Hive):"
echo "  Alice: grpc://localhost:8815 (PID: $ALICE_FLIGHT_PID)"
echo "  Bob:   grpc://localhost:8816 (PID: $BOB_FLIGHT_PID)"
echo ""
echo "SCQL 服务:"
echo "  Alice Engine:  http://localhost:8003 (PID: $ALICE_PID)"
echo "  Bob Engine:    http://localhost:8004 (PID: $BOB_PID)"
echo "  SCDB Server:   http://localhost:8080 (PID: $SCDB_PID)"
echo ""
echo "日志文件位置: $TUTORIAL_DIR/logs/"
echo "  - alice_flight.log  (Arrow Flight)"
echo "  - bob_flight.log    (Arrow Flight)"
echo "  - alice_engine.log  (SCQL Engine)"
echo "  - bob_engine.log    (SCQL Engine)"
echo "  - scdb_server.log   (SCDB Server)"
echo ""
echo "停止服务: bash $TUTORIAL_DIR/stop_all_hive.sh"
echo ""
echo "运行测试: bash $PROJECT_ROOT/test_privacy_hive.sh"

