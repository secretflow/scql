#!/bin/bash
# 启动 Alice 和 Bob 的 Arrow Flight SQL 测试服务器

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== 启动 Arrow Flight SQL 测试服务器 ==="

# 停止之前可能运行的服务器
pkill -f "arrow_flight_server.py" 2>/dev/null
sleep 1

# 启动 Alice 服务器 (端口 8815)
echo "启动 Alice Arrow Flight SQL 服务器 (端口 8815)..."
python3 "$SCRIPT_DIR/arrow_flight_server.py" --party alice --port 8815 &
ALICE_PID=$!
echo "Alice PID: $ALICE_PID"

# 启动 Bob 服务器 (端口 8816)
echo "启动 Bob Arrow Flight SQL 服务器 (端口 8816)..."
python3 "$SCRIPT_DIR/arrow_flight_server.py" --party bob --port 8816 &
BOB_PID=$!
echo "Bob PID: $BOB_PID"

sleep 2

echo ""
echo "=== Arrow Flight SQL 服务器已启动 ==="
echo "Alice: grpc://localhost:8815"
echo "Bob:   grpc://localhost:8816"
echo ""
echo "停止服务器: pkill -f arrow_flight_server.py"
echo ""

# 等待任意进程结束
wait

