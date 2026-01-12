#!/bin/bash
# SCQL 停止脚本 - Hive 后端版本

echo "停止 SCQL 服务 (Hive 后端)..."

# 停止 Arrow Flight SQL 服务器
pkill -f "arrow_flight_server.py.*alice" && echo "✓ Stopped Alice Arrow Flight"
pkill -f "arrow_flight_server.py.*bob" && echo "✓ Stopped Bob Arrow Flight"

# 停止 SCQL 服务
pkill -f "scqlengine.*alice" && echo "✓ Stopped Alice Engine"
pkill -f "scqlengine.*bob" && echo "✓ Stopped Bob Engine"
pkill -f "scdbserver" && echo "✓ Stopped SCDB Server"

echo ""
echo "所有服务已停止 (Hive 后端)"

