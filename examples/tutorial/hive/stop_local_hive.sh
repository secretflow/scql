#!/bin/bash
# 停止本地 Hive 服务

echo "停止 Hive 服务..."

# 停止 HiveServer2
pkill -f "HiveServer2" && echo "✓ Stopped HiveServer2" || echo "HiveServer2 未运行"

# 停止 Metastore
pkill -f "HiveMetaStore" && echo "✓ Stopped Hive Metastore" || echo "Hive Metastore 未运行"

# 清理 PID 文件
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -f "$SCRIPT_DIR/logs/metastore.pid" "$SCRIPT_DIR/logs/hiveserver2.pid" 2>/dev/null

echo ""
echo "Hive 服务已停止"
