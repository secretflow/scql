#!/bin/bash
# 启动本地 Hive 服务脚本
# 包括 Hive Metastore 和 HiveServer2

set -e

HIVE_HOME=${HIVE_HOME:-/opt/hive}
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

echo "=========================================="
echo "  启动本地 Hive 服务"
echo "=========================================="
echo ""

# 检查 Hive 安装
if [ ! -f "$HIVE_HOME/bin/hive" ]; then
    log_error "找不到 Hive: $HIVE_HOME/bin/hive"
    exit 1
fi

# 创建日志目录
mkdir -p "$LOG_DIR"

# 停止可能正在运行的服务
log_info "清理旧服务..."
pkill -f "HiveServer2" 2>/dev/null || true
pkill -f "HiveMetaStore" 2>/dev/null || true
sleep 2

# 清理 Derby 锁文件
rm -f /tmp/hive/metastore_db/*.lck 2>/dev/null || true

# 检查是否需要初始化 schema
if [ ! -d "/tmp/hive/metastore_db" ]; then
    log_info "首次启动，初始化 Metastore schema..."
    $HIVE_HOME/bin/schematool -dbType derby -initSchema 2>/dev/null || true
fi

# 启动 Hive Metastore (监听 9083 端口)
log_info "启动 Hive Metastore (端口 9083)..."
nohup $HIVE_HOME/bin/hive --service metastore \
    > "$LOG_DIR/hive_metastore.log" 2>&1 &
METASTORE_PID=$!
echo $METASTORE_PID > "$LOG_DIR/metastore.pid"
log_success "Hive Metastore 启动 (PID: $METASTORE_PID)"

# 等待 Metastore 就绪
log_info "等待 Metastore 就绪..."
for i in {1..60}; do
    if nc -z localhost 9083 2>/dev/null; then
        log_success "Metastore 已就绪 (端口 9083)"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

if ! nc -z localhost 9083 2>/dev/null; then
    log_error "Metastore 启动超时"
    cat "$LOG_DIR/hive_metastore.log"
    exit 1
fi

# 等待额外时间确保 Metastore 完全初始化
sleep 5

# 启动 HiveServer2 (监听 10000 端口)
log_info "启动 HiveServer2 (端口 10000)..."
nohup $HIVE_HOME/bin/hiveserver2 \
    > "$LOG_DIR/hiveserver2.log" 2>&1 &
HS2_PID=$!
echo $HS2_PID > "$LOG_DIR/hiveserver2.pid"
log_success "HiveServer2 启动 (PID: $HS2_PID)"

# 等待 HiveServer2 就绪
log_info "等待 HiveServer2 就绪 (可能需要 1-2 分钟)..."
for i in {1..120}; do
    if nc -z localhost 10000 2>/dev/null; then
        log_success "HiveServer2 已就绪 (端口 10000)"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# 验证服务状态
echo ""
log_info "检查服务状态..."

if nc -z localhost 9083 2>/dev/null; then
    log_success "✓ Hive Metastore 运行在端口 9083"
else
    log_error "✗ Hive Metastore 未响应"
fi

if nc -z localhost 10000 2>/dev/null; then
    log_success "✓ HiveServer2 运行在端口 10000"
else
    log_warning "✗ HiveServer2 未响应 (端口 10000)"
    log_info "检查日志: $LOG_DIR/hiveserver2.log"
fi

echo ""
echo "=========================================="
echo "  Hive 服务启动完成"
echo "=========================================="
echo ""
echo "连接信息:"
echo "  Metastore:    thrift://localhost:9083"
echo "  HiveServer2:  jdbc:hive2://localhost:10000"
echo "  用户: $(whoami)"
echo ""
echo "日志文件:"
echo "  $LOG_DIR/hive_metastore.log"
echo "  $LOG_DIR/hiveserver2.log"
echo ""
echo "测试连接:"
echo "  $HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000/default' -e 'show databases;'"
echo ""
echo "初始化测试数据:"
echo "  bash $SCRIPT_DIR/init_hive_test_data.sh"
echo ""
echo "停止服务:"
echo "  bash $SCRIPT_DIR/stop_local_hive.sh"
