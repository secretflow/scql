#!/bin/bash
# 在本地 Hive 中初始化 SCQL 测试数据
# 复用 initdb/ 目录下的 HQL 文件

HIVE_HOME=${HIVE_HOME:-/opt/hive}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INITDB_DIR="$SCRIPT_DIR/initdb"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "=========================================="
echo "  初始化 Hive 测试数据"
echo "=========================================="
echo ""

# 检查 HiveServer2 是否运行
if ! nc -z localhost 10000 2>/dev/null; then
    log_error "HiveServer2 未运行 (端口 10000)"
    echo "请先运行: bash $SCRIPT_DIR/start_local_hive.sh"
    exit 1
fi

# 检查 initdb 文件
if [ ! -f "$INITDB_DIR/alice_init.hql" ] || [ ! -f "$INITDB_DIR/bob_init.hql" ]; then
    log_error "找不到初始化文件: $INITDB_DIR/alice_init.hql 或 bob_init.hql"
    exit 1
fi

BEELINE="$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"

# 初始化 Alice 数据
log_info "初始化 Alice 数据 (alice_init.hql)..."
echo "  表: alice.user_credit"
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000' --silent=true -f "$INITDB_DIR/alice_init.hql"

if [ $? -eq 0 ]; then
    log_success "✓ Alice 数据初始化成功"
else
    log_error "✗ Alice 数据初始化失败"
    exit 1
fi

# 初始化 Bob 数据
log_info "初始化 Bob 数据 (bob_init.hql)..."
echo "  表: bob.user_stats"
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000' --silent=true -f "$INITDB_DIR/bob_init.hql"

if [ $? -eq 0 ]; then
    log_success "✓ Bob 数据初始化成功"
else
    log_error "✗ Bob 数据初始化失败"
    exit 1
fi

echo ""
log_info "验证数据..."
echo ""

echo "Alice - user_credit 表:"
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000/alice' --silent=true -e "SELECT COUNT(*) as count FROM user_credit;"

echo ""
echo "Bob - user_stats 表:"
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000/bob' --silent=true -e "SELECT COUNT(*) as count FROM user_stats;"

echo ""
echo "=========================================="
echo "  测试数据初始化完成"
echo "=========================================="
echo ""
echo "已创建的表:"
echo "  - alice.user_credit (ID, credit_rank, income, age) - 19 行"
echo "  - bob.user_stats (ID, order_amount, is_active) - 19 行"
echo ""
echo "下一步:"
echo "  bash $SCRIPT_DIR/start_scql_with_real_hive.sh"
