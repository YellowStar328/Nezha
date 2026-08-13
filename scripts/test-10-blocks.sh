#!/bin/bash
set -euo pipefail

# ====================================================================
# 10 块小规模测试脚本
#
# 前提条件：
#   - Alchemy PAYG 账户（Debug API 在 Free Tier 不可用）
#   - 绑卡升级到 Pay As You Go：https://dashboard.alchemy.com/signup
#
# 预估成本（10 块）：
#   - debug_traceTransaction × 2（prestate + diffMode）× ~150 tx/块
#   - ≈ 3000 次调用 × 40 CU = 120,000 CU
#   - 120,000 CU × $0.45/M = ~$0.05（约 5 美分）
# ====================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BLOCKS="${BLOCKS:-10}"
FROM_BLOCK="${FROM_BLOCK:-24000000}"
RPC_URL="${RPC_URL:-https://eth-mainnet.g.alchemy.com/v2/}"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/datasets/test-${FROM_BLOCK}-$(($FROM_BLOCK + BLOCKS - 1))}"

if [ -z "${ALCHEMY_API_KEY:-}" ]; then
    echo "ERROR: ALCHEMY_API_KEY 环境变量未设置"
    echo ""
    echo "获取 PAYG API Key："
    echo "  1. 注册/登录 https://dashboard.alchemy.com"
    echo "  2. 绑卡升级到 Pay As You Go（绑卡即用，无月费）"
    echo "  3. 创建 App，复制 API Key"
    echo "  4. export ALCHEMY_API_KEY=your-payg-key"
    exit 1
fi

TO_BLOCK=$(($FROM_BLOCK + BLOCKS - 1))

echo "=== Ethereum 10 块数据集导出与重放测试 ==="
echo "区块范围: $FROM_BLOCK - $TO_BLOCK ($BLOCKS 块)"
echo "输出目录: $OUT_DIR"
echo "预估成本: ~\$0.05（PAYG Debug API）"
echo ""

mkdir -p "$OUT_DIR"

echo "=== Step 1: 从 Alchemy 导出 $BLOCKS 个区块 ==="
cd "$PROJECT_ROOT/cmd/eth-dataset-exporter"
go run . \
    --rpc "$RPC_URL" \
    --rpc-key "$ALCHEMY_API_KEY" \
    --from "$FROM_BLOCK" \
    --to "$TO_BLOCK" \
    --out "$OUT_DIR"

echo ""
echo "=== Step 2: 本地重放并验证交易 ==="
cd "$PROJECT_ROOT/cmd/eth-replayd"
go run . \
    --dataset "$OUT_DIR"

echo ""
echo "=== 测试完成 ==="
echo "数据集已保存到: $OUT_DIR"
echo "可重复运行重放: go run ./cmd/eth-replayd --dataset $OUT_DIR"
