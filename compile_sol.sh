#!/bin/bash
# 编译 SmallBank 合约的脚本

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTRACT_DIR="$SCRIPT_DIR/SmallBank"
SOLC_VERSION="0.6.12"

echo "正在编译 SmallBank 合约..."
echo "合约目录: $CONTRACT_DIR"

cd "$CONTRACT_DIR"
docker run --rm -v $(pwd):/sources ethereum/solc:$SOLC_VERSION --optimize --abi --bin /sources/small_bank.sol -o /sources --overwrite

# 重命名生成的文件，确保文件名符合 Go 代码的引用
mv -f SmallBank.abi small_bank_sol_SmallBank.abi 2>/dev/null || true
mv -f SmallBank.bin small_bank_sol_SmallBank.bin 2>/dev/null || true

echo "✓ 编译成功！"
echo "文件更新:"
echo "  - small_bank_sol_SmallBank.abi"
echo "  - small_bank_sol_SmallBank.bin"
