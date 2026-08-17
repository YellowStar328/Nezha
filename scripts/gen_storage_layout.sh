#!/bin/bash
# 为 cache/mainnet_rw/<addr>/source.sol 生成 storage.json
# 策略：修改 pragma 到可用 solc 版本（storage layout 与编译器版本无关）
set -eu

BASE_DIR="./cache/mainnet_rw"

gen_for_contract() {
    local addr="$1"
    local d="$BASE_DIR/$addr"
    local src="$d/source.sol"
    local storage="$d/storage.json"

    if [ ! -f "$src" ]; then
        return 0
    fi
    if [ -f "$storage" ]; then
        echo "✓ $addr: 已存在"
        return 0
    fi

    # 复制到临时文件并修改 pragma
    local tmp="$d/source_tmp.sol"
    cp "$src" "$tmp"

    # 将所有 pragma 改为 0.8.25（支持 storage-layout）
    # 0.4.x/0.6.x 的语法大多兼容 0.8.x（storage layout 不变）
    sed -i.bak 's/pragma solidity[^;]*/pragma solidity ^0.8.25/' "$tmp" 2>/dev/null || \
        sed -i '' 's/pragma solidity[^;]*/pragma solidity ^0.8.25/' "$tmp"

    # 编译生成 storage layout
    local json_out
    json_out=$(docker run --rm \
        -v "$(pwd)/$d:/sources" \
        --platform linux/amd64 \
        ethereum/solc:0.8.25 \
        --combined-json storage-layout \
        --allow-paths /sources \
        "/sources/source_tmp.sol" 2>/dev/null || true)

    if [ -z "$json_out" ]; then
        # 0.8.25 失败，尝试 0.6.12（对 0.4.x/0.6.x 兼容性更好）
        sed -i.bak 's/pragma solidity[^;]*/pragma solidity ^0.6.12/' "$tmp" 2>/dev/null || \
            sed -i '' 's/pragma solidity[^;]*/pragma solidity ^0.6.12/' "$tmp"
        json_out=$(docker run --rm \
            -v "$(pwd)/$d:/sources" \
            --platform linux/amd64 \
            ethereum/solc:0.6.12 \
            --combined-json storage-layout \
            --allow-paths /sources \
            "/sources/source_tmp.sol" 2>/dev/null || true)
    fi

    rm -f "$tmp" "$tmp.bak"

    if [ -z "$json_out" ]; then
        echo "✗ $addr: 编译失败"
        return 0
    fi

    # 提取 storage layout，写入合约目录
    export STORAGE_PATH="$d/storage.json"
    echo "$json_out" | python3 -c "
import json, sys, os
try:
    data = json.load(sys.stdin)
except:
    print('  ✗ JSON 解析失败')
    sys.exit(0)
out_path = os.environ['STORAGE_PATH']
for full_name, contract_data in data.get('contracts', {}).items():
    contract_name = full_name.split(':')[-1]
    sl = contract_data.get('storage-layout')
    if sl is None:
        continue
    parsed = json.loads(sl) if isinstance(sl, str) else sl
    if not parsed.get('storage'):
        continue
    with open(out_path, 'w') as out:
        json.dump(parsed, out, indent=2)
    print(f'  ✓ {contract_name}: {len(parsed[\"storage\"])} fields')
    break
" 2>/dev/null

    if [ ! -f "$d/storage.json" ]; then
        echo "✗ $addr: 未提取到 storage layout"
    fi
}

# 切换到项目根目录
cd /Users/yellowstar/Desktop/code/Nezha

for d in "$BASE_DIR"/0x*/; do
    addr=$(basename "$d")
    [ -f "$d/source.sol" ] || continue
    [ -f "$d/storage.json" ] && continue
    echo "=== $addr ==="
    gen_for_contract "$addr"
done

echo ""
echo "=== 最终结果 ==="
for d in "$BASE_DIR"/0x*/; do
    addr=$(basename "$d")
    if [ -f "$d/storage.json" ]; then
        fields=$(python3 -c "import json; print(len(json.load(open('$d/storage.json'))['storage']))" 2>/dev/null || echo "?")
        echo "  ✓ $addr: $fields fields"
    elif [ -f "$d/source.sol" ]; then
        echo "  ✗ $addr: 有源码但无 storage.json"
    else
        echo "  - $addr: 无源码"
    fi
done
