#!/bin/bash
# 通用合约编译脚本
# 使用方式: ./compile_sol.sh <合约源文件路径>

set -e

SOLC_VERSION="0.6.12"

print_usage() {
    echo "用法: $0 <合约源文件路径>"
    echo "示例:"
    echo "  $0 ./contracts/SmallBank/small_bank.sol"
    echo "  $0 /path/to/your/contract.sol"
}

if [[ $# -ne 1 ]]; then
    print_usage
    exit 1
fi

source_file="$1"

if [[ ! -f "$source_file" ]]; then
    echo "错误: 文件不存在 - $source_file"
    exit 1
fi

contract_dir=$(dirname "$source_file")
source_name=$(basename "$source_file")

cd "$contract_dir"

existing_files=""
for f in *.abi *.bin; do
    if [[ -f "$f" ]]; then
        existing_files="$existing_files $f"
    fi
done

echo "正在编译合约: $source_file"
echo "输出目录: $contract_dir"

docker run --rm -v "$(pwd):/sources" ethereum/solc:"$SOLC_VERSION" --optimize --abi --bin "/sources/$source_name" -o /sources --overwrite

# 生成 storage layout JSON
docker run --rm -v "$(pwd):/sources" ethereum/solc:"$SOLC_VERSION" --optimize --combined-json storage-layout "/sources/$source_name" > .storage_combined.json 2>/dev/null

# 从 combined JSON 中提取每个合约的 storage layout，保存为 <ContractName>.storage.json
python3 -c "
import json, sys, os
with open('.storage_combined.json') as f:
    data = json.load(f)
for full_name, contract_data in data.get('contracts', {}).items():
    contract_name = full_name.split(':')[-1]
    sl = contract_data.get('storage-layout')
    if sl is None:
        continue
    # storage-layout 是字符串化的 JSON，解析后再保存
    parsed = json.loads(sl) if isinstance(sl, str) else sl
    if not parsed.get('storage'):
        continue  # 跳过 interface（空 storage）
    out_file = f'{contract_name}.storage.json'
    with open(out_file, 'w') as out:
        json.dump(parsed, out, indent=2)
    print(f'✓ 生成: {out_file}')
" 2>/dev/null

rm -f .storage_combined.json

for file in *.abi *.bin; do
    if [[ -f "$file" ]]; then
        exists=0
        for f in $existing_files; do
            if [[ "$f" == "$file" ]]; then
                exists=1
                break
            fi
        done
        if [[ $exists -eq 0 ]]; then
            base_name="${file%.*}"
            ext="${file##*.}"
            new_name="${base_name}.${ext}"
            mv -f "$file" "$new_name"
            echo "✓ 生成: $new_name"
        fi
    fi
done

echo ""
echo "✓ 编译完成！"
