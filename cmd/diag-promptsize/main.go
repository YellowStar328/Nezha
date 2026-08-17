package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Exactly mirror the real buildMainnetPrompt concatenation path as-is
func readFile(base, name string) string {
	b, err := os.ReadFile(base + "/" + name)
	if err != nil {
		return ""
	}
	return string(b)
}

func truncateStr(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func main() {
	addrs := []string{
		"0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT 14KB (known OK in diag with EN prompt)
		"0x0000000071727de22e5e9d8baf0edac6f37da032", // Seaport 31KB (recently 400 HTML)
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // UniswapV2 34KB
		"0x111111125421ca6dc452d289314280a0f8842a65", // 1inch 253KB (capped 60KB)
	}
	for _, addr := range addrs {
		base := "cache/mainnet_rw/" + addr
		source := truncateStr(readFile(base, "source.sol"), 60000)
		_ = readFile(base, "funcs.json")
		abi := readFile(base, "abi.json")
		storage := readFile(base, "storage.json")
		meta := readFile(base, "meta.json")

		// use the real Chinese prompt (identical to actual buildMainnetPrompt)
		// mirroring *real* availableContracts size (3KB cap now in place)
		availableContracts := truncateStr(`[20 contracts; each with ~40 funcs + full signatures.
- 0x111111125421ca6dc452d289314280a0f8842a65 (alias=1inchAggregationRouterV6): unoswap(uint256,bytes), swap(address,(address,address,address,uint256,uint256,bytes),(uint256,uint256,uint256,uint256),bytes), uniswapV3Swap(uint256,uint256[]), ... 40 more lines...
- 0x7a250d5630b4cf539739df2c5dacb4c659f2488d (alias=UniswapV2Router02): swapExactTokensForTokens(uint256,uint256,address[],address,uint256), swapTokensForExactTokens(uint256,uint256,address[],address,uint256), addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256), removeLiquidity(address,address,uint256,uint256,uint256,address,uint256), ... 20 more...
- 0xdac17f958d2ee523a2206206994597c13d831ec7 (alias=TetherToken/USDT): transfer(address,uint256), transferFrom(address,address,uint256), approve(address,uint256), allowance(address,address), balanceOf(address), totalSupply(), ... 30 more functions]`,
			3000)
		fieldOptions := truncateStr(storage, 10000)
		globalVars := truncateStr(meta, 2000) + truncateStr(abi, 8000)
		prompt := fmt.Sprintf(`分析以下以太坊主网合约 (地址: %s) 中函数 "transfer" (selector: 0xa9059cbb) 的保守读写集。直接返回JSON格式，不要任何分析文字。

合约代码：
%s

参数映射规则：
- transfer: _to=addr1, _value=addr2（占位）

字段选项 (from storage layout):
%s

全局变量列表：
%s

可用合约与函数列表 (用于识别跨合约调用)：
%s

重要规则：
1. 直接访问全局变量: {"account": "global", "field": "owner"}
2. 用函数参数作为键访问mapping: {"account": "addr1", "field": "balances"}
3. account可以是：addr1、addr2（函数参数）、global（直接访问全局变量）、或某个全局变量名（用该变量的值作为键）
4. 保守规则：对于if/else等条件语句，必须包含两个分支中所有可能的存储访问
5. 全局变量读写一致性：任何出现在writes中的全局变量，必须同时出现在reads中
6. 跨合约调用：如果合约代码中调用了上述"可用合约与函数列表"中的其他合约函数，必须在 crossCalls 数组中列出。crossCall 的 contract 字段可用合约地址（0x开头）或合约别名（如 USDT，见可用合约列表中的 alias 标注），function 字段用 selector (0x开头)

返回示例：
{"reads":[{"account":"addr1","field":"balances"},{"account":"addr2","field":"balances"}],"writes":[{"account":"addr1","field":"balances"},{"account":"addr2","field":"balances"}],"crossCalls":[{"contract":"0xdac17f958d2ee523a2206206994597c13d831ec7","function":"0xa9059cbb"}]}`,
			addr, source, fieldOptions, globalVars, availableContracts)

		body := map[string]interface{}{
			"model":       "DeepSeek-V4-Flash",
			"messages":    []map[string]string{{"role": "user", "content": prompt}},
			"temperature": 0.0,
			"max_tokens":  5000,
		}
		rb, _ := json.Marshal(body)
		// 中文→JSON转义开销
		chinesePct := 0
		totalRunes := 0
		for _, r := range prompt {
			totalRunes++
			if r > 127 {
				chinesePct++
			}
		}
		fmt.Printf("addr=%s source=%dB promptRunes=%d (nonASCII=%d%%) promptBytes=%d JSONbody=%dB\n",
			addr, len(source), totalRunes, (100*chinesePct)/max(1, totalRunes), len(prompt), len(rb))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
