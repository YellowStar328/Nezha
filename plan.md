下面是一套面向 `24,000,000–24,010,000` 主网区间、适配你当前 Nezha 框架的完整方案。

目标是：用真实 Ethereum 用户交易、真实块环境和每块最小状态，测试 Serial / CG / Nezha / NezhaVariable / Vegeta 的执行吞吐；不保存完整世界状态，也不修改现有合成 workload 路径。

该区间已在 Fusaka 后，必须使用现代执行客户端；不能继续用仓库内旧版 `levm` 作为真实回放执行器。[Ethereum forks timeline](https://ethereum.org/ethereum-forks/)

---

## 1. 实验边界

本方案测量的是：

```text
真实块内用户交易
+ 真实 EVM / 真实交易类型 / 真实区块上下文
+ Nezha 的并发调度和验证
= 每块可重复、可验证的实际执行吞吐
```

每块独立初始化：

```text
Block 24,000,000：稀疏初始状态 → 调度 → 执行 → 验证
Block 24,000,001：稀疏初始状态 → 调度 → 执行 → 验证
...
汇总所有块的交易数与耗时
```

不做的事情：

- 不导入并维护 1 万块连续的完整 Ethereum 世界状态；
- 不以主网 `stateRoot` 一致为目标；
- 不在第一阶段评估基于源码/ABI/StorageLayout 的 Depurge-LLM 路径。

这是“真实交易 EVM 执行 benchmark”，不是“完整执行客户端同步 benchmark”。

---

## 2. 总体架构

```text
┌────────────────────────┐
│ Archive Geth / RPC      │
│ blocks + traces + state │
└────────────┬───────────┘
             │ 一次性导出
┌────────────▼───────────┐
│ Replay Dataset          │
│ 每块：tx + env+witness  │
└────────────┬───────────┘
             │ 离线、可重复
┌────────────▼───────────┐
│ Nezha Adapter           │
│ RWNode / Context / DAG  │
└────────────┬───────────┘
             │ schedule plan
┌────────────▼───────────┐
│ modern-eth-replay       │
│ 新版 geth EVM 执行器    │
└────────────┬───────────┘
             │ receipt / rwset / miss
┌────────────▼───────────┐
│ 结果与正确性验证        │
└────────────────────────┘
```

---

## 3. 数据获取前提

需要一个可查询历史状态且开放 trace 的节点：

- 自建 archive Geth，推荐；
- 或提供 `debug_traceTransaction` / `debug_traceBlockByNumber` 的 archive RPC 服务；
- 普通免费 RPC 通常不够。

Geth 的 `prestateTracer` 可提取交易实际访问的账户、代码和 storage，是构建稀疏 witness 的基础。[Geth built-in tracers](https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers)

---

## 4. 数据集格式

建议目录：

```text
datasets/
  mainnet-24000000-24010000/
    manifest.json
    headers.jsonl.zst
    code/
      <codeHash>.bin.zst
    blocks/
      24000000.json.zst
      24000001.json.zst
      ...
```

`manifest.json`：

```json
{
  "formatVersion": 1,
  "chainId": 1,
  "fromBlock": 24000000,
  "toBlock": 24010000,
  "exportedAt": "2026-08-11T00:00:00Z",
  "stateAnchor": "pre-first-user-tx",
  "executionMode": "block-local-user-tx",
  "sourceClient": "archive-geth",
  "hashWindow": 256
}
```

每个块文件应有：

```json
{
  "header": {},
  "transactions": [],
  "witness": {},
  "canonical": {},
  "rwsets": []
}
```

### `header`

至少保存：

```text
number, hash, parentHash, timestamp,
beneficiary, gasLimit, baseFeePerGas,
prevRandao, withdrawalsRoot,
parentBeaconBlockRoot,
blobGasUsed, excessBlobGas,
requestsHash
```

同时保存该块前 256 个 block hash，支持合约 `BLOCKHASH`。

### `transactions`

每笔交易保存：

```text
hash, transactionIndex, type,
from, to, nonce, value, input, gas,
gasPrice,
maxFeePerGas, maxPriorityFeePerGas,
accessList,
maxFeePerBlobGas, blobVersionedHashes,
authorizationList,
v, r, s,
rawRLP
```

必须支持 legacy、access-list、EIP-1559、blob 和 EIP-7702 交易。Pectra 已在主网上启用 EIP-7702，因此不能假定全是普通合约调用。[Pectra announcement](https://blog.ethereum.org/2025/04/23/pectra-mainnet)

### `witness`

```json
{
  "accounts": {
    "0x...": {
      "balance": "0x...",
      "nonce": "0x...",
      "codeHash": "0x...",
      "storage": {
        "0xslot": "0xvalue"
      }
    }
  }
}
```

代码按 hash 去重存到 `code/`，避免在每个块里重复保存。

---

## 5. 稀疏状态构造算法

对每个区块独立执行下列流程。

### 5.1 获取交易、回执和块环境

```text
eth_getBlockByNumber(N, true)
eth_getTransactionReceipt(txHash)
```

按 `transactionIndex` 排序。

### 5.2 对每笔交易获取两份 trace

普通访问集：

```json
{
  "method": "debug_traceTransaction",
  "params": ["0xtxHash", {"tracer": "prestateTracer"}]
}
```

写集：

```json
{
  "method": "debug_traceTransaction",
  "params": [
    "0xtxHash",
    {
      "tracer": "prestateTracer",
      "tracerConfig": { "diffMode": true }
    }
  ]
}
```

### 5.3 生成 BlockWitness

按交易原顺序扫描，每个账户字段和 storage slot 只保存首次出现的 pre-state：

```text
首次看到 account.balance   → 记录
首次看到 account.nonce     → 记录
首次看到 account.code      → 记录
首次看到 address + slot    → 记录
之后出现                   → 忽略
```

这会得到“该块第一笔用户交易执行前，所有后续会访问的状态”的最小闭包。

### 5.4 构建 RWSet

RW key 不应只包含合约 slot，还必须覆盖账户状态：

```text
acct:<address>:balance
acct:<address>:nonce
acct:<address>:code
slot:<contract-address>:<storage-slot>
```

规则：

```text
ReadSet  = 普通 prestate trace 中的所有 touched key
WriteSet = diff trace 的 pre/post 中所有变动 key
```

这会捕获：

- SLOAD / SSTORE；
- ETH 转账；
- sender nonce；
- gas 费用；
- CREATE / CREATE2；
- SELFDESTRUCT；
- 内部调用涉及的账户。

---

## 6. 缺失状态的处理

这是正确性的硬约束：

```text
读取不存在的 account / code / storage
      ↓
不得返回默认空值或零值
      ↓
返回 WitnessMiss
```

处理策略：

```text
WitnessMiss
  ├─ 标记当前交易验证失败
  ├─ 该交易 abort
  ├─ 串行回退
  └─ 从 archive RPC 按需补齐状态，再重试
```

这样，若并发重排序导致合约走入 canonical 顺序下未出现的新分支，不会悄悄产生错误状态。

建议同时记录：

```text
witness_miss_count
witness_miss_tx_count
dynamic_rwset_expansion_count
```

它们本身是很有价值的实验指标。

---

## 7. 新执行器：不要修改旧 levm

当前：

- [`evm/levm/levm.go`](/Users/yellowstar/Desktop/code/Nezha/evm/levm/levm.go)
- [`evm/levm/vminterface/vmcontext.go`](/Users/yellowstar/Desktop/code/Nezha/evm/levm/vminterface/vmcontext.go)

只保留给现有合成合约 benchmark 使用。

新增独立模块：

```text
cmd/eth-dataset-exporter/
cmd/eth-replayd/
```

两者使用新版、Fusaka 兼容的 `github.com/ethereum/go-ethereum`。

不建议直接升级仓库内 `ethereum/go-ethereum/`：它被现有 `levm`、自定义 transaction 类型及旧接口深度绑定，升级会牵动整个项目。

### `eth-replayd` 的职责

```text
LoadBlock(block dataset)
  → 根据 witness 初始化独立稀疏 StateDB
  → 加载真实 BlockEnv
  → 接收某算法产生的执行顺序
  → 执行、抓取真实 RWSet、返回 receipt 和状态变化
```

建议用本地 Unix socket 或 HTTP JSON 通信；但 EVM 执行计时在 `eth-replayd` 内部记录，不把 IPC、文件加载和导出耗时混入执行 TPS。

---

## 8. 对 Nezha 的最小适配

保留原有：

```text
core/conflict_queue.go
core/classical_graph.go
core/nezha_variable.go
core/Depurge.go
graph/
```

新增：

```text
utils/replay_dataset.go
utils/replay_adapter.go
core/replay_types.go
```

在 [`core.TransactionContext`](/Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go:77) 增加：

```go
type ReplayRef struct {
    BlockNumber uint64
    TxIndex     uint
    TxHash      string
}

type TransactionContext struct {
    // 保留现有字段
    Replay *ReplayRef // synthetic 模式为 nil
}
```

`ReplayRef` 只做定位；原始交易和 witness 始终从 dataset 读取，不复制到每个 context。

新增入口：

```go
func LoadReplayBlocks(
    datasetDir string,
    from, to uint64,
) ([]ReplayBlock, error)

func BuildReplayContexts(
    block ReplayBlock,
) ([]*core.RWNode, map[string]*core.TransactionContext, error)
```

原有入口保留：

```go
txList := utils.GenerateTransactions(...)
```

新增模式：

```go
blocks := utils.LoadReplayBlocks(dataset, from, to)
for _, block := range blocks {
    rwNodes, contexts := utils.BuildReplayContexts(block)
    RunAlgorithm(rwNodes, contexts)
}
```

---

## 9. 算法接入策略

第一阶段运行：

```text
Serial
CG
Nezha
NezhaVariable
Vegeta
```

暂不将真实主网数据用于现有 LLM-Depurge 结论性实验，因为它依赖：

```text
源码 + ABI + storage layout + 可解释函数名
```

而主网绝大多数地址并没有这些信息。

如果一定要保留 Depurge：

```text
阶段 1：以真实 RWSet 作为 oracle 输入
阶段 2：仅选择 verified contracts 子集，做 LLM 预测实验
```

两者必须在论文或结果中明确分开，不能混为同一性能结论。

---

## 10. 每块实验流程

```text
加载 Block N dataset
      ↓
初始化独立稀疏 StateDB
      ↓
预执行，获得真实 RWSet
      ↓
Nezha / CG / Vegeta 等生成调度顺序
      ↓
按调度顺序由 eth-replayd 执行
      ↓
验证真实 RWSet、receipt 与写集
      ↓
abort / WitnessMiss 交易串行回退
      ↓
导出该块指标
```

每个算法、每个重复实验都重新从同一 `BlockWitness` 初始化，保证公平。

---

## 11. 正确性验证

每块至少验证：

| 项目 | 方式 |
|---|---|
| 交易是否成功/回滚 | `receipt.status` |
| 日志是否一致 | `logs`、topics、data、顺序 |
| gas 是否合理 | `gasUsed`、累计 gas |
| 写入状态 | 对最终 touched key 查询并比较 |
| 动态访问 | 真实 RWSet 与预执行 RWSet 对比 |
| 稀疏状态遗漏 | `WitnessMiss` 必须为 0，或已 abort 后安全回退 |
| 账户语义 | nonce、balance、code 作为显式 RW key |

对于完全串行 canonical 重放：

```text
本地 receipt / logs / gasUsed
== archive 节点的原始 receipt
```

这是 dataset 和现代执行器的验收条件。先随机抽 100 个块通过，再导出全区间。

---

## 12. 指标定义

建议分别报告，不要只给一个 TPS：

```text
T_export       数据集一次性导出时间，不纳入实验 TPS
T_load         witness 加载时间
T_preexec      预执行 / RWSet 捕获时间
T_schedule     图构建、排序、调度时间
T_execute      EVM 执行时间
T_validate     验证与 abort 检测时间
T_commit       状态合并时间
T_total        T_load + T_preexec + T_schedule + T_execute + T_validate + T_commit
```

核心指标：

```text
TPS_execute = 总交易数 / ΣT_execute
TPS_end_to_end = 总交易数 / ΣT_total
AbortRate
WitnessMissRate
RWSetExpansionRate
平均每块交易数
P50 / P95 block latency
```

---

## 13. 实施顺序

1. 先完成 `eth-dataset-exporter`，只导出 10–100 个块。
2. 用 Serial 模式验证本地结果与原始 receipts 完全一致。
3. 完成 `eth-replayd` 的稀疏状态加载与 `WitnessMiss`。
4. 将 trace RWSet 转为现有 `RWNode`。
5. 首先接 Serial、CG、Nezha。
6. 接 NezhaVariable、Vegeta 的重执行验证。
7. 以 100 块为 chunk，支持 checkpoint/resume，完成 1 万块导出。
8. 对每种算法每块至少重复 3 次，固定 CPU 核数与运行环境。

最终运行形态：

```bash
# 一次性生成数据集
eth-dataset-exporter \
  --rpc "$ARCHIVE_GETH_RPC" \
  --from 24000000 \
  --to 24010000 \
  --out ./datasets/mainnet-24000000-24010000

# 重复实验
go run test.go \
  -dataset ./datasets/mainnet-24000000-24010000 \
  -from 24000000 \
  -to 24010000 \
  -Nezha=true \
  -NezhaVariable=true \
  -Vegeta=true \
  -CG=true
```

这套方案的核心取舍是：以“每块可验证稀疏状态”换取可重复、可控存储和对现有 Nezha 算法代码的最小侵入；同时通过 `WitnessMiss → abort/回退` 保证不会因状态不足而静默执行错误。