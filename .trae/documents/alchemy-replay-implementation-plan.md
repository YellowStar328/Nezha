# 基于 Alchemy PAYG 的主网真实交易回放实施计划

## 概要

在 [mainnet-replay-feasibility-plan.md](file:///Users/yellowstar/Desktop/code/Nezha/.trae/documents/mainnet-replay-feasibility-plan.md) 可行性评估基础上，用户已确认使用 **Alchemy PAYG**（$5 最低档起步）作为 Archive/Trace 数据源。本文档将可行性评估转化为**精确到具体文件/函数/数据结构的代码修改实施计划**，并更新已解阻塞的决策与成本分阶段方案。

**成本策略**：分三档付费，避免一次性 $77：
- 阶段 1–2：$5（~600–2,000 块余量，导 10–100 块）
- 阶段 3–5：验证通过后追加预算，导 1,000 块
- 阶段 6–8：最后追加预算，导 10,000 块全区间

---

## 现状分析（代码确认）

| 项 | 实际状态 |
|---|---|
| Go 版本 | `go 1.18`（[go.mod](file:///Users/yellowstar/Desktop/code/Nezha/go.mod#L1-L58)），新模块需独立 go.mod |
| vendored geth | `Nezha/ethereum/go-ethereum`（~v1.9 pre-London），[levm.go](file:///Users/yellowstar/Desktop/code/Nezha/evm/levm/levm.go#L18-L66) 用 `params.AllEthashProtocolChanges`，无 BaseFee/Random |
| RWNode 键模型 | `RWSet{Key,Value []byte}` + `CompositeKey() = ContractName:hex(Key)`（[rwset_builder.go](file:///Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go#L11-L67)）。Replay 下 ContractName 设为 `""`，Key 用字符串格式 `acct:<addr>:balance`/`slot:<addr>:<slot>`，需加新方法避免 `":` 前缀 |
| TransactionContext | 现有字段偏合成（ContractName/Function/Addr1/Addr2/LLM*）（[rwset_builder.go](file:///Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go#L75-L90)）。加 `Replay *ReplayRef` 旁路，其余字段零值 |
| 算法入口 | `TestSerialExecution`/`TestConflictQueue`/`TestConflictGraph`/`TestDepurge`/`TestNezhaVariable`/`TestVegeta`（[test.go](file:///Users/yellowstar/Desktop/code/Nezha/test.go#L293-L1297)），签名均为 `(txList []utils.Transaction, writer *bufio.Writer, dbFile string)`，需新增 replay 版重载 |
| 重执行函数 | `utils.ReExecuteAndGetRealRWSet(ctx, dbFile, logicalState)`（[data.go](file:///Users/yellowstar/Desktop/code/Nezha/utils/data.go#L938-L1007)）走 levm。需引入 `ReplayExecutor` 接口旁路 |
| cmd/ 目录 | 不存在 |

---

## 架构决策更新

### 决策 0（已验证可行）：Alchemy PAYG 成本分段

| 阶段 | 导出范围 | Alchemy 预算 | 说明 |
|---|---|---|---|
| 1–2 | 10 块（24,000,000–24,000,009） | $5 最低档 | 验证 exporter + 串行正确性 |
| 3–5 | 1,000 块 | 加 $10–15 | 接 Serial/CG/Nezha，验证算法行为 |
| 6–8 | 10,000 块 | 加 $60–80 | 全区间 NezhaVariable/Vegeta，3 次重复 |

### 决策 1：go.work 多模块（不变）

```
go.work：include ./  ./cmd/eth-dataset-exporter  ./cmd/eth-replayd
```

三个模块完全隔离：
- 主模块 `Nezha`（go 1.18）：现有代码完全不动 levm/旧 geth
- `Nezha/cmd/eth-dataset-exporter`（go 1.22）：`github.com/ethereum/go-ethereum v1.14+`（Fusaka 链配置）
- `Nezha/cmd/eth-replayd`（go 1.22）：同上

跨模块通信：**Unix socket JSON**（eth-replayd 作为本地服务端）+ **文件 dataset**（主模块读）。

### 决策 2：Archive 节点 → Alchemy PAYG（已确认）

RPC endpoint: `https://eth-mainnet.g.alchemy.com/v2/<YOUR_KEY>`

Alchemy 关键 CU 权重：
| 方法 | CU |
|---|---|
| `debug_traceTransaction`（prestateTracer + diffMode 共用） | 40 |
| `eth_getBlockByNumber`（full txs） | 20 |
| `eth_getTransactionReceipt` | 20 |

$5 预存 = 11.11M CU + 30M 免费 = ~41M CU（需确认免费档是否含 trace：不含的话用 11.11M 付费 = 足够 10 块）。

### 决策 3：ReplayExecutor 接口（细化）

不修改现有算法内部，只在算法入口前做「执行器适配」。接口放在新文件，避免侵入主模块现有代码：

```
core/replay_types.go（新增）
```

```go
package core

import "math/big"

type ReplayRef struct {
    BlockNumber uint64
    TxIndex     uint
    TxHash      string // hex, 0x prefix
}

// ReplayRWSet 记录单笔交易的真实读写（prestate + diff 捕获，或执行后抓）
type ReplayRWSet struct {
    ReadKeys    []string            // "acct:0x..:balance" / "slot:0x..:<hexslot>"
    ReadValues  map[string][]byte   // key -> pre-value (big-endian bytes, 与 levm 保持一致)
    WriteKeys   []string
    WriteDeltas map[string]*big.Int // key -> signed delta (沿用现有 delta 约定: 负数转换 2^255 逻辑)
}

type ExecuteResult struct {
    Status    uint64            // receipt.Status
    GasUsed   uint64
    Logs      []interface{}     // 简化版：[]map[string]interface{}
    RWSet     ReplayRWSet       // 执行过程真实读写
    Missed    []string          // WitnessMiss 的 key 列表（非空 → abort）
    ExecTimeNs int64            // 纯 EVM 执行耗时（不含 socket/io）
}

// ReplayExecutor 定义 replay 模式的执行器抽象
// 主模块通过 HTTP/JSON 与 eth-replayd 交互的客户端实现此接口
// synthetic 模式另有 LevmExecutor 包装现有逻辑（供后续统一，本阶段可不实现）
type ReplayExecutor interface {
    // LoadBlock 让 eth-replayd 加载某块的 witness，重置内部稀疏 StateDB
    LoadBlock(blockNum uint64) error

    // PreExecute 按 canonical 顺序串行预执行某笔交易，抓真实 RWSet（用于图构建）
    PreExecute(blockNum uint64, txIndex uint) (*ReplayRWSet, error)

    // Execute 按指定 logicalState（合并后的当前状态快照）执行某笔交易，返回 receipt + RWSet
    // blockNum/txIndex 定位 tx；logicalState 是 key -> value 的合并快照
    Execute(blockNum uint64, txIndex uint, logicalState map[string][]byte) (*ExecuteResult, error)

    // Close 释放
    Close() error
}
```

### 决策 4：RW key 格式与 CompositeKey 兼容

[rwset_builder.go](file:///Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go#L51-L53) 现有 `CompositeKey()` 为 `ContractName + ":" + hex(Key)`。

**Replay 模式下**：
- `ContractName` 固定为 `""`（空字符串）
- `RWSet.Key` 直接是人类可读字符串的 bytes：`acct:0xabc:balance` / `slot:0xdef:000..01`（slot 用 32B hex，无前导零截断，保持 64 hex 字符）
- `CompositeKey()` 会变成 `":acct:0xabc:balance"`（开头多一个冒号）

→ 增加一个新方法，replay 算法入口**全局替换** key 比较逻辑：

```go
// 在 core/rwset_builder.go 追加
func (rw *RWNode) CanonicalKey() string {
    if rw.ContractName == "" {
        // replay 模式：key 本身已是 "acct:..." / "slot:..." 字符串
        return string(rw.RWSet.Key)
    }
    // synthetic 模式：保持原来的 composite 格式
    return rw.ContractName + ":" + ConvertByte2String(rw.RWSet.Key)
}
```

然后修改 `conflict_queue.go`、`classical_graph.go` 中对 RWNode 键的比较，从 `CompositeKey()` 切到 `CanonicalKey()`。但这是**全局侵入**——更安全的做法：在 replay 版算法入口把 `ContractName` 设为 `"__REPLAY__"`（合成合约不可能叫这个名），并把 RWSet.Key 直接作为字节存储，然后在冲突检测处改用 `ConvertByte2String(rw.RWSet.Key)` 比较——实际上 RWNode 冲突检测走的是 `ConvertByte2String` 全字节比较，**ContractName 只用于 `CompositeKey()`（已废弃的旧比较方法）**。先确认 conflict_queue.go 的冲突检测实现再决定，如用 `string(rw.RWSet.Key)` 直接比较则 ContractName 不影响，**零侵入**。

**验证点（执行时）**：打开 [conflict_queue.go](file:///Users/yellowstar/Desktop/code/Nezha/core/conflict_queue.go) 与 [classical_graph.go](file:///Users/yellowstar/Desktop/code/Nezha/core/classical_graph.go)，grep 看是否调用 `CompositeKey()` 或 `ContractName`。若不调用，则决策 4 退化为「ContractName="", Key=raw bytes of key-string」即可，无需新增 `CanonicalKey()`。

---

## 具体文件与修改清单

### 模块 A：`go.work` + 两个 cmd 子模块脚手架

| 文件 | 动作 | 内容 |
|---|---|---|
| `go.work`（根目录） | **新增** | `go 1.22` + `use (./  ./cmd/eth-dataset-exporter  ./cmd/eth-replayd)` |
| `cmd/eth-dataset-exporter/go.mod` | **新增** | `module Nezha/cmd/eth-dataset-exporter`, go 1.22，require `github.com/ethereum/go-ethereum v1.14.x` + `github.com/klauspost/compress`（zstd） |
| `cmd/eth-replayd/go.mod` | **新增** | 同上，额外 `github.com/gorilla/websocket`（可选） |

先 `cd cmd/eth-dataset-exporter && go build` 确认模块隔离。

---

### 模块 B：`cmd/eth-dataset-exporter`（阶段 1）

**目标**：Alchemy RPC → plan.md 第 4 节的 dataset 格式（`datasets/mainnet-24000000-24010000/`）。

| 文件 | 内容 |
|---|---|
| `cmd/eth-dataset-exporter/main.go` | CLI 解析：`--rpc URL --from 24000000 --to 24000009 --out ./datasets/mainnet-24000000-24010000 --rpc-key $ALCHEMY_KEY --chunk-size 100 --checkpoint ./datasets/.../.checkpoint` |
| `cmd/eth-dataset-exporter/exporter/rpc_client.go` | 封装 ethclient + raw JSON-RPC（`debug_traceTransaction` 需要 raw client，ethclient 不含）：`GetBlock(N)`, `GetReceipts(blockHash)`, `TraceTx(txHash, diffMode bool)`, `GetBlockHash256(windowStart)` |
| `cmd/eth-dataset-exporter/exporter/trace.go` | prestateTracer 响应 Go struct（pre + diffMode 两个变体）：`type PrestateAccount{Balance, Nonce, CodeHash, Storage map[string]string}`，JSON tag 对齐 Geth prestateTracer 输出 |
| `cmd/eth-dataset-exporter/exporter/witness.go` | `BuildBlockWitness(traces []*TraceResult, sortedByTxIndex) -> BlockWitness`：按 §5.3 首次出现即记录；生成 `acct:<addr>:balance/nonce/code` + `slot:<addr>:<slot>` 的 RW key 集；每笔 tx 的读集/写集（§5.4 RWSet 构建） |
| `cmd/eth-dataset-exporter/exporter/dataset.go` | 写文件：manifest.json、headers.jsonl.zst、`code/<codeHash>.bin.zst`（去重写入，用 sync.Map 记录已保存 codeHash）、`blocks/<N>.json.zst`（{header, transactions, witness, canonical:{receipts, statuses, gasUsed}, rwsets:[]TxRWSets}） |
| `cmd/eth-dataset-exporter/exporter/checkpoint.go` | 断点续传：记录上一次成功块号 N，从 N+1 继续；每块成功写盘后 `atomic.Store` checkpoint 文件 |

**依赖**：`github.com/ethereum/go-ethereum/rpc`（for `rpc.Client`） + `github.com/klauspost/compress/zstd`（`Writer.Pipe` 流式压缩 JSON）。

**Alchemy 限流**：`debug_traceTransaction` 40 CU → PAYG 10,000 CU/s → ~250 trace/s。exporter 内部 **并发 goroutine 池 50–100**（ants 或简单 channel worker），`debug_traceTransaction` 单独一个 worker 池（避免打满 CU/s 触发 429），配指数退避（429 → sleep 1s×retry，上限 30s）。

**验证**：导 10 块后，用 `jq .manifest.json` 检查 `fromBlock/toBlock/sourceClient="alchemy"`；随机抽 `blocks/24000000.json.zst` 解压后检查 `witness.accounts` 非空、`transactions[0].maxFeePerBlobGas` 字段存在（检查 blob/7702 支持）、`rwsets[0].ReadKeys` 与 `WriteKeys` 非空。

---

### 模块 C：`cmd/eth-replayd`（阶段 3）

**目标**：加载 dataset + 新版 geth StateDB 稀疏初始化 → 按顺序执行交易 → 返回 receipt + 真实 RWSet。

| 文件 | 内容 |
|---|---|
| `cmd/eth-replayd/main.go` | CLI：`--dataset ./datasets/mainnet-24000000-24010000 --socket /tmp/eth-replayd.sock --http :8599`；同时支持 Unix socket（本地 IPC 低延迟）和 HTTP JSON（方便调试）；监听 `LoadBlock/PreExecute/Execute/Close` 命令 |
| `cmd/eth-replayd/replayd/types.go` | 与主模块 `core/replay_types.go` 对齐的 JSON 请求/响应类型（用 JSON 跨模块，不共享 Go 类型） |
| `cmd/eth-replayd/replayd/dataset_reader.go` | `LoadDatasetBlock(N) -> *BlockData`：读 `blocks/<N>.json.zst`，解压 header/transactions/witness/canonical/rwsets；按需从 `code/` 取 code；cache 最近 8 块的 dataset 在内存 |
| `cmd/eth-replayd/replayd/sparse_statedb.go` | `SparseStateDB`：用 `github.com/ethereum/go-ethereum/core/state.New(statedb)` + `ethdb/memorydb.New()` 或 leveldb（内存级足够：单块 witness 稀疏）；从 witness 注入：`SetBalance/SetNonce/SetCode/SetState`；**缺失即返回 WitnessMiss**（自定义 hook 覆写 `stateDB.GetBalance/..`：witness 没 key 时不返回 0，写入 `witnessMissSet` 并返回标记） |
| `cmd/eth-replayd/replayd/block_env.go` | `BuildBlockEnv(header, blockHashes256) -> vm.BlockContext + vm.TxContext`：从 header 填 `BaseFee/PrevRandao/Timestamp/GasLimit/BlobBaseFee/ExcessBlobGas`；`BlockHash(n)` 从前 256 块 hash 表里查（不足 256 时补零或不访问）；链 config 用 `params.MainnetChainConfig`（确保 Fusaka 规则，必要时手动 config 过 `Config.LondonBlock` 等） |
| `cmd/eth-replayd/replayd/executor.go` | `ExecuteTx(tx *types.Transaction, sparseDB, blockEnv) -> ExecuteResult`：调用 `ApplyTransaction`（新版 geth core/state_process 调用），同时用 **StructLogger** 包装 EVM 记录 SLOAD/SSTORE/BALANCE/EXTCODEHASH 以抓真实 RWSet（执行完后转成 `acct:`/`slot:` 格式的 RW key）；同时记录 `ExecTimeNs`（仅 `ApplyTransaction` 内部计时，前后用 `time.Now()` 包一下） |
| `cmd/eth-replayd/replayd/miss_handler.go` | `WitnessMiss` 检测：sparseDB 的 hook 在 ExecuteTx 中每次读缺失时把 key 记到 miss 列表；返回给主模块；支持从 Alchemy RPC 「按需补齐」（向 Alchemy 发 `eth_getProof` 或 `debug_traceTransaction` 抓单 key），然后重执行（串行回退逻辑在主模块，replayd 只负责补状态 + 重跑） |

**关键：WitnessMiss 硬约束**（plan.md §6）：`sparse_statedb.go` 中所有 `Get*` 方法，先查 witness map，**miss 时不返回零值**，记录 miss key 并 propagate 到 ExecuteResult.Missed。

**EVM 并发池**：`runtime.NumCPU()` 个独立 `SparseStateDB + EVM` 实例，每实例一个 goroutine（与 project_memory「每个 goroutine 自建 EVM」教训一致）。

**正确性测试（阶段 2，先在 eth-replayd 内部实现）**：main.go 独立模式 `--self-test 24000000-24000009`，加载 dataset 后 canonical 顺序串行执行每笔交易，对比 ExecuteResult.Status/GasUsed/Logs 与 dataset 中 canonical.receipts，**100% 相等才算过**。

---

### 模块 D：主 Nezha 模块最小侵入新增（阶段 4–6）

#### D.1 新增文件（无侵入，仅 append）

| 文件 | 内容 |
|---|---|
| `core/replay_types.go` | `ReplayRef`、`ReplayRWSet`、`ExecuteResult`、`ReplayExecutor` 接口（见决策 3） |
| `core/replay_rw_keys.go` | RW key 格式工具：`AcctKey(addr, field)`, `SlotKey(addr, slotHex)`, `ParseKey(s)`；与 witness.go 格式严格一致 |
| `utils/replay_dataset.go` | `LoadReplayBlocks(datasetDir string, from, to uint64) ([]*ReplayBlockData, error)`：读 manifest → 遍历块文件；`type ReplayBlockData struct{Header map[string]interface{}; TxHashes []string; TxCount int; WitnessRef string}`（不加载整 witness，只给引用） |
| `utils/replay_adapter.go` | 两个关键函数：`BuildReplayRWNodes(block *ReplayBlockData, preExecRWs []*core.ReplayRWSet) []*core.RWNode`（把 PreExecute 返回的 RWSet 转成现有 RWNode 列表，Label="r"/"w"，ContractName=""，Key=[]byte("acct:.../slot:...")）；`BuildReplayContexts(block *ReplayBlockData, preExecRWs []*core.ReplayRWSet) map[string]*core.TransactionContext`（TxID = `fmt.Sprintf("%d-%d", blockNum, txIndex)`，ReplayRef 填好，其余字段零值） |
| `utils/replay_executor_client.go` | `ReplayExecutor` 的 HTTP/Unix socket 客户端实现：`type HTTPClient struct{ Endpoint string; client *http.Client }`；`LoadBlock/PreExecute/Execute` 直接发 JSON POST 到 eth-replayd |
| `utils/replay_metrics.go` | `type BlockMetrics struct{ TLoad, TPreexec, TSchedule, TExecute, TValidate, TCommit time.Duration; AbortCount, WitnessMissCount, RWSetExpandCount int }`；对应 plan.md §12 的分项计时；`func (m *BlockMetrics) TPSExecute(txCount int) float64`、`TPS(...)`、`CSVHeader() string` |

#### D.2 修改现有文件（侵入最小，尽量新增分支不碰原逻辑）

| 文件 | 改动点 | 为什么 |
|---|---|---|
| `core/rwset_builder.go` | `TransactionContext` 追加 `Replay *ReplayRef` 字段（json 忽略或可选） | 算法内部用 TxID 回溯定位；synthetic 模式 Replay=nil，不影响 |
| `core/conflict_queue.go` | 确认 RWNode 冲突比较用的是 `RWSet.Key`（不含 ContractName）；如果有 `CompositeKey()` 调用，全局替换为 `CanonicalKey()` 或 string(rw.RWSet.Key) | Replay 模式 ContractName==""，冲突检测仍需正确 |
| `core/classical_graph.go` | 同上 | 同上 |
| `test.go` | 追加 replay 模式 CLI flag：`-dataset string`、`-replayFrom uint`、`-replayTo uint`、`-replaydAddr string`；main() 中新增分支：`if dataset != "" { runReplayMode(...) os.Exit(0) }` | 保留合成 mode 不变；replay 是独立入口 |
| `test.go` | 新增 `runReplayMode()`、`TestReplaySerial()`、`TestReplayCG()`、`TestReplayNezha()`、`TestReplayNezhaVariable()`、`TestReplayVegeta()`（replay 版算法入口） | 算法复用 core 包调度逻辑，只是：输入来自 `BuildReplayRWNodes`、重执行走 `ReplayExecutor.Execute`、状态合并在 HTTPClient 之上、abort 检测含 `result.Missed` 判断 |

#### D.3 Replay 版算法执行模式（test.go 新函数内部逻辑）

```
TestReplayNezha(blockNum, preExecRWs, writer, executor, metrics):
  1. TLoad += load & witness init（executor.LoadBlock 已做，测它的耗时）
  2. TPreexec += 已由 exporter 产出，不重复测（或测一次 PreExecute 串行）
  3. rwNodes := BuildReplayRWNodes(block, preExecRWs)
  4. contexts := BuildReplayContexts(block, preExecRWs)
  5. TSchedule := timed Run Nezha conflict_queue 调度（现 core 代码）
  6. for 每个调度好的 batch 并行：
       logicalState := batch 开始前的 committedState（批级快照，Vegeta 必选）
       for each tx in batch:
         TExecute += timed executor.Execute(blockNum, txIndex, logicalState)
         TValidate += check delta 相等 & !missed
           失败 → abort + 加到串行回退列表 + 记录 AbortCount/WitnessMissCount
         ok → merge committedState
  7. TCommit += final state merge
  8. 指标写 writer
```

Vegeta 专用：**批级快照隔离**（project_memory 教训）：每个 batch 开始前 clone committedState 到 snapshot，所有 tx 在 snapshot 上读，写入 batch-local pending，batch 完后原子合并。

---

### 模块 E：dataset 格式 Go struct 定义

在 `cmd/eth-dataset-exporter/exporter/dataset.go` 和 `cmd/eth-replayd/replayd/dataset_reader.go` 共用（或单独放一个共享的 `types` 包，跨模块用 JSON 耦合，不用共享 Go 包——以避免 go.work 类型循环依赖；直接 JSON tag 对齐更稳）。

```go
// 两边 struct 一致，tag 对齐
type BlockDataset struct {
    Header       map[string]interface{} `json:"header"`
    Transactions []interface{}          `json:"transactions"` // 保留 raw tx 对象
    Witness      BlockWitness           `json:"witness"`
    Canonical    CanonicalReceipts      `json:"canonical"`
    RWSets       []TxRWSets             `json:"rwsets"`
}

type BlockWitness struct {
    Accounts map[string]*WitnessAccount `json:"accounts"`
}

type WitnessAccount struct {
    Balance  string            `json:"balance"`  // hex
    Nonce    string            `json:"nonce"`
    CodeHash string            `json:"codeHash"`
    Storage  map[string]string `json:"storage"`  // hexSlot -> hexValue
}

type TxRWSets struct {
    TxHash     string   `json:"txHash"`
    TxIndex    int      `json:"txIndex"`
    ReadKeys   []string `json:"readKeys"`   // "acct:..." / "slot:..."
    WriteKeys  []string `json:"writeKeys"`
}
```

---

## 实施步骤（8 阶段，可验证）

### 阶段 0：脚手架（1 天）

1. 建 `go.work`，建 `cmd/eth-dataset-exporter` 和 `cmd/eth-replayd` 空 go.mod 子目录，各自 `go mod tidy` 确认可编译。
2. 验证：`go work sync` 通过，主模块 `go build ./...` 仍然通过（不引新 geth）。

### 阶段 1：exporter + 导 10 块

1. 写 rpc_client.go / trace.go / witness.go / dataset.go / checkpoint.go / main.go。
2. Alchemy KEY 从环境变量 `ALCHEMY_API_KEY` 读。
3. 导块 24,000,000–24,000,009。
4. 验证：manifest.json 正确；blocks JSON.zst 解压可 jq 读；witness.accounts 至少有 50+ 个账户（正常块水平）。

### 阶段 2：eth-replayd 串行自检验收

1. 写 dataset_reader.go / sparse_statedb.go / block_env.go / executor.go / main.go self-test。
2. 跑 `eth-replayd --dataset ... --self-test 24000000-24000009`。
3. **验收门槛**：10 块中每笔 tx 的 `Status == canonical.Status`、`GasUsed == canonical.GasUsed`、`Logs 数一致`（100% 匹配）。
4. 同时在阶段 2 验证 WitnessMiss：随机删除一个块 witness 中某个 storage slot 的值，再 self-test，应该返回 non-empty Missed 并 abort（不返回零值）。

### 阶段 3：主模块类型 + ReplayExecutor 客户端

1. 新增 `core/replay_types.go`、`core/replay_rw_keys.go`。
2. 新增 `utils/replay_dataset.go`、`utils/replay_adapter.go`、`utils/replay_executor_client.go`。
3. `TransactionContext` 追加 `Replay` 字段。
4. 验证：主模块 `go build ./...` 通过。

### 阶段 4：接 Serial + CG + Nezha（replay 入口）

1. `test.go` 加 CLI flag `-dataset/-replayFrom/-replayTo/-replaydAddr`。
2. 新增 `runReplayMode()`、`TestReplaySerial()`、`TestReplayCG()`、`TestReplayNezha()`。
3. 启动 eth-replayd，跑 replay 模式。
4. 验证：Serial 的 AbortCount=0、WitnessMissCount=0、GasUsed 匹配；Nezha 与 Serial 的最终状态一致（读所有 touched key 最终值比对）。

### 阶段 5：接 NezhaVariable + Vegeta（含重执行验证）

1. 新增 `TestReplayNezhaVariable()`、`TestReplayVegeta()`。
2. Vegeta 实现批级快照隔离（在 executor 客户端侧 clone logicalState；或在 eth-replayd 内部按 batch 建 SparseStateDB 快照）。
3. 验证：Vegeta 的 T_execute ≠ 0；AbortRate 合理（1–5%，主网冲突真实）；多次运行 AbortCount 稳定（Vegeta 批内非确定性已消除）。

### 阶段 6：扩 1,000 块 + checkpoint

1. exporter 加 checkpoint，导块 24,000,000–24,000,999。
2. 阶段 2 self-test 在 100 块样本随机抽 10 块仍过 → 通过。
3. 5 个算法 × 1000 块跑一次。
4. 验证：输出 metrics CSV，T_execute/T_total 分栏完整。

### 阶段 7：导全区间 10,000 块

1. exporter 调大池，按 100 块为 chunk，断点续传。
2. 预计 Alchemy 成本 ~$60–80（在 $5 基础上追加）。
3. 验证：manifest.json 的 `fromBlock/toBlock == 24000000/24010000`；每块 JSON.zst 存在；code 目录 codeHash 去重命中。

### 阶段 8：3 次重复 + 指标报告

1. 对每种算法（Serial/CG/Nezha/NezhaVariable/Vegeta），每块重复 3 次，固定 `GOMAXPROCS=runtime.NumCPU()` 或固定值。
2. 汇总 plan.md §12 全部指标：TPS_execute、TPS_end_to_end、AbortRate、WitnessMissRate、RWSetExpansionRate、P50/P95 latency。
3. 输出 CSV + summary。

---

## 风险与缓解

| 风险 | 缓解 |
|---|---|
| Alchemy 429 限流 | exporter 指数退避；trace 池独立；批处理 batch（200 tx × 2 trace = 400/块，控制并发块数 ≤10 即 4,000 trace 并发，10K CU/s 下 ~0.4s/块） |
| 新版 geth prestateTracer 的 EIP-7702 authorizationList 账户覆盖不全 | 阶段 2 自检会直接报 WitnessMiss/不匹配；WitnessMiss → 从 archive `eth_getProof` 补齐；若 7702 类型 tx 的 set-code-authority 账户没有出现在 prestate，再单独从 Alchemy `eth_getCode/eth_getBalance/eth_getTransactionCount` 抓一遍补 witness |
| RW key 冲突检测在 replay 模式漏边 | 阶段 4 Serial vs Nezha 校验：相同 tx 集执行后最终状态必须 byte 级相等（读每个 key 的值从 sparse_state_db 取出比对） |
| levm 路径被 replay 侵入 | 决策 1：go.work 隔离；决策 3：ReplayExecutor 接口旁路，合成模式不动 |
| Vegeta 批非确定性 | project_memory 教训已内化：Vegeta replay 版强制 orderedTxs 字典序（replay 下 txIndex 已有序，但仍显式按 TxID 排）、批级 logicalState clone、从 addr 在 tx 里固定（天然） |
| 10k 块 dataset 内存爆 | dataset 流式按块加载 + 内存缓存 8 块，算法跑完一块释放一块 |
| sparse_statedb 内存泄露 | executor 每块 reset StateDB（`statedb = state.New(memdb)`）或复用 memdb 后 clear |

---

## 验证清单

### 可执行命令（阶段 1–2 验收）

```bash
# exporter 导 10 块
cd cmd/eth-dataset-exporter && go build .
ALCHEMY_API_KEY=<KEY> ./eth-dataset-exporter \
  --rpc https://eth-mainnet.g.alchemy.com/v2/ \
  --from 24000000 --to 24000009 \
  --out ../../datasets/mainnet-24000000-24010000 \
  --checkpoint ../../datasets/mainnet-24000000-24010000/.checkpoint

# self-test
cd cmd/eth-replayd && go build .
./eth-replayd \
  --dataset ../../datasets/mainnet-24000000-24010000 \
  --self-test 24000000-24000009
# Expected: "10 blocks passed: receipts match; 0 WitnessMiss; 0 abort"
```

### 阶段 4 验收

```bash
# 启动 eth-replayd 服务
./eth-replayd --dataset ... --http :8599 &

# 跑 Nezha replay
cd ../.. && go run test.go \
  -dataset ./datasets/mainnet-24000000-24010000 \
  -replayFrom 24000000 -replayTo 24000009 \
  -replaydAddr http://127.0.0.1:8599 \
  -Nezha=true -serial=true -CG=true
# Expected: metrics.csv 中各算法指标完整
```

### 最终运行（阶段 8）

```bash
# 导出全区间
ALCHEMY_API_KEY=<KEY> ./eth-dataset-exporter --rpc ... --from 24000000 --to 24010000 --out ./datasets/mainnet-24000000-24010000

# 3 次重复实验
for i in 1 2 3; do
  go run test.go \
    -dataset ./datasets/mainnet-24000000-24010000 \
    -replayFrom 24000000 -replayTo 24009999 \
    -replaydAddr http://127.0.0.1:8599 \
    -serial=true -CG=true -Nezha=true -NezhaVariable=true -Vegeta=true \
    > results_run${i}.csv 2>&1
done
```

---

## 假设

1. 用户已经有 Alchemy PAYG key（已充值 ≥$5）。
2. Go 1.22+ 已安装（或 cmd/子模块构建机已有，本机可用 `brew install go@1.22`）。
3. 磁盘空间 ≥ 15 GB（dataset 1.1 GB + exporter 临时文件 + eth-replayd sparse statedb 内存/临时 leveldb）。
4. 网络能直连 `eth-mainnet.g.alchemy.com:443`。
5. 本阶段不接 Depurge（plan.md §9：主网缺源码/ABI/storage layout，Depurge 结论性实验等后期单独接 verified contracts 子集）。
