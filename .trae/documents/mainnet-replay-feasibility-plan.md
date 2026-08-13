# plan.md 可行性评估与实施规划

## 概要

针对 [plan.md](file:///Users/yellowstar/Desktop/code/Nezha/plan.md)（24,000,000–24,010,000 主网区间真实交易 EVM 执行 benchmark）逐节评估可行性，并给出基于当前 Nezha 代码库现状的可执行实施路线。

**核心结论**：方案的技术设计是正确且可落地的，但有三个必须先解决的前提：

1. **Archive/Trace 节点（硬阻塞）**：Alchemy 免费档 **不支持 Debug API 与 Trace API**（`debug_traceTransaction` + `prestateTracer` 在免费档为 ✗）。witness 抽取无法在免费节点完成。必须升级到 PAYG/Enterprise 或自建 archive Geth。
2. **Go 模块边界（已决策）**：采用 `go.work` 多模块，`cmd/eth-dataset-exporter` 与 `cmd/eth-replayd` 各自独立 `go.mod` 引入新版 `github.com/ethereum/go-ethereum`，与主 Nezha 模块（含 vendored 旧版 geth）通过 JSON/Unix socket 通信。
3. **算法重执行委托**：现有算法（NezhaVariable/Vegeta/Depurge）通过 `utils.ReExecuteAndGetRealRWSet` 调用 levm 重执行；replay 模式必须改为委托 `eth-replayd`，因此需要新增 replay 专用算法入口或可插拔执行器接口。

---

## 现状分析（基于代码探查）

| 维度 | 现状 | 对 plan.md 的影响 |
|---|---|---|
| Go 版本 | `go 1.18`（[go.mod](file:///Users/yellowstar/Desktop/code/Nezha/go.mod)） | 偏旧；新版 go-ethereum 需 Go 1.21+，新模块独立 go.mod 可各自指定更高 Go 版本 |
| vendored geth | `Nezha/ethereum/go-ethereum`（~v1.9 时代，pre-London） | vm 包**无** `BaseFee`/`BlobGas`/`excessBlobGas`/`requestsHash`/EIP-7702 支持。无法回放 24M+（post-Pectra/post-Fusaka）块。plan.md 第 7 节判断正确 |
| levm | [evm/levm/levm.go](file:///Users/yellowstar/Desktop/code/Nezha/evm/levm/levm.go) 用 `params.AllEthashProtocolChanges` + 旧 `vm.Context`（无 BaseFee/Random） | 仅供合成 benchmark；不可升级，plan.md 不动 levm 的决策正确 |
| EthTransaction | [core/ethTransaction.go](file:///Users/yellowstar/Desktop/code/Nezha/core/ethTransaction.go) 仅 legacy（无 type/accessList/1559/blob/7702 字段） | 无法承载主网真实交易；必须由新模块处理 |
| TransactionContext | [core/rwset_builder.go:77](file:///Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go) 字段偏合成合约（`ContractName`/`Function`/`Addr1`/`Addr2 uint64`/`LLMReads`/`LLMWrites`） | replay 路径下这些字段无意义；需 `Replay *ReplayRef` 旁路，合成字段留空 |
| RWNode | `{RWSet{Key,Value}, TransInfo{ID,Ts}, Label, Sequence, ContractName}` | key 为 `[]byte`，可承载 `acct:<addr>:<field>` / `slot:<addr>:<slot>`；适配可行 |
| 算法入口 | [test.go](file:///Users/yellowstar/Desktop/code/Nezha/test.go) 中 `TestSerialExecution`/`TestConflictQueue`(Nezha)/`TestConflictGraph`(CG)/`TestDepurge`/`TestNezhaVariable`/`TestVegeta`，均吃 `[]utils.Transaction` 并经 levm 预执行/重执行 | 需新增 replay 版算法入口，把 RWSet 来源与重执行目标切到 eth-replayd |
| cmd/ 目录 | 不存在 | 需新建 |
| 已有重放雏形 | `TestReplayingTx` + `levm.ReplayTransaction`（用旧 geth，legacy-only） | 仅作参考，不能直接用于 24M+ |

---

## plan.md 各节可行性评估

| 节 | 标题 | 可行性 | 说明 / 风险 |
|---|---|---|---|
| 1 | 实验边界 | ✅ 可行 | 每块独立稀疏状态、不维护连续世界状态、不追 stateRoot。范围清晰 |
| 2 | 总体架构 | ✅ 可行 | exporter → dataset → adapter → eth-replayd → 验证。分层合理 |
| 3 | 数据获取前提 | ⚠️ **硬阻塞** | Alchemy 免费档 Debug/Trace API 为 ✗；prestateTracer 不可用。需 PAYG/自建 archive Geth/其他 trace 提供商 |
| 4 | 数据集格式 | ✅ 可行 | manifest + headers + code 按 codeHash 去重 + 每块 JSON.zst。需覆盖 legacy/AL/1559/blob/7702 全交易类型（24M+ 已 Pectra，含 7702） |
| 5 | 稀疏状态构造 | ✅ 可行 | 普通 prestate trace 取读集、diffMode trace 取写集；按 txIndex 序首次出现即记录。标准做法。需验证新版 geth prestateTracer 对 7702 `authorizationList` 账户的覆盖 |
| 6 | 缺失状态处理 | ✅ 可行且正确 | `WitnessMiss → abort → 串行回退 → archive 补齐 → 重试`。这是防静默错误的关键硬约束，设计正确 |
| 7 | 新执行器不动旧 levm | ✅ 可行（需补模块边界细节） | 必须用 go.work 多模块；新模块独立 go.mod 引入新版 geth，与主模块经 JSON/socket 边界隔离（plan.md 已暗示，需明确为决策） |
| 8 | 对 Nezha 最小适配 | ✅ 可行（有保留） | 加 `ReplayRef` 与 `replay_*.go` 文件侵入小。但算法重执行路径仍依赖 levm，需新增 replay 入口或执行器接口（见关键决策 3） |
| 9 | 算法接入策略 | ✅ 可行 | 第一阶段 Serial/CG/Nezha/NezhaVariable/Vegeta；Depurge 暂不用于主网（缺源码/ABI/storage layout）。判断合理 |
| 10 | 每块实验流程 | ✅ 可行 | 加载→稀疏 StateDB→预执行取真实 RWSet→调度→eth-replayd 执行→验证→abort/miss 回退→指标 |
| 11 | 正确性验证 | ✅ 可行且严格 | receipt.status/logs/gasUsed/写集/动态访问/WitnessMiss=0/账户语义。串行 canonical 重放 == archive receipt 是合格门槛 |
| 12 | 指标定义 | ✅ 可行 | 拆分 T_export/T_load/T_preexec/T_schedule/T_execute/T_validate/T_commit；TPS_execute vs TPS_end_to_end 分开。避免单一 TPS 误导 |
| 13 | 实施顺序 | ✅ 可行 | 8 步分阶段合理：exporter(10–100 块) → Serial 验证 → eth-replayd 稀疏+miss → trace→RWNode → Serial/CG/Nezha → NezhaVariable/Vegeta → 100 块 chunk+checkpoint → 3×重复 |

**总体**：13 节中 12 节可行，第 3 节为外部前提阻塞（非设计缺陷），第 7/8 节需补充模块边界与执行器委托两项实现决策。

---

## 关键架构决策

### 决策 1：go.work 多模块（用户已确认）

仓库根新增 `go.work`：

```text
Nezha/
├── go.work                      // include ./  ./cmd/eth-dataset-exporter  ./cmd/eth-replayd
├── go.mod                       // 主模块不变（module Nezha，go 1.18，含 vendored 旧 geth）
├── core/  utils/  evm/  graph/  // 不动
└── cmd/
    ├── eth-dataset-exporter/
    │   ├── go.mod               // module Nezha/cmd/eth-dataset-exporter，go 1.22+
    │   └── main.go              // 引入 github.com/ethereum/go-ethereum（新版，Fusaka 兼容）
    └── eth-replayd/
        ├── go.mod               // module Nezha/cmd/eth-replayd，go 1.22+
        └── main.go              // 引入 github.com/ethereum/go-ethereum（新版）
```

- 主模块的 `Nezha/ethereum/go-ethereum`（vendored 旧版）与新模块的 `github.com/ethereum/go-ethereum`（新版）**互不可见**（不同 import path + 不同 module），类型不冲突。
- 跨模块通信仅经 **JSON/Unix socket**，不共享 Go 类型。
- 新模块各自的 `go.mod` 可独立指定 Go 1.22+（新版 geth 要求），不受主模块 `go 1.18` 约束。

### 决策 2：Archive 节点（硬阻塞，必须先解决）

Alchemy 免费档 **不支持 Debug API / Trace API**（`debug_traceTransaction`+`prestateTracer` 在免费档为 ✗）。三个可选方案：

| 方案 | 成本 | 适用 |
|---|---|---|
| Alchemy PAYG | ~$5/mo + 用量；trace 调用 40 CU/次，全区间约 3M trace 调用 ≈ 120M CU ≈ $50–100 | 快速启动，推荐起步 |
| 自建 archive Geth | 免费；存储 ~2TB+，同步数天 | 长期、大批量、可重复 |
| 其他 trace 提供商（Dwellir/QuickNode archive 等） | 视提供商 | 备选 |

**未解决前，第 13 节步骤 1（exporter 导出 10–100 块）无法启动。** 先用 PAYG 导 10 块验证链路，再决定长期方案。

### 决策 3：算法重执行的可插拔执行器接口

现有算法重执行经 `utils.ReExecuteAndGetRealRWSet(ctx, dbFile, logicalState)` → levm。replay 模式需切到 eth-replayd。引入接口而非改写每个算法：

```go
// core/replay_types.go（新增）
type ReplayExecutor interface {
    // 预执行：返回 RWSet（用于图构建）+ context
    PreExecute(blockNum uint64, txIndex uint) (readKeys, writeKeys []string, writeDelta map[string]*big.Int, err error)
    // 重执行：按 logicalState 重跑，返回真实 RWSet（用于验证）
    ReExecute(blockNum uint64, txIndex uint, logicalState map[string][]byte) (readKeys, writeKeys []string, writeDelta map[string]*big.Int, err error)
}
```

- 合成模式：实现走 levm（包装现有 `utils.ReExecuteAndGetRealRWSet`）。
- replay 模式：实现走 eth-replayd（HTTP/socket）。
- 算法入口（`TestNezhaVariable`/`TestVegeta` 等）参数化执行器，replay 复用同一调度逻辑。

### 决策 4：TransactionContext 的 Replay 旁路

在 [core/rwset_builder.go](file:///Users/yellowstar/Desktop/code/Nezha/core/rwset_builder.go) 的 `TransactionContext` 加：

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

replay 路径下 `ContractName`/`Function`/`Addr1`/`Addr2`/`LLMReads`/`LLMWrites` 留空/零值；真实交易与 witness 始终从 dataset 读取，不复制到每个 context。

---

## 实施路线图

按 plan.md 第 13 节顺序，补充模块边界与执行器接口。每步均可独立验证。

### 阶段 0：前提准备

- [ ] 解决 Archive/Trace 节点（决策 2）。未解决则阶段 1 无法启动。
- [ ] 仓库根建 `go.work`，建 `cmd/eth-dataset-exporter/go.mod`、`cmd/eth-replayd/go.mod`（决策 1）。
- [ ] 验证新版 `github.com/ethereum/go-ethereum` 在新模块可编译（最小 hello world 调 `ethclient`）。

### 阶段 1：eth-dataset-exporter（plan.md 13.1）

- 文件：`cmd/eth-dataset-exporter/main.go` + `exporter/{rpc_client,trace,witness,dataset}.go`
- 能力：`eth_getBlockByNumber(N,true)` + 每笔 tx 两次 `debug_traceTransaction`（普通 + diffMode prestateTracer）→ 生成 manifest/headers/code/blocks（plan.md 第 4 节格式）。
- 先导出 10 块（24,000,000–24,000,009）。
- 验证：manifest 字段齐全；code 按 codeHash 去重；保存前 256 块 hash。

### 阶段 2：Serial 本地正确性验收（plan.md 13.2）

- 用 eth-replayd 串行 canonical 重放 10 块。
- 验证：本地 receipt/logs/gasUsed == archive 原始 receipt（plan.md 第 11 节）。
- 抽 100 块通过前不导出全区间。

### 阶段 3：eth-replayd 稀疏状态 + WitnessMiss（plan.md 13.3）

- 文件：`cmd/eth-replayd/main.go` + `replayd/{sparse_statedb,block_env,executor,miss}.go`
- 能力：按 witness 初始化独立稀疏 StateDB → 加载真实 BlockEnv（含 baseFee/prevRandao/excessBlobGas/requestsHash 等）→ 接收执行顺序 → 执行 → 抓真实 RWSet → 返回 receipt/状态变化。
- WitnessMiss 实现：读不存在 account/code/storage → 返回 `WitnessMiss`（不返回默认零值）→ abort → 串行回退 → 从 archive RPC 补齐 → 重试（plan.md 第 6 节）。
- EVM 执行计时在 replayd 内部记录，不混入 IPC/文件加载耗时（plan.md 第 7 节）。
- 通信：本地 Unix socket 或 HTTP JSON。

### 阶段 4：trace RWSet → RWNode 适配（plan.md 13.4）

- 文件：`utils/replay_dataset.go`、`utils/replay_adapter.go`、`core/replay_types.go`
- `LoadReplayBlocks(datasetDir, from, to) ([]ReplayBlock, error)`
- `BuildReplayContexts(block ReplayBlock) ([]*core.RWNode, map[string]*core.TransactionContext, error)`
- RW key 规则（plan.md 5.4）：`acct:<addr>:balance` / `acct:<addr>:nonce` / `acct:<addr>:code` / `slot:<contract>:<slot>`。
- `ReplayExecutor` 接口（决策 3）与 `ReplayRef`（决策 4）。
- 确保复合键防跨合约冲突（与 project_memory 中 `committedState` 复合键约定一致）。

### 阶段 5：接 Serial / CG / Nezha（plan.md 13.5）

- 新增 replay 版入口（参数化 `ReplayExecutor`），复用 [core/conflict_queue.go](file:///Users/yellowstar/Desktop/code/Nezha/core/conflict_queue.go)、[core/classical_graph.go](file:///Users/yellowstar/Desktop/code/Nezha/core/classical_graph.go) 调度逻辑。
- 每块从同一 BlockWitness 重新初始化，保证公平（plan.md 第 10 节）。

### 阶段 6：接 NezhaVariable / Vegeta 重执行验证（plan.md 13.6）

- replay 版入口经 `ReplayExecutor.ReExecute` 委托 eth-replayd，替代 `utils.ReExecuteAndGetRealRWSet`。
- 套用 project_memory 教训：Vegeta 批级快照隔离、orderedTxs 字典序、fromAddr 确定性派生（replay 下 fromAddr 来自真实 tx，天然确定性）。

### 阶段 7：100 块 chunk + checkpoint/resume（plan.md 13.7）

- exporter 支持 chunk 导出与断点续传。
- 全区间 1 万块导出。

### 阶段 8：重复实验与指标（plan.md 13.8）

- 每算法每块 ≥3 次，固定 CPU 核数与环境。
- 输出 plan.md 第 12 节全部指标（分项计时 + TPS_execute/TPS_end_to_end + AbortRate + WitnessMissRate + RWSetExpansionRate）。

### 最终运行形态（plan.md 第 13 节末）

```bash
eth-dataset-exporter --rpc "$ARCHIVE_GETH_RPC" --from 24000000 --to 24010000 --out ./datasets/mainnet-24000000-24010000
eth-replayd --socket /tmp/eth-replayd.sock &
go run test.go -dataset ./datasets/mainnet-24000000-24010000 -from 24000000 -to 24010000 -Nezha=true -NezhaVariable=true -Vegeta=true -CG=true
```

---

## 假设、风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| Archive/Trace 节点不可用 | 高（硬阻塞） | 决策 2：先 PAYG 导 10 块验链路，再决定自建或长期付费 |
| 新版 geth prestateTracer 对 7702 authorizationList 账户覆盖不全 | 中 | 阶段 2 串行验收会暴露；用 WitnessMiss + archive 补齐兜底 |
| 算法重执行经 levm 的隐式耦合未拆干净 | 中 | 决策 3：用 `ReplayExecutor` 接口隔离，不逐个改写算法 |
| 10k 块存储/IO 放大 | 中 | zstd + code 按 codeHash 去重 + 每块独立；checkpoint/resume |
| Vegeta 并发非确定性（project_memory 教训） | 中 | 批级快照隔离 + orderedTxs 字典序；replay 下 fromAddr 确定性 |
| 升级牵动现有 levm | 低 | go.work 隔离；主模块与 levm 完全不动 |
| go.work 在 CI/构建环境的兼容性 | 低 | 文档化 `go work sync`；新模块可独立 `go build` |

---

## 验收标准

1. **阶段 2（最高优先级门槛）**：串行 canonical 重放，本地 receipt/logs/gasUsed == archive 原始 receipt，10 块全过；扩到随机 100 块全过后才导出全区间。
2. **WitnessMiss 安全性**：任意并发重排序导致走入新分支时，必须 `WitnessMiss → abort → 串行回退`，不得静默返回零值。
3. **指标完备**：每块每算法输出 plan.md 第 12 节全部分项计时与比率指标，`T_execute` 与 `T_total` 分离。
4. **可重复**：同一 dataset + 固定 CPU 核数，3 次重复实验结果稳定（Vegeta 需满足批级快照隔离后的确定性要求）。
5. **最小侵入**：主 Nezha 模块（core/utils/evm/graph）仅新增 `replay_*.go` 与 `TransactionContext.Replay` 字段，不改现有合成 benchmark 路径行为。
