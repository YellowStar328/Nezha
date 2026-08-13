# 主网真实交易 EVM 回放 - Implementation Plan

## [x] Task 1: go.work + 模块脚手架
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 创建 `go.work`，include 三个模块
  - 创建 `cmd/eth-dataset-exporter/go.mod`（module Nezha/cmd/eth-dataset-exporter, go 1.22, require github.com/ethereum/go-ethereum v1.14+, github.com/klauspost/compress）
  - 创建 `cmd/eth-replayd/go.mod`（同上）
  - 验证三个模块可独立编译
- **Acceptance Criteria Addressed**: AC-1
- **Test Requirements**:
  - `programmatic` TR-1.1: `go work sync` 成功
  - `programmatic` TR-1.2: 主模块 `go build ./...` 通过（现有合成 benchmark 路径不变）
  - `programmatic` TR-1.3: `cd cmd/eth-dataset-exporter && go build .` 通过（hello world 引用 ethclient）
  - `programmatic` TR-1.4: `cd cmd/eth-replayd && go build .` 通过
- **Notes**: Go 1.22+ 必须已安装；新版 geth v1.14+ 需要 Go 1.21+

## [x] Task 2: eth-dataset-exporter — RPC 客户端 + Trace 解析
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `cmd/eth-dataset-exporter/exporter/rpc_client.go`: 封装 ethclient + raw JSON-RPC（debug_traceTransaction 需 raw client），实现 GetBlock、GetReceipts、TraceTx、GetBlockHash256
  - `cmd/eth-dataset-exporter/exporter/trace.go`: prestateTracer 响应 struct（pre + diffMode），JSON tag 对齐 Geth 输出
  - 支持 Alchemy 429 指数退避重试
- **Acceptance Criteria Addressed**: AC-2
- **Test Requirements**:
  - `programmatic` TR-2.1: 对已知交易 hash 调用 TraceTx，返回非空 Accounts map
  - `programmatic` TR-2.2: diffMode 返回的变化量正确（至少一个账户有变化）
  - `programmatic` TR-2.3: 429 错误时自动重试（sleep 指数退避）
  - `human-judgement` TR-2.4: Trace 解析代码结构清晰，字段映射与 Geth prestateTracer 文档一致

## [x] Task 3: eth-dataset-exporter — Witness 构造 + Dataset 写入
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `cmd/eth-dataset-exporter/exporter/witness.go`: BuildBlockWitness 按 plan.md §5.3 首次出现即记录，生成 acct/slot 格式 RW key
  - `cmd/eth-dataset-exporter/exporter/dataset.go`: 写 manifest.json、headers.jsonl.zst、code/<hash>.bin.zst（去重）、blocks/<N>.json.zst
  - `cmd/eth-dataset-exporter/exporter/checkpoint.go`: 断点续传
  - `cmd/eth-dataset-exporter/main.go`: CLI 解析
- **Acceptance Criteria Addressed**: AC-2
- **Test Requirements**:
  - `programmatic` TR-3.1: 导出 10 块后 manifest.json 字段正确（fromBlock=24000000, toBlock=24000009）
  - `programmatic` TR-3.2: blocks/24000000.json.zst 解压后 witness.accounts 非空（≥50 账户）
  - `programmatic` TR-3.3: rwsets 非空，至少覆盖 read 和 write key
  - `programmatic` TR-3.4: code/ 目录按 codeHash 去重写入
  - `programmatic` TR-3.5: checkpoint 断点续传可用（模拟中断后从断点继续）

## [ ] Task 4: eth-replayd — Dataset 读取 + SparseStateDB
- **Priority**: high
- **Depends On**: Task 1, Task 3
- **Description**:
  - `cmd/eth-replayd/replayd/types.go`: JSON 请求/响应类型（与 exporter dataset 格式对齐）
  - `cmd/eth-replayd/replayd/dataset_reader.go`: LoadDatasetBlock 读 zstd 压缩的块数据
  - `cmd/eth-replayd/replayd/sparse_statedb.go`: SparseStateDB，从 witness 注入账户/代码/storage；缺失即返回 WitnessMiss（不返回零值）
  - `cmd/eth-replayd/replayd/block_env.go`: BuildBlockEnv 构造 vm.BlockContext + vm.TxContext
- **Acceptance Criteria Addressed**: AC-3, AC-4
- **Test Requirements**:
  - `programmatic` TR-4.1: SparseStateDB 能正确返回 witness 中存在的账户余额/nonce/storage
  - `programmatic` TR-4.2: 读取 witness 中不存在的 key 时，返回 WitnessMiss 标记（不返回零值）
  - `programmatic` TR-4.3: BlockEnv 的 BaseFee/PrevRandao/Timestamp 正确填充
  - `programmatic` TR-4.4: Dataset 读取 zstd 解压后结构正确

## [ ] Task 5: eth-replayd — EVM 执行 + 自检验收
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - `cmd/eth-replayd/replayd/executor.go`: ExecuteTx 调用 ApplyTransaction，用 StructLogger 抓真实 RWSet，记录 ExecTimeNs
  - `cmd/eth-replayd/main.go`: CLI + self-test 模式 + HTTP/Unix socket 服务
  - 自检验收：canonical 串行重放 10 块，对比 receipt.Status/GasUsed/Logs
- **Acceptance Criteria Addressed**: AC-3, AC-4
- **Test Requirements**:
  - `programmatic` TR-5.1: self-test 模式 10 块全部通过（Status/GasUsed/Logs 100% 匹配）
  - `programmatic` TR-5.2: 人为删除 witness 中某个 slot 后 self-test，返回 non-empty Missed 并 abort
  - `programmatic` TR-5.3: ExecTimeNs 记录为纯 EVM 执行耗时（不含 IPC/文件 IO）
  - `programmatic` TR-5.4: HTTP 服务正确响应 LoadBlock/PreExecute/Execute 命令

## [ ] Task 6: 主模块 Replay 类型 + 适配器
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `core/replay_types.go`: ReplayRef、ReplayRWSet、ExecuteResult、ReplayExecutor 接口
  - `core/replay_rw_keys.go`: AcctKey、SlotKey、ParseKey 工具函数
  - `utils/replay_dataset.go`: LoadReplayBlocks 加载本地 dataset
  - `utils/replay_adapter.go`: BuildReplayRWNodes、BuildReplayContexts
  - `utils/replay_executor_client.go`: HTTPClient 实现 ReplayExecutor
  - `utils/replay_metrics.go`: BlockMetrics 分阶段计时
  - `core/rwset_builder.go`: TransactionContext 追加 Replay 字段
- **Acceptance Criteria Addressed**: AC-5
- **Test Requirements**:
  - `programmatic` TR-6.1: `go build ./...` 通过
  - `programmatic` TR-6.2: AcctKey/SlotKey 格式与 witness.go 生成的 key 严格一致
  - `programmatic` TR-6.3: HTTPClient 发送 JSON 请求到 eth-replayd 并正确解析响应
  - `programmatic` TR-6.4: BuildReplayRWNodes 生成的 RWNode 列表 Label 正确（r/w），ContractName=""

## [ ] Task 7: Replay Serial + CG + Nezha 算法入口
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Description**:
  - test.go 新增 CLI flag: -dataset、-replayFrom、-replayTo、-replaydAddr
  - 新增 runReplayMode()、TestReplaySerial()、TestReplayCG()、TestReplayNezha()
  - Serial: 无调度直接按序执行
  - CG: classical_graph.go 调度后并行执行
  - Nezha: conflict_queue.go 调度后并行执行
  - 每块从同一 BlockWitness 重新初始化，保证公平
- **Acceptance Criteria Addressed**: AC-6
- **Test Requirements**:
  - `programmatic` TR-7.1: TestReplaySerial 对 10 块全部通过，AbortCount=0，WitnessMissCount=0
  - `programmatic` TR-7.2: TestReplayCG 对 10 块全部通过
  - `programmatic` TR-7.3: TestReplayNezha 对 10 块全部通过
  - `programmatic` TR-7.4: Nezha 最终状态与 Serial 最终状态 byte 级相等
  - `programmatic` TR-7.5: 输出 metrics CSV，T_execute/T_total 分栏完整
  - `programmatic` TR-7.6: 零 Alchemy API 调用（完全本地加载）

## [ ] Task 8: Replay NezhaVariable + Vegeta 算法入口
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - 新增 TestReplayNezhaVariable()、TestReplayVegeta()
  - Vegeta 实现批级快照隔离：每个 batch 开始前 clone committedState 到 snapshot
  - orderedTxs 字典序排列（replay 下 txIndex 已有序，仍显式按 TxID 排）
  - fromAddr 直接来自真实交易（天然确定性）
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `programmatic` TR-8.1: TestReplayNezhaVariable 对 10 块全部通过
  - `programmatic` TR-8.2: TestReplayVegeta 对 10 块全部通过，AbortRate 合理（1–5%）
  - `programmatic` TR-8.3: Vegeta 相同 dataset 连续运行 3 次，AbortCount/WitnessMissCount 完全一致
  - `programmatic` TR-8.4: 输出 metrics CSV，T_execute/T_total 分栏完整

## [ ] Task 9: 1,000 块扩展 + 全区间导出
- **Priority**: medium
- **Depends On**: Task 7, Task 8
- **Description**:
  - exporter 扩展到 1,000 块（24,000,000–24,000,999）
  - 全区间 10,000 块（24,000,000–24,010,000）
  - checkpoint/resume 确保可断点续传
  - 每 100 块为 chunk
- **Acceptance Criteria Addressed**: AC-2, AC-8
- **Test Requirements**:
  - `programmatic` TR-9.1: 1,000 块导出成功，manifest 正确
  - `programmatic` TR-9.2: 100 块随机抽样 self-test 通过
  - `programmatic` TR-9.3: 10,000 块全区间导出成功
  - `programmatic` TR-9.4: 5 种算法 × 1000 块跑一次，metrics 输出完整

## [ ] Task 10: 3 次重复实验 + 指标报告
- **Priority**: medium
- **Depends On**: Task 9
- **Description**:
  - 每种算法 × 每块 × 3 次重复
  - 固定 GOMAXPROCS
  - 汇总 plan.md §12 指标：TPS_execute、TPS_end_to_end、AbortRate、WitnessMissRate、RWSetExpansionRate、P50/P95 latency
  - 输出 CSV + summary
- **Acceptance Criteria Addressed**: AC-7, AC-8
- **Test Requirements**:
  - `programmatic` TR-10.1: 3 次重复实验全部通过
  - `programmatic` TR-10.2: metrics CSV 包含所有分项指标
  - `programmatic` TR-10.3: T_execute 与 T_total 分离
  - `human-judgement` TR-10.4: 指标 summary 清晰可读，各算法 TPS 差异合理
