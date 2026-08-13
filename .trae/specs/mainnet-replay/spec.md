# 主网真实交易 EVM 回放 - Product Requirement Document

## Overview
- **Summary**: 基于 Alchemy PAYG RPC 获取 Ethereum 主网 24,000,000–24,010,000 区间的真实交易、区块环境和稀疏状态（witness），构建本地可重复使用的 dataset，然后用新版 geth EVM 重放交易，测试 Serial/CG/Nezha/NezhaVariable/Vegeta 五种算法的并发执行吞吐。
- **Purpose**: 解决当前 Nezha 框架仅支持合成合约 benchmark、无法验证真实主网交易调度算法有效性的问题。
- **Target Users**: 从事并发 EVM 执行研究、调度算法设计的开发者和研究员。

## Goals
- 构建可重复的主网 dataset exporter（一次导出，多次复用）
- 实现 eth-replayd：新版 geth EVM 的稀疏状态回放服务，支持 WitnessMiss 安全机制
- 实现 ReplayExecutor 接口，将现有算法（Serial/CG/Nezha/NezhaVariable/Vegeta）适配到 replay 模式
- 输出分阶段性能指标：TPS_execute、TPS_end_to_end、AbortRate、WitnessMissRate、RWSetExpansionRate、P50/P95 block latency

## Non-Goals (Out of Scope)
- 不导入并维护连续的完整 Ethereum 世界状态（仅每块独立稀疏状态）
- 不以主网 stateRoot 一致为目标
- 不实现基于源码/ABI/StorageLayout 的 Depurge-LLM 路径（plan.md §9：主网缺源码）
- 不修改现有 levm/旧 geth vendored 代码（用 go.work 隔离）
- 不实现完整的 archive Geth 自建（用 Alchemy PAYG）

## Background & Context
- 现有代码库：Go 1.18 + vendored old geth (~v1.9 pre-London)，仅支持合成合约 benchmark
- 新模块用 go.work 多模块隔离，独立 go.mod 引入新版 geth v1.14+
- 数据源：Alchemy PAYG（debug_traceTransaction 40 CU/次，eth_getBlockByNumber 20 CU/次）
- 目标区块区间：24,000,000–24,010,000（post-Pectra/post-Fusaka，含 EIP-7702 blob 交易）

## Functional Requirements
- **FR-1**: 构建 `cmd/eth-dataset-exporter`：从 Alchemy RPC 获取指定区块区间的 block/tx/receipt + prestateTracer 导出的 witness + RWSet，按 plan.md §4 格式写入 zstd 压缩的 JSON 文件
- **FR-2**: 构建 `cmd/eth-replayd`：加载 dataset，用新版 geth 的 sparse StateDB + BlockEnv 串行重放每笔交易，返回 receipt/status/gasUsed/真实 RWSet，支持 WitnessMiss 检测
- **FR-3**: 新增 `core/replay_types.go`：ReplayRef、ReplayRWSet、ExecuteResult、ReplayExecutor 接口
- **FR-4**: 新增 `core/replay_rw_keys.go`：RW key 格式工具（acct:addr:field / slot:addr:slot）
- **FR-5**: 新增 `utils/replay_dataset.go`：加载本地 dataset 块数据
- **FR-6**: 新增 `utils/replay_adapter.go`：将 ReplayRWSet 转换为现有 RWNode/TransactionContext
- **FR-7**: 新增 `utils/replay_executor_client.go`：ReplayExecutor 的 HTTP/socket 客户端实现
- **FR-8**: 新增 `utils/replay_metrics.go`：分阶段计时指标收集
- **FR-9**: 为 test.go 添加 replay 模式 CLI flag 和 5 种算法的 replay 版入口
- **FR-10**: 实现 WitnessMiss 硬约束：读不存在的 account/code/storage 时不返回零值，标记 miss 并 abort

## Non-Functional Requirements
- **NFR-1**: 模块隔离：go.work 确保主模块（go 1.18，旧 geth）与新模块（go 1.22，新 geth）完全不交叉
- **NFR-2**: 本地存储：dataset 以 zstd 压缩存储，10,000 块约 1.1 GB，可重复加载
- **NFR-3**: 正确性：串行 canonical 重放的 receipt.Status/GasUsed/Logs 必须与 archive 原始 receipt 100% 一致
- **NFR-4**: 安全性：WitnessMiss 不得静默返回零值，必须显式标记并 abort
- **NFR-5**: 可重复：同一 dataset + 固定 GOMAXPROCS，3 次重复实验结果稳定
- **NFR-6**: 限流友好：exporter 对 Alchemy 429 响应指数退避

## Constraints
- **Technical**: Go 1.22+ 必须安装用于新模块；主模块保持 go 1.18 不动
- **Business**: Alchemy PAYG 分阶段付费（$5 起步，后续按需追加）
- **Dependencies**: Alchemy RPC endpoint、`github.com/ethereum/go-ethereum v1.14+`、`github.com/klauspost/compress/zstd`
- **External**: 目标区块 24M+ 已 post-Pectra，需支持 EIP-7702 授权列表和 blob 交易

## Assumptions
1. 用户已有 Alchemy PAYG key（充值 ≥$5）
2. Go 1.22+ 已安装（或可通过 brew install go@1.22 安装）
3. 磁盘空间 ≥ 15 GB
4. 网络可访问 eth-mainnet.g.alchemy.com:443
5. 本阶段不接 Depurge 算法（后期单独处理）

## Acceptance Criteria

### AC-1: go.work 脚手架可编译
- **Given**: 仓库根目录存在 go.work，引用 ./ 和 ./cmd/eth-dataset-exporter 和 ./cmd/eth-replayd
- **When**: 执行 `go work sync` 和主模块 `go build ./...`
- **Then**: 三个模块均编译通过，主模块现有合成 benchmark 路径行为不变
- **Verification**: `programmatic`

### AC-2: Dataset exporter 导出 10 块成功
- **Given**: Alchemy API key 已配置，区块区间为 24,000,000–24,000,009
- **When**: 运行 eth-dataset-exporter 导出 10 块
- **Then**: 本地 datasets/ 目录下存在 manifest.json + 10 个 blocks/*.json.zst + code/*.bin.zst；manifest.fromBlock=24000000，toBlock=24000009；witness.accounts 非空；rwsets 非空
- **Verification**: `programmatic`

### AC-3: Serial canonical 重放正确性
- **Given**: 已导出 10 块 dataset
- **When**: eth-replayd 自检验收模式串行重放 10 块所有交易
- **Then**: 每笔交易的 Status == 原始 receipt.Status，GasUsed == 原始 receipt.GasUsed，Logs 数量和顺序一致；0 个 WitnessMiss，0 个 abort
- **Verification**: `programmatic`

### AC-4: WitnessMiss 硬约束
- **Given**: 已导出的 dataset，人为删除某笔交易 witness 中的一个 storage slot
- **When**: eth-replayd 重放该交易
- **Then**: ExecuteResult.Missed 非空，交易被标记为 abort，不返回零值状态
- **Verification**: `programmatic`

### AC-5: ReplayExecutor 接口可用
- **Given**: 主模块已新增 replay_types.go，eth-replayd 服务已启动
- **When**: HTTPClient 实现 LoadBlock/PreExecute/Execute
- **Then**: 三个方法均正确与 eth-replayd JSON 通信，返回预期的 RWSet/ExecuteResult
- **Verification**: `programmatic`

### AC-6: Nezha replay 模式运行
- **Given**: eth-replayd 已启动，10 块 dataset 已加载
- **When**: 运行 `go run test.go -dataset ... -Nezha=true -replayFrom 24000000 -replayTo 24000009`
- **Then**: 输出 metrics CSV，包含 T_execute/T_total/AbortRate 等指标；与 Serial 最终状态 byte 级相等
- **Verification**: `programmatic`

### AC-7: Vegeta 批级快照隔离确定性
- **Given**: Vegeta replay 模式已实现
- **When**: 相同 dataset 连续运行 3 次
- **Then**: AbortCount、WitnessMissCount 完全一致（批级快照隔离消除非确定性）
- **Verification**: `programmatic`

### AC-8: Dataset 可重复使用
- **Given**: 已导出的 10 块 dataset
- **When**: 重复运行任意算法 replay 模式
- **Then**: 零 Alchemy API 调用（完全从本地加载），结果稳定
- **Verification**: `programmatic`

## Open Questions
- [ ] Alchemy 免费档是否包含 debug_traceTransaction？（已确认：不含，必须 PAYG）
- [ ] 新版 geth prestateTracer 对 EIP-7702 authorizationList 账户的覆盖情况如何？（需阶段 2 验证）
