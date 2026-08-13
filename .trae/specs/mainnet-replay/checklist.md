# 主网真实交易 EVM 回放 - Verification Checklist

## 模块隔离与编译
- [ ] Checkpoint 1: go.work 创建成功，包含三个模块引用
- [ ] Checkpoint 2: 主模块 `go build ./...` 通过（现有合成 benchmark 路径行为不变）
- [ ] Checkpoint 3: cmd/eth-dataset-exporter `go build .` 通过
- [ ] Checkpoint 4: cmd/eth-replayd `go build .` 通过

## Dataset Exporter
- [ ] Checkpoint 5: RPC 客户端能正确调用 eth_getBlockByNumber
- [ ] Checkpoint 6: debug_traceTransaction (prestateTracer) 返回非空 Accounts map
- [ ] Checkpoint 7: debug_traceTransaction (diffMode) 返回变化量
- [ ] Checkpoint 8: 429 限流时指数退避重试
- [ ] Checkpoint 9: 导出 10 块后 manifest.json 字段正确（fromBlock/toBlock/sourceClient）
- [ ] Checkpoint 10: blocks/*.json.zst 解压后结构完整（header/transactions/witness/canonical/rwsets）
- [ ] Checkpoint 11: witness.accounts 非空（≥50 账户/块）
- [ ] Checkpoint 12: rwsets 非空，readKeys 和 writeKeys 均非空
- [ ] Checkpoint 13: code/ 目录按 codeHash 去重存储
- [ ] Checkpoint 14: checkpoint 断点续传可用

## eth-replayd 正确性
- [ ] Checkpoint 15: SparseStateDB 正确返回 witness 中存在的账户余额
- [ ] Checkpoint 16: SparseStateDB 读取不存在的 key 时返回 WitnessMiss（不返回零值）
- [ ] Checkpoint 17: BlockEnv 的 BaseFee/PrevRandao/Timestamp 正确填充
- [ ] Checkpoint 18: Self-test 10 块串行重放，每笔 Status == 原始 receipt.Status
- [ ] Checkpoint 19: Self-test 10 块串行重放，每笔 GasUsed == 原始 receipt.GasUsed
- [ ] Checkpoint 20: Self-test 10 块串行重放，Logs 数量和顺序一致
- [ ] Checkpoint 21: 人为删除 witness slot 后 self-test 返回 non-empty Missed 并 abort
- [ ] Checkpoint 22: ExecTimeNs 记录为纯 EVM 执行耗时（不含 IPC/IO）
- [ ] Checkpoint 23: HTTP 服务正确响应 LoadBlock/PreExecute/Execute 命令

## 主模块 Replay 适配
- [ ] Checkpoint 24: core/replay_types.go 编译通过
- [ ] Checkpoint 25: core/replay_rw_keys.go 的 AcctKey/SlotKey 与 exporter 格式一致
- [ ] Checkpoint 26: utils/replay_dataset.go 能正确加载本地 dataset
- [ ] Checkpoint 27: utils/replay_adapter.go 的 BuildReplayRWNodes 生成的 RWNode Label 正确
- [ ] Checkpoint 28: utils/replay_executor_client.go 的 HTTPClient 正确与 eth-replayd 通信
- [ ] Checkpoint 29: TransactionContext.Replay 字段正确设置，synthetic 模式为 nil
- [ ] Checkpoint 30: BlockMetrics 分阶段计时正确

## 算法 Replay 入口
- [ ] Checkpoint 31: TestReplaySerial 对 10 块全部通过，AbortCount=0
- [ ] Checkpoint 32: TestReplayCG 对 10 块全部通过
- [ ] Checkpoint 33: TestReplayNezha 对 10 块全部通过
- [ ] Checkpoint 34: Nezha 最终状态与 Serial 最终状态 byte 级相等
- [ ] Checkpoint 35: TestReplayNezhaVariable 对 10 块全部通过
- [ ] Checkpoint 36: TestReplayVegeta 对 10 块全部通过，AbortRate 合理（1-5%）
- [ ] Checkpoint 37: Vegeta 相同 dataset 连续 3 次运行，AbortCount 完全一致
- [ ] Checkpoint 38: 每种算法输出 metrics CSV，T_execute/T_total 分栏完整
- [ ] Checkpoint 39: Replay 模式零 Alchemy API 调用（完全本地加载）

## 扩展与重复实验
- [ ] Checkpoint 40: 1,000 块导出成功，self-test 随机 10 块通过
- [ ] Checkpoint 41: 10,000 块全区间导出成功
- [ ] Checkpoint 42: 5 种算法 × 1000 块跑一次，metrics 输出完整
- [ ] Checkpoint 43: 3 次重复实验全部通过
- [ ] Checkpoint 44: 最终 metrics CSV 包含 TPS_execute、TPS_end_to_end、AbortRate、WitnessMissRate、RWSetExpansionRate、P50/P95 latency
