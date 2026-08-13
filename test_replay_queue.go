package main

import (
	"bufio"
	"fmt"
	"time"

	"Nezha/core"
	"Nezha/utils"
)

// TestReplayConflictQueue 用主网重放的 speculative RW sets 跑 TestConflictQueue
// 的核心调度链：CreateGraph → QueuesSort → DeSS → GetAbortedNums。
//
// 与 TestReplayCG 的区别：
//   - CG 用 NewBuildConflictGraph + sccBreakCycles（ClassicalGraph 路线）
//   - CQ 用 CreateGraph + QueuesSort + DeSS（QueueGraph 路线）
//
// 不跑 commit 到 DB 的步骤——主网重放只关心 abort rate 和调度时间。
func TestReplayConflictQueue(
	writer *bufio.Writer,
	blockNum uint64,
	specs []*core.ReplayRWSet,
	block *core.ReplayBlock,
) {
	start := time.Now()

	// Step 1: spec RW → RWNode
	txs := utils.RWSetsToRWNodes(specs)
	adaptDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CQ] adapt → rwnodes: %v\n", adaptDur))
	writer.Flush()

	// Step 2: CreateGraph — 构建 QueueGraph（Queues + Edges）
	start = time.Now()
	queueGraph := core.CreateGraph(txs)
	graphDur := time.Since(start)
	rwNum := queueGraph.GetRWNums()
	writer.WriteString(fmt.Sprintf("  [CQ] CreateGraph (%d queues, %d rw nodes): %v\n",
		len(queueGraph.Queues), rwNum, graphDur))
	writer.Flush()

	// Step 3: QueuesSort — 拓扑排序得到 queue 执行顺序
	start = time.Now()
	sequence := queueGraph.QueuesSort()
	sortDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CQ] QueuesSort (%d queues ordered): %v\n",
		len(sequence), sortDur))
	writer.Flush()

	// Step 4: DeSS — 确定性排序，标记 isAborted
	start = time.Now()
	commitOrder := queueGraph.DeSS(sequence)
	dessDur := time.Since(start)
	committedLayers := len(commitOrder)
	writer.WriteString(fmt.Sprintf("  [CQ] DeSS (%d commit layers): %v\n",
		committedLayers, dessDur))
	writer.Flush()

	// Step 5: 统计 abort
	abortedNum := queueGraph.GetAbortedNums()

	// Step 6: validate + report
	report := validateAll(specs, block.Canonical)
	report.Name = "REPLAY-CONFLICT-QUEUE"
	report.BlockNum = blockNum
	report.Committed = report.Total - abortedNum
	report.SchedulingAborted = abortedNum
	report.DurationMs = (adaptDur + graphDur + sortDur + dessDur).Milliseconds()
	report.WriteTo(writer)
}
