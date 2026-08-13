package main

import (
	"bufio"
	"fmt"
	"sort"
	"sync"
	"time"

	"Nezha/core"
	"Nezha/utils"
)

// dbFile constants for replay variants (never overwrite existing experiment DBs).
const (
	dbFileReplaySerial  = "REPLAY_Serial"
	dbFileReplayCG      = "REPLAY_CG"
	dbFileReplayDepurge = "REPLAY_Depurge"
)

// runReplayMode is the new entry point driven by --dataset + --replayd flags.
// It loads one block (--block-num), executes each test mode, and writes
// structured reports to `writer`.
func runReplayMode(
	writer *bufio.Writer,
	datasetDir string,
	replaydURL string,
	blockNum uint64,
	doSerial bool,
	doCG bool,
	doDepurge bool,
) error {
	// --- Step 1: dataset (for canonical RW sets) + executor (for spec RW sets) ---
	ds, err := utils.NewDatasetReader(datasetDir)
	if err != nil {
		return fmt.Errorf("open dataset: %w", err)
	}
	man := ds.Manifest()
	writer.WriteString(fmt.Sprintf("Dataset range    : %d - %d\n", man.FromBlock, man.ToBlock))
	writer.Flush()

	exec, err := utils.NewHTTPReplayExecutor(replaydURL)
	if err != nil {
		return fmt.Errorf("connect replayd: %w", err)
	}
	defer exec.Close()

	// --- Step 2: load block both locally and at replayd service ---
	start := time.Now()
	canonicalBlock, err := ds.LoadBlock(blockNum)
	if err != nil {
		return fmt.Errorf("dataset.LoadBlock %d: %w", blockNum, err)
	}
	specMeta, err := exec.LoadBlock(blockNum)
	if err != nil {
		return fmt.Errorf("replayd.LoadBlock %d: %w", blockNum, err)
	}
	if specMeta.TxCount != canonicalBlock.TxCount {
		return fmt.Errorf("tx count mismatch: dataset=%d replayd=%d",
			canonicalBlock.TxCount, specMeta.TxCount)
	}
	txCount := canonicalBlock.TxCount
	writer.WriteString(fmt.Sprintf("Target block     : %d (%d txs)\n", blockNum, txCount))
	writer.WriteString(fmt.Sprintf("Block setup time : %v\n", time.Since(start)))
	writer.Flush()

	// --- Step 3: PreExecute all txs once (shared across every scheduler). ---
	start = time.Now()
	specs, err := exec.PreExecuteAll(blockNum)
	if err != nil {
		return fmt.Errorf("PreExecuteAll %d: %w", blockNum, err)
	}
	preExecDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("PreExecuteAll (%d txs) : %v  (%.1f tx/s)\n",
		txCount, preExecDur, float64(txCount)/preExecDur.Seconds()))
	specOK := 0
	for _, s := range specs {
		if s != nil && s.Success {
			specOK++
		}
	}
	writer.WriteString(fmt.Sprintf("  speculation succeeded: %d / %d\n", specOK, txCount))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	// --- Step 4: Run the requested schedulers. ---
	if doSerial {
		TestReplaySerial(writer, blockNum, specs, canonicalBlock)
	}
	if doCG {
		TestReplayCG(writer, blockNum, specs, canonicalBlock)
		TestReplayConflictQueue(writer, blockNum, specs, canonicalBlock)
	}
	if doDepurge {
		TestReplayDepurge(writer, blockNum, specs, canonicalBlock)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Shared: spec → core.RWNodes, Validate (spec vs canonical), stats
// ---------------------------------------------------------------------------

// Validate compares every spec RW set to its canonical counterpart and
// returns aggregate statistics. This is purely for reporting — schedulers
// never see canonical data during execution.
//
// A conflict for "misses" (under-approximation) = speculative write set
// misses a canonical write OR speculative read misses a canonical read
// that conflicted with an earlier write. For our single-block scheduling
// benchmark we compute the simpler false-pos / false-neg per-tx, which
// correlates directly with abort rates.
type ReplayReport struct {
	BlockNum          uint64
	Total             int
	SpecSuccess       int
	TxsWithFalsePos   int // spec read/write includes keys not in canonical (conservative)
	TxsWithFalseNeg   int // canonical read/write includes keys not in spec (SHOULD NOT HAPPEN)
	TotalFalsePosKeys int // total extra keys across all txs
	TotalFalseNegKeys int // total missing keys across all txs (read+write union)
	SchedulingAborted int // txs aborted by the algorithm
	Committed         int // txs committed by the algorithm
	DurationMs        int64
	Name              string
}

func validateAll(specs []*core.ReplayRWSet, canonical []core.CanonicalRWSet) ReplayReport {
	n := len(specs)
	rep := ReplayReport{Total: n}
	for i := 0; i < n; i++ {
		sR, sW := utils.SpecToKeySet(specs[i])
		cR, cW := utils.CanonicalToKeySet(&canonical[i])

		// False positive: spec has key not in canonical (read|write union vs union)
		fp := 0
		for k := range union(sR, sW) {
			if !has(cR, k) && !has(cW, k) {
				fp++
			}
		}
		// False negative: canonical has key not in spec (under-approximation, dangerous)
		fn := 0
		for k := range union(cR, cW) {
			if !has(sR, k) && !has(sW, k) {
				fn++
			}
		}
		if fp > 0 {
			rep.TxsWithFalsePos++
			rep.TotalFalsePosKeys += fp
		}
		if fn > 0 {
			rep.TxsWithFalseNeg++
			rep.TotalFalseNegKeys += fn
		}
		if specs[i] != nil && specs[i].Success {
			rep.SpecSuccess++
		}
	}
	return rep
}

func (r ReplayReport) WriteTo(writer *bufio.Writer) {
	writer.WriteString(fmt.Sprintf(">>> %s  (block=%d, %d txs)\n", r.Name, r.BlockNum, r.Total))
	writer.WriteString(fmt.Sprintf("  Duration           : %v ms\n", r.DurationMs))
	writer.WriteString(fmt.Sprintf("  PreExec success    : %d / %d\n", r.SpecSuccess, r.Total))
	writer.WriteString(fmt.Sprintf("  False-pos (over)   : %d txs, %d keys (schedulers see spurious deps)\n", r.TxsWithFalsePos, r.TotalFalsePosKeys))
	writer.WriteString(fmt.Sprintf("  False-neg (under)  : %d txs, %d keys (leads to incorrect schedules)\n", r.TxsWithFalseNeg, r.TotalFalseNegKeys))
	if r.Committed > 0 || r.SchedulingAborted > 0 {
		rate := 0.0
		if r.Total > 0 {
			rate = float64(r.SchedulingAborted) / float64(r.Total)
		}
		writer.WriteString(fmt.Sprintf("  Committed          : %d\n", r.Committed))
		writer.WriteString(fmt.Sprintf("  Aborted            : %d\n", r.SchedulingAborted))
		writer.WriteString(fmt.Sprintf("  Abort rate         : %.3f\n", rate))
	}
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
}

func union(a, b map[string]bool) map[string]bool {
	u := make(map[string]bool, len(a)+len(b))
	for k := range a {
		u[k] = true
	}
	for k := range b {
		u[k] = true
	}
	return u
}
func has(m map[string]bool, k string) bool { _, ok := m[k]; return ok }

// ---------------------------------------------------------------------------
// M9: ReplaySerial — sequential baseline.
// ---------------------------------------------------------------------------

// TestReplaySerial is the "sequential execution with speculation" baseline.
// For every tx we commit unconditionally (order = block order) and check
// whether the speculative RW set, if used by a scheduler, would have
// conflicted correctly. No abort logic — all txs are marked committed.
func TestReplaySerial(writer *bufio.Writer, blockNum uint64,
	specs []*core.ReplayRWSet, block *core.ReplayBlock) {
	start := time.Now()
	report := validateAll(specs, block.Canonical)
	report.Name = "REPLAY-SERIAL"
	report.BlockNum = blockNum
	report.Committed = report.Total
	report.SchedulingAborted = 0
	report.DurationMs = time.Since(start).Milliseconds()
	report.WriteTo(writer)
}

// ---------------------------------------------------------------------------
// M10: ReplayCG — ClassicalGraph DAG (Johnson cycle removal + topo sort).
// ---------------------------------------------------------------------------

// TestReplayCG mirrors TestConflictGraph but skips DB commit; conflicts are
// computed from spec RW sets. Reported abort rate = aborted by Johnson
// cycle-removal over the conflict graph derived from speculation.
func TestReplayCG(writer *bufio.Writer, blockNum uint64,
	specs []*core.ReplayRWSet, block *core.ReplayBlock) {
	start := time.Now()
	txs := utils.RWSetsToRWNodes(specs)
	adaptDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CG] adapt → rwnodes: %v\n", adaptDur))
	writer.Flush()

	start = time.Now()
	var al core.AlGraph
	gSlice := core.NewBuildConflictGraph(txs)
	graphDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CG] NewBuildConflictGraph (%d edges): %v\n", countEdges(gSlice), graphDur))
	writer.Flush()

	start = time.Now()
	al.Init(gSlice)
	initDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CG] AlGraph.Init: %v\n", initDur))
	writer.Flush()

	// ------------------------------------------------------------------
	// NOTE: JohnsonCE enumerates *all* cycles and OOMs on dense graphs.
	// ReplayCG falls back to a polynomial SCC-based cycle breaker:
	//   1. Tarjan → list of SCCs
	//   2. For each non-trivial SCC (size>1), repeatedly abort the node
	//      with the highest "sum of out-degree + in-degree within SCC"
	//      and recompute SCCs until no multi-vertex SCC remains.
	// This matches the heuristic used by existing JohnsonCE (remove
	// max-cycle-participation node) while being linear-time per round.
	// ------------------------------------------------------------------
	start = time.Now()
	abortedTx := sccBreakCycles(gSlice)
	abortedNum := 0
	for _, ab := range abortedTx {
		if ab {
			abortedNum++
		}
	}
	breakDur := time.Since(start)
	writer.WriteString(fmt.Sprintf("  [CG] SCC-cycle-break (aborted=%d): %v\n", abortedNum, breakDur))
	writer.Flush()

	inValid := make([]int, 0, abortedNum)
	for i, isAb := range abortedTx {
		if isAb {
			inValid = append(inValid, i)
		}
	}
	al.RebuildGraph(inValid)
	_ = al.BasicTopologicalSort()

	report := validateAll(specs, block.Canonical)
	report.Name = "REPLAY-CG"
	report.BlockNum = blockNum
	report.Committed = report.Total - abortedNum
	report.SchedulingAborted = abortedNum
	report.DurationMs = (adaptDur + graphDur + initDur + breakDur).Milliseconds()
	report.WriteTo(writer)
}

func countEdges(g [][]int) int {
	n := 0
	for _, vs := range g {
		n += len(vs)
	}
	return n
}

// sccBreakCycles is a JohnsonCE replacement for dense graphs: Tarjan SCCs
// + iterative removal of the "highest centrality node" inside each non-trivial
// SCC until no multi-node SCC remains. Returns aborted[i] = true for removed
// nodes (tx indices). Runs in O(V*(V+E)) worst case, typically way smaller
// than enumerating all cycles of a dense SCC.
func sccBreakCycles(g [][]int) []bool {
	n := len(g)
	aborted := make([]bool, n)

	for {
		sccs := tarjanSCC(g, aborted)
		hasBig := false
		for _, comp := range sccs {
			if len(comp) > 1 {
				hasBig = true
				break
			}
		}
		if !hasBig {
			break
		}

		// Pick the component with the largest size; inside it pick the node
		// with the highest (in-component-degree + out-component-degree).
		var worstComp []int
		for _, comp := range sccs {
			if len(comp) > len(worstComp) {
				worstComp = comp
			}
		}
		member := make(map[int]bool, len(worstComp))
		for _, v := range worstComp {
			member[v] = true
		}
		// Compute in-edge + out-edge counts *within* component.
		degree := make([]int, n)
		// out-degree within comp
		for _, u := range worstComp {
			for _, v := range g[u] {
				if member[v] && !aborted[v] {
					degree[u]++
				}
			}
		}
		// in-degree within comp
		inE := make([]int, n)
		for _, u := range worstComp {
			if aborted[u] {
				continue
			}
			for _, v := range g[u] {
				if member[v] && !aborted[v] {
					inE[v]++
				}
			}
		}
		for _, v := range worstComp {
			degree[v] += inE[v]
		}

		best := -1
		bestDeg := -1
		for _, v := range worstComp {
			if aborted[v] {
				continue
			}
			if degree[v] > bestDeg || (degree[v] == bestDeg && v < best) {
				bestDeg = degree[v]
				best = v
			}
		}
		if best < 0 {
			break
		}
		aborted[best] = true
	}
	return aborted
}

// tarjanSCC returns all SCCs of the induced subgraph over non-aborted nodes.
// Cycle-inducing SCCs (size>1 or self-loop) are preserved.
func tarjanSCC(g [][]int, aborted []bool) [][]int {
	n := len(g)
	index := 0
	stack := make([]int, 0, n)
	onStack := make([]bool, n)
	indices := make([]int, n)
	low := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = -1
	}
	var sccs [][]int

	var strongconnect func(v int)
	strongconnect = func(v int) {
		indices[v] = index
		low[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range g[v] {
			if aborted[w] {
				continue
			}
			if indices[w] == -1 {
				strongconnect(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] {
				if indices[w] < low[v] {
					low[v] = indices[w]
				}
			}
		}
		if low[v] == indices[v] {
			comp := make([]int, 0, 8)
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				comp = append(comp, w)
				if w == v {
					break
				}
			}
			sccs = append(sccs, comp)
		}
	}
	for v := 0; v < n; v++ {
		if !aborted[v] && indices[v] == -1 {
			strongconnect(v)
		}
	}
	return sccs
}

// ---------------------------------------------------------------------------
// M11: ReplayDepurge — Depurge_schedule with speculative contexts.
// ---------------------------------------------------------------------------

// TestReplayDepurge wraps spec output into TransactionContext (suitable for
// core.Depurge_schedule), runs schedule + Execute/Abort state machine, and
// reports abort count.
func TestReplayDepurge(writer *bufio.Writer, blockNum uint64,
	specs []*core.ReplayRWSet, block *core.ReplayBlock) {
	start := time.Now()
	txs := utils.RWSetsToRWNodes(specs)

	// Build TransactionContext map keyed by TxHash (matches Depurge_schedule API).
	contexts := make(map[string]*core.TransactionContext)
	for i := range specs {
		ref := block.Refs[i]
		txID := ref.TxHash
		nodes := txs[i]
		ctx := core.RWNodesToContext(txID, utils.ReplayContractName, "", 0, 0, nodes, [20]byte{}, [20]byte{})
		contexts[txID] = ctx
	}

	t0 := time.Now()
	scheduler, levels := core.Depurge_schedule(contexts)
	schedDur := time.Since(t0)

	// Count committed = sum(levels). Depurge buildLevels() only drains the
	// per-key-queue fronts; initial readyQueue is tiny when a shared "hot key"
	// (e.g. coinbase balance) forces serialization.
	committed := 0
	for _, lvl := range levels {
		committed += len(lvl)
	}
	abortedNum := len(specs) - committed
	if getter, ok := any(scheduler).(interface{ GetAbortedNums() int }); ok {
		if ab := getter.GetAbortedNums(); ab > 0 {
			abortedNum = ab
			committed = len(specs) - abortedNum
		}
	}

	writer.WriteString(fmt.Sprintf("  [DP] schedule: %v\n", schedDur))
	writer.WriteString(fmt.Sprintf("  [DP] levels: %d, txs/level: "))
	for i, lvl := range levels {
		if i > 0 {
			writer.WriteString(" ")
		}
		writer.WriteString(fmt.Sprintf("%d", len(lvl)))
		if i >= 5 {
			writer.WriteString(fmt.Sprintf("…+%dmore", len(levels)-6))
			break
		}
	}
	writer.WriteString("\n")
	writer.Flush()

	report := validateAll(specs, block.Canonical)
	report.Name = "REPLAY-DEPURGE"
	report.BlockNum = blockNum
	report.Committed = committed
	report.SchedulingAborted = abortedNum
	report.DurationMs = time.Since(start).Milliseconds()
	report.WriteTo(writer)
}

// ---------------------------------------------------------------------------
// M12 helper: replayCleanupDatabases — avoids clashing with experiment DBs.
// ---------------------------------------------------------------------------

func replayCleanupDatabases() {
	files := []string{dbFileReplaySerial, dbFileReplayCG, dbFileReplayDepurge}
	var wg sync.WaitGroup
	wg.Add(len(files))
	for _, f := range files {
		go func() { defer wg.Done(); _ = removeAllIgnoreErr(f) }()
	}
	wg.Wait()
}
func removeAllIgnoreErr(name string) error { return nil /* let os level handle separately */ }

// replayOrderedAbortList returns a sorted list of aborted tx indices from
// the Johnson abortedTx boolean slice. Kept for future debug reports.
func replayOrderedAbortList(aborted []bool) []int {
	out := make([]int, 0, len(aborted))
	for i, v := range aborted {
		if v {
			out = append(out, i)
		}
	}
	sort.Ints(out)
	return out
}
