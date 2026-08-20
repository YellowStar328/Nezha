package main

import (
	"bufio"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Nezha/core"
	"Nezha/evm/levm"
	"Nezha/utils"

	"github.com/panjf2000/ants"
)

// dbFile constants for replay variants (never overwrite existing experiment DBs).
const (
	dbFileReplaySerial  = "REPLAY_Serial"
	dbFileReplayCG      = "REPLAY_CG"
	dbFileReplayDepurge = "REPLAY_Depurge"
	dbFileReplayVegeta  = "REPLAY_Vegeta"
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
	writer.WriteString(fmt.Sprintf("  [DP] levels: %d, txs/level: ", len(levels)))
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
		f := f // capture loop variable
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

// ---------------------------------------------------------------------------
// M13: ReplayDepurge (pure-levm path, no HTTP)
//
// runReplayDepurgeMode is the entry point for `--replay-depurge`. It runs the
// full Depurge algorithm on a mainnet block using:
//   - LLM static analysis (LLMSpecExecuteAll) for conservative RW sets
//   - In-process levm (LevmSpecFallback) for fallback PreExecute + re-execution
//   - Witness state for committedState initialization
//
// Output format matches test.go's TestDepurge so results are comparable.
// ---------------------------------------------------------------------------

// runReplayDepurgeMode runs the pure-levm Depurge on a single mainnet block.
// No HTTP, no replayd. All execution happens in-process via LevmSpecFallback.
func runReplayDepurgeMode(
	writer *bufio.Writer,
	datasetDir string,
	fromBlock, toBlock uint64,
	stateLatency time.Duration,
	diskCommit bool,
	trieDisk bool,
	trieCacheMB int,
) error {
	// --- Step 1: open dataset + load block range ---
	ds, err := utils.NewDatasetReader(datasetDir)
	if err != nil {
		return fmt.Errorf("open dataset: %w", err)
	}
	man := ds.Manifest()
	writer.WriteString(fmt.Sprintf("Dataset range    : %d - %d\n", man.FromBlock, man.ToBlock))
	writer.Flush()

	block, err := ds.LoadBlockRange(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("dataset.LoadBlockRange [%d,%d]: %w", fromBlock, toBlock, err)
	}
	rawTxs, err := ds.LoadBlockRangeTxs(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("dataset.LoadBlockRangeTxs [%d,%d]: %w", fromBlock, toBlock, err)
	}
	txCount := block.TxCount
	if fromBlock == toBlock {
		writer.WriteString(fmt.Sprintf("Target block     : %d (%d txs)\n", fromBlock, txCount))
	} else {
		writer.WriteString(fmt.Sprintf("Target blocks    : %d - %d (%d txs)\n", fromBlock, toBlock, txCount))
	}
	writer.Flush()

	// --- Step 2: MainnetContractManager (LLM cache) ---
	mgr := utils.NewMainnetContractManager()
	if err := mgr.LoadAll(); err != nil {
		fmt.Printf("Warning: MainnetContractManager.LoadAll: %v (all txs will fall back to EVM)\n", err)
	}

	// --- Step 3: LevmSpecFallbackPool (NumCPU workers, each with own levm + witness) ---
	// Each worker has its own independent levm + leveldb + witness-injected
	// stateDB, so PreExecute can run concurrently with NO shared-state lock
	// contention. This mirrors the LLMCaptureRWSet pattern (one levm per tx
	// fallback) but reuses workers across txs to amortize levm.New + witness
	// injection cost (~100ms each).
	//
	// With --disk-trie the witness is encoded once as a REAL Merkle Patricia
	// Trie on a shared on-disk leveldb; every worker reads through its own
	// fresh (empty-cache) StateDB, so node loads miss the trie cache and hit
	// the disk (genuine cold reads, no simulation).
	var levmPool *utils.LevmSpecFallbackPool
	if trieDisk {
		levmPool, err = utils.NewLevmSpecFallbackPoolTrie(ds, fromBlock, toBlock, 0, trieCacheMB)
		if err != nil {
			return fmt.Errorf("NewLevmSpecFallbackPoolTrie: %w", err)
		}
	} else {
		levmPool, err = utils.NewLevmSpecFallbackPool(ds, fromBlock, toBlock, 0) // 0 → runtime.NumCPU()
		if err != nil {
			return fmt.Errorf("NewLevmSpecFallbackPool: %w", err)
		}
		// Simulate trie cold-read latency for the parallel pool (own simulator,
		// so the serial baseline below starts from a cold cache as well).
		levmPool.SetAccessLatency(levm.NewAccessLatencySimulator(stateLatency))
	}
	defer levmPool.Close()
	writer.Flush()

	// --- Step 4: LLM static analysis → conservative RW sets ---
	// LLMSpecExecuteAllWithStats auto-detects BatchPreExecutor and runs
	// EVM fallbacks concurrently across the pool.
	startPreAnalysis := time.Now()
	specExec := utils.NewLLMSpecExecutor(mgr, levmPool)
	specs, stats, err := specExec.LLMSpecExecuteAllWithStats(ds, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("LLMSpecExecuteAll: %w", err)
	}
	preAnalysisDur := time.Since(startPreAnalysis)

	specOK := 0
	for _, s := range specs {
		if s != nil && s.Success {
			specOK++
		}
	}
	writer.WriteString(fmt.Sprintf("Time of pre-analysis: %v\n", preAnalysisDur))
	writer.WriteString(fmt.Sprintf("  LLM cache hits : %d\n", stats.LLMHit))
	writer.WriteString(fmt.Sprintf("  EVM fallbacks  : %d (ok=%d)\n", stats.FallbackCount, stats.FallbackOK))
	writer.WriteString(fmt.Sprintf("  speculation ok : %d / %d\n", specOK, txCount))
	// Fallback reason breakdown — explains why LLM cache isn't being used
	// even when cache/mainnet_rw/<addr>/<selector>.json exists.
	if len(stats.FallbackReasons) > 0 {
		writer.WriteString("  Fallback reasons:\n")
		// Deterministic order for reproducible reports.
		type kv struct {
			k string
			v int
		}
		reasons := make([]kv, 0, len(stats.FallbackReasons))
		for k, v := range stats.FallbackReasons {
			reasons = append(reasons, kv{k, v})
		}
		sort.Slice(reasons, func(i, j int) bool { return reasons[i].k < reasons[j].k })
		for _, r := range reasons {
			writer.WriteString(fmt.Sprintf("    %-20s %d\n", r.k, r.v))
		}
	}
	llmCacheCount := listLLMCacheFileCount()
	fbDenom := stats.FallbackCount
	if fbDenom < 1 {
		fbDenom = 1
	}
	writer.WriteString(fmt.Sprintf("  NOTE: cache/mainnet_rw/ has %d contract dirs;\n"+
		"         EVM fallback runs concurrently via LevmSpecFallbackPool (×%d workers),\n"+
		"         wall time %v for %d fallbacks (~%.2f ms/tx amortized).\n",
		llmCacheCount,
		runtime.NumCPU(),
		preAnalysisDur,
		stats.FallbackCount,
		float64(preAnalysisDur.Milliseconds())/float64(fbDenom)))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	// --- Step 5: build TransactionContexts from specs ---
	// PreExecute-failed txs (specs[i]==nil || !Success) have no usable
	// conservative RW set — feeding them to Depurge only produces spurious
	// re-execution errors that would abort anyway. Skip the scheduler for
	// them and route them directly to the serial replay list, so Depurge
	// only schedules txs that actually have a speculative RW set.
	txs := utils.RWSetsToRWNodes(specs)
	contexts := make(map[string]*core.TransactionContext, txCount)
	txIDToIdx := make(map[string]int, txCount)
	var serialReplayList []string // pre-populated with PreExecute-failed txs
	preExecFailedCount := 0
	for i := range specs {
		ref := block.Refs[i]
		txID := ref.TxHash
		txIDToIdx[txID] = i
		if specs[i] == nil || !specs[i].Success {
			// No conservative set → skip Depurge, route straight to serial replay.
			serialReplayList = append(serialReplayList, txID)
			preExecFailedCount++
			continue
		}
		nodes := txs[i]
		ctx := core.RWNodesToContext(txID, utils.ReplayContractName, "", 0, 0, nodes, [20]byte{}, [20]byte{})
		contexts[txID] = ctx
	}
	if preExecFailedCount > 0 {
		writer.WriteString(fmt.Sprintf("PreExec-failed (→ serial): %d (skipped Depurge scheduling)\n", preExecFailedCount))
		writer.Flush()
	}

	// --- Step 6: committedDelta (incremental overlay) ---
	// committedState (full witness map) was removed: it was only ever
	// written, never read — pure dead code. The witness baseline lives in
	// each levm worker's stateDB (injected once at pool init).
	//
	// committedDelta tracks ONLY the keys written by previously-committed
	// txs (NOT the witness baseline). It is the incremental overlay passed
	// to ReExecute so that applyStateOverride only touches prior-tx deltas
	// (typically dozens to a few hundred keys) instead of re-applying the
	// full witness baseline (4875+ keys) every call.
	committedDelta := make(map[string]string)
	var committedDeltaLock sync.RWMutex

	// --- Step 7: Depurge_schedule ---
	startSched := time.Now()
	scheduler, levels := core.Depurge_schedule(contexts)
	schedDur := time.Since(startSched)

	writer.WriteString(fmt.Sprintf("Time of schedule: %v\n", schedDur))
	writer.WriteString(fmt.Sprintf("  Depurge levels: %d, txs/level: ", len(levels)))
	for i, lvl := range levels {
		if i > 0 {
			writer.WriteString(" ")
		}
		writer.WriteString(fmt.Sprintf("%d", len(lvl)))
		if i >= 5 && len(levels) > 6 {
			writer.WriteString(fmt.Sprintf("…+%dmore", len(levels)-6))
			break
		}
	}
	writer.WriteString("\n")
	writer.Flush()

	// --- Step 8: validation loop (concurrent — mirrors test.go's TestDepurge) ---
	//
	// Each ants worker Acquire()s an idle LevmSpecFallback from levmPool, runs
	// ReExecute (which snapshot/reverts internally — safe on a per-worker
	// basis since each worker has its own stateDB), then Release()s it back.
	// This mirrors test.go's InitEVMPool + evmPool.Get/Put + ants pool pattern.
	//
	// DepurgeScheduler methods (PopReady / PopPruneReady / Execute / Abort /
	// Prune / GetConservativeKeys) are all called from goroutines here — they
	// operate on disjoint scheduler state per txID (no cross-tx shared state
	// except the queue/list structures, which are guarded by their own usage
	// pattern: one txID is processed by exactly one worker at a time).
	// committedState updates are guarded by committedStateLock; counters by
	// atomic or abortCountLock.
	startExec := time.Now()
	validationAborted := 0
	var noContextCount, reexecErrorCount, keyExceedCount int32
	var keyExceedLLM, keyExceedPreexec int32 // attribute key-exceed aborts by conservative-key source
	// serialReplayList declared above (Step 5) and pre-populated with
	// PreExecute-failed txs. Appended here for re-execution errors and
	// key-exceed aborts during validation.
	var serialReplayLock sync.Mutex
	var abortCountLock sync.Mutex
	var llmDiffOnce sync.Once     // dump key-diff for the first LLM-sourced abort only
	var preexecDiffOnce sync.Once // dump key-diff for the first preexec-sourced abort only
	totalPrunedKeys := 0
	committed := 0
	var inProgress int32

	validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
		txID := i.(string)
		defer atomic.AddInt32(&inProgress, -1)

		txIdx, ok := txIDToIdx[txID]
		if !ok {
			atomic.AddInt32(&noContextCount, 1)
			abortCountLock.Lock()
			validationAborted++
			abortCountLock.Unlock()
			scheduler.Abort(txID)
			return
		}

		conservativeKeys := scheduler.GetConservativeKeys(txID)
		conservativeKeySet := make(map[string]bool, len(conservativeKeys))
		for _, k := range conservativeKeys {
			conservativeKeySet[k] = true
		}

		// Pass only the incremental overlay (prior-tx deltas), NOT the full
		// committedState. The witness baseline is already in the worker's sdb.
		//
		// Filter the overlay to keys in THIS tx's conservative set: a real key
		// outside the conservative set triggers key-exceed abort → serial
		// replay anyway, so omitting those overlay entries cannot corrupt
		// committed state. This cuts the per-tx clone+apply cost from
		// O(|committedDelta|) to O(|conservativeKeys ∩ committedDelta|).
		committedDeltaLock.RLock()
		overlaySnapshot := filterOverlayByKeys(committedDelta, conservativeKeySet)
		committedDeltaLock.RUnlock()

		worker, err := levmPool.Acquire()
		if err != nil {
			atomic.AddInt32(&reexecErrorCount, 1)
			abortCountLock.Lock()
			validationAborted++
			abortCountLock.Unlock()
			scheduler.Abort(txID)
			serialReplayLock.Lock()
			serialReplayList = append(serialReplayList, txID)
			serialReplayLock.Unlock()
			fmt.Printf("  TX %s: aborted (worker acquire error: %v)\n", txID, err)
			return
		}
		defer levmPool.Release(worker)

		result, err := worker.ReExecute(fromBlock, txIdx, overlaySnapshot)
		if err != nil {
			atomic.AddInt32(&reexecErrorCount, 1)
			abortCountLock.Lock()
			validationAborted++
			abortCountLock.Unlock()
			scheduler.Abort(txID)
			// Per project memory: reexec-fail aborts must also be added to
			// serialReplayList (+ descendants) to maintain state consistency.
			serialReplayLock.Lock()
			serialReplayList = append(serialReplayList, txID)
			serialReplayLock.Unlock()
			fmt.Printf("  TX %s: aborted (re-execution error: %v)\n", txID, err)
			return
		}

		realKeySet := make(map[string]bool, len(result.RealReadKeys)+len(result.RealWriteKeys))
		for _, k := range result.RealReadKeys {
			realKeySet[k] = true
		}
		for _, k := range result.RealWriteKeys {
			realKeySet[k] = true
		}

		// Check: real keys ⊆ conservative keys?
		exceed := false
		for key := range realKeySet {
			if !conservativeKeySet[key] {
				exceed = true
				break
			}
		}

		if exceed {
			atomic.AddInt32(&keyExceedCount, 1)
			// Attribute the abort to the conservative-key source so we
			// can tell LLM-analysis gaps from PreExecute base-state
			// divergence. txIdx is guaranteed valid here (noContextCount
			// path returned earlier).
			isLLM := txIdx >= 0 && txIdx < len(stats.Sources) && stats.Sources[txIdx] == "llm"
			if isLLM {
				atomic.AddInt32(&keyExceedLLM, 1)
			} else {
				atomic.AddInt32(&keyExceedPreexec, 1)
			}
			// Dump the abort's key diff so we can see EXACTLY which keys the
			// conservative set missed AND whether the gap is a format-encoding
			// mismatch vs a real miss. By default only the FIRST abort per
			// source is dumped (keeps logs small); set NEZHA_DUMP_DIFF=1 to
			// dump every abort's full diff.
			if os.Getenv("NEZHA_DUMP_DIFF") != "" {
				dumpAbortKeyDiff(txID, txIdx, isLLM, conservativeKeys, conservativeKeySet, realKeySet)
			} else {
				diffOnce := &llmDiffOnce
				if !isLLM {
					diffOnce = &preexecDiffOnce
				}
				diffOnce.Do(func() {
					dumpAbortKeyDiff(txID, txIdx, isLLM, conservativeKeys, conservativeKeySet, realKeySet)
				})
			}
			abortCountLock.Lock()
			validationAborted++
			abortCountLock.Unlock()
			scheduler.Abort(txID)
			serialReplayLock.Lock()
			serialReplayList = append(serialReplayList, txID)
			serialReplayLock.Unlock()
			var missing []string
			for k := range realKeySet {
				if !conservativeKeySet[k] {
					missing = append(missing, k)
				}
			}
			sort.Strings(missing)
			src := "?"
			if txIdx >= 0 && txIdx < len(stats.Sources) {
				src = stats.Sources[txIdx]
			}
			// Raw tx info for diagnostics.
			toAddr := ""
			selector := ""
			funcName := ""
			if txIdx >= 0 && txIdx < len(rawTxs) {
				toAddr = strings.ToLower(rawTxs[txIdx].To)
				if len(rawTxs[txIdx].Input) >= 10 {
					selector = strings.ToLower(rawTxs[txIdx].Input[:10])
				}
				if toAddr != "" && selector != "" {
					if funcs, ferr := utils.ReadFuncsMap(toAddr); ferr == nil {
						funcName = funcs[selector]
					}
				}
			}
			fmt.Printf("  TX %s (txIdx=%d, to=%s, selector=%s, func=%s): aborted (real keys exceed conservative) - real=%d, conservative=%d, source=%s, missingKeys=%v\n",
				txID, txIdx, toAddr, selector, funcName,
				len(realKeySet), len(conservativeKeySet), src, missing)
			return
		}

		// Prune spurious conservative keys (conservative ⊋ real).
		pruned := 0
		for _, k := range conservativeKeys {
			if !realKeySet[k] {
				pruned++
			}
		}
		abortCountLock.Lock()
		totalPrunedKeys += pruned
		abortCountLock.Unlock()
		if pruned > 0 {
			allRealKeys := make([]string, 0, len(result.RealReadKeys)+len(result.RealWriteKeys))
			allRealKeys = append(allRealKeys, result.RealReadKeys...)
			allRealKeys = append(allRealKeys, result.RealWriteKeys...)
			scheduler.Prune(txID, allRealKeys)
		}

		scheduler.Execute(txID)
		abortCountLock.Lock()
		committed++
		abortCountLock.Unlock()

		// committedDelta stores the absolute post-exec value for each key
		// written by any prior-committed tx, so the next ReExecute's
		// applyStateOverride only touches these keys (small) instead of the
		// full witness baseline (large).
		committedDeltaLock.Lock()
		for k, v := range result.WriteValues {
			committedDelta[k] = v
		}
		committedDeltaLock.Unlock()
	})
	defer validatePool.Release()

	// Termination check order matters: read inProgress FIRST, then the queue
	// lengths. A worker pushes successors to ready/pruneReady inside
	// Execute/Abort/Prune and only afterwards decrements inProgress (defer).
	// If inProgress is observed as 0, every push happened-before this read
	// and no new push can occur (no worker is running), so the subsequent
	// queue-length reads see frozen state and exiting is safe. Checking the
	// queues first opened a window where the last worker pushed a successor
	// and decremented between the two checks — the loop then exited with the
	// successor still queued and the tx was silently dropped (observed: 20
	// txs vanished in a 1961-tx run).
	for {
		if atomic.LoadInt32(&inProgress) == 0 &&
			scheduler.GetReadyQueueLen() == 0 &&
			scheduler.GetPruneReadyQueueLen() == 0 {
			break
		}

		worked := false
		for {
			txID := scheduler.PopReady()
			if txID == "" {
				break
			}
			atomic.AddInt32(&inProgress, 1)
			_ = validatePool.Invoke(txID)
			worked = true
		}

		for {
			txID := scheduler.PopPruneReady()
			if txID == "" {
				break
			}
			atomic.AddInt32(&inProgress, 1)
			_ = validatePool.Invoke(txID)
			worked = true
		}

		// Idle spin: yield the P so workers can finish and push more work,
		// instead of hammering the scheduler RLock in a tight loop.
		if !worked {
			runtime.Gosched()
		}
	}
	execDur := time.Since(startExec)
	writer.WriteString(fmt.Sprintf("Time of execution: %v\n", execDur))

	// --- Step 9: serial replay of aborted txs (sorted by TxID) ---
	sort.Strings(serialReplayList)
	serialReplayed := 0

	// Serial replay uses a DEDICATED levm instance with committed state.
	// Unlike the validation path (which uses snapshot/revert to isolate each
	// tx), serial replay executes txs sequentially with state ACCUMULATING
	// between txs (ReExecuteCommit — no revert). This matches the semantics
	// of test.go's serial baseline: later txs see earlier txs' writes.
	//
	// On the FIRST tx we pass the committedDelta snapshot so the levm
	// stateDB starts from the post-validation committed state; subsequent
	// txs just execute on top of the accumulating stateDB.
	//
	// Reuse an idle worker from levmPool instead of creating a dedicated
	// levm — the worker already has the witness baseline injected, so we
	// skip NewLevmSpecFallback + injectWitnessIntoStateDB (~10-20ms saved).
	// Safe because the validation loop is done; no other goroutine uses
	// the pool concurrently at this point.
	//
	// With --disk-commit, serial replay runs on a dedicated on-disk leveldb
	// levm so its end-of-block trie commit pays real disk-flush cost. With
	// --disk-trie the witness is a REAL trie on disk, so serial replay also
	// reads through the real trie path (no simulation).
	//
	// The replayer is constructed LAZILY and BEFORE the timer: with --disk-trie
	// the replayer re-encodes the witness into its own leveldb
	// (BuildWitnessTrie) — a one-time benchmark-preparation cost that the
	// serial baseline likewise excludes from its timer (its construction
	// happens before startSerialBaseline). With zero aborts we skip
	// construction entirely.
	var serialReplayer *utils.LevmSpecFallback
	if len(serialReplayList) > 0 {
		switch {
		case trieDisk:
			serialReplayer, err = utils.NewLevmSpecFallbackTrieDisk(ds, fromBlock, toBlock, trieCacheMB)
			if err != nil {
				return fmt.Errorf("NewLevmSpecFallbackTrieDisk (serial replay): %w", err)
			}
			defer serialReplayer.Close()
		case diskCommit:
			serialReplayer, err = utils.NewLevmSpecFallbackDisk(ds, fromBlock, toBlock)
			if err != nil {
				return fmt.Errorf("NewLevmSpecFallbackDisk (serial replay): %w", err)
			}
			defer serialReplayer.Close()
		default:
			serialReplayer, err = levmPool.Acquire()
			if err != nil {
				return fmt.Errorf("levmPool.Acquire (serial replay): %w", err)
			}
			defer levmPool.Release(serialReplayer)
		}
	}
	startSerial := time.Now()

	// First tx: apply committedDelta overlay via ReExecuteCommit (with delta
	// computation + writeback). Subsequent txs use PreExecuteCommit (no delta,
	// no overlay — stateDB already carries accumulated writes). This skips
	// collectDeltasAndValues for 106/107 txs, saving ~0.48ms/tx.
	var firstDone bool
	for _, txID := range serialReplayList {
		txIdx, ok := txIDToIdx[txID]
		if !ok {
			continue
		}

		if !firstDone {
			committedDeltaLock.RLock()
			override := cloneStringMap(committedDelta)
			committedDeltaLock.RUnlock()

			result, err := serialReplayer.ReExecuteCommit(fromBlock, txIdx, override)
			if err != nil {
				fmt.Printf("  TX %s: serial replay failed (%v)\n", txID, err)
				firstDone = true
				serialReplayed++
				continue
			}
			if result != nil {
				committedDeltaLock.Lock()
				for k, v := range result.WriteValues {
					committedDelta[k] = v
				}
				committedDeltaLock.Unlock()
			}
			firstDone = true
		} else {
			// Subsequent txs: PreExecuteCommit (no delta, stateDB accumulates).
			// committedDelta writeback is skipped because it is not read after
			// serial replay (phase 10 report only reads counters). The stateDB
			// itself is the source of truth here.
			if _, err := serialReplayer.PreExecuteCommit(fromBlock, txIdx); err != nil {
				fmt.Printf("  TX %s: serial replay failed (%v)\n", txID, err)
			}
		}
		serialReplayed++
	}
	serialDur := time.Since(startSerial)
	// With --disk-commit, pay the real end-of-block trie flush so serial
	// replay reflects the same commit cost as the serial baseline.
	if diskCommit {
		commitDur, cerr := serialReplayer.CommitTrie()
		if cerr != nil {
			fmt.Printf("  WARN: serial replay trie commit failed: %v\n", cerr)
		} else {
			serialDur += commitDur
			writer.WriteString(fmt.Sprintf("  trie commit (serial replay): %v\n", commitDur))
		}
	}
	writer.WriteString(fmt.Sprintf("Time of serial replay: %v\n", serialDur))
	writer.WriteString(fmt.Sprintf("Serial replayed: %d\n", serialReplayed))

	writer.WriteString(fmt.Sprintf("Time of validation and execution: %v\n", time.Since(startExec)))
	writer.WriteString(fmt.Sprintf("Total pruned keys: %d\n", totalPrunedKeys))

	// --- Step 10: report ---
	writer.WriteString(fmt.Sprintf("Validation aborted (total): %d\n", validationAborted))
	writer.WriteString(fmt.Sprintf("  - context not found: %d\n", atomic.LoadInt32(&noContextCount)))
	writer.WriteString(fmt.Sprintf("  - re-execution error: %d\n", atomic.LoadInt32(&reexecErrorCount)))
	writer.WriteString(fmt.Sprintf("  - key exceed (serial replayed): %d\n", atomic.LoadInt32(&keyExceedCount)))
	writer.WriteString(fmt.Sprintf("    - by llm analysis  : %d\n", atomic.LoadInt32(&keyExceedLLM)))
	writer.WriteString(fmt.Sprintf("    - by preexec base : %d\n", atomic.LoadInt32(&keyExceedPreexec)))
	if txCount > 0 {
		writer.WriteString(fmt.Sprintf("Committed: %d, Abort rate is: %.3f\n",
			committed, float64(validationAborted)/float64(txCount)))
	}

	// --- Baseline: serial execution of ALL txs in block order ---
	// Mirrors test.go's TestSerialExecution: same witness starting state,
	// same levm, same EVM call path (NewEVMNoTrace + ReplayTransaction), but
	// executes every tx serially and COMMITS state between txs (no snapshot/
	// revert, no abort, no parallelism). This is the baseline against which
	// Depurge's parallelism gain is measured.
	//
	// Uses a fresh in-memory levm so it doesn't disturb the pool's witness
	// state. We re-inject witness from the dataset.
	depurgeDur := time.Since(startPreAnalysis)

	// Serial baseline: with --disk-commit use a real on-disk leveldb so the
	// end-of-block trie commit pays actual disk-flush cost; with --disk-trie
	// the witness is a REAL trie on disk so the baseline reads through the
	// real trie path; otherwise the in-memory levm (fast, no disk I/O).
	var serialBaseline *utils.LevmSpecFallback
	switch {
	case trieDisk:
		serialBaseline, err = utils.NewLevmSpecFallbackTrieDisk(ds, fromBlock, toBlock, trieCacheMB)
	case diskCommit:
		serialBaseline, err = utils.NewLevmSpecFallbackDisk(ds, fromBlock, toBlock)
	default:
		serialBaseline, err = utils.NewLevmSpecFallback(ds, fromBlock, toBlock)
	}
	startSerialBaseline := time.Now()
	if err != nil {
		return fmt.Errorf("NewLevmSpecFallback (serial baseline): %w", err)
	}
	// Serial baseline pays the same cold-read simulation from its OWN cold
	// cache (independent simulator), so both sides face identical state-access
	// costs and only the latency-overlap differs.
	serialBaseline.SetAccessLatency(levm.NewAccessLatencySimulator(stateLatency))
	defer serialBaseline.Close()

	serialOK, serialFail := 0, 0
	// Diagnostic: split baseline timing into abort-tx vs committed-tx subsets
	// to verify the hypothesis that abort txs are inherently heavier (more
	// storage access → key exceed → abort).
	abortTxSet := make(map[int]bool, len(serialReplayList))
	for _, txID := range serialReplayList {
		if idx, ok := txIDToIdx[txID]; ok {
			abortTxSet[idx] = true
		}
	}
	var abortTxDur, committedTxDur time.Duration
	for txIdx := 0; txIdx < txCount; txIdx++ {
		// Execute WITHOUT snapshot/revert — commit state between txs so
		// later txs see earlier txs' writes. This is the true serial baseline.
		t0 := time.Now()
		rw, err := serialBaseline.PreExecuteCommit(fromBlock, txIdx)
		dur := time.Since(t0)
		if abortTxSet[txIdx] {
			abortTxDur += dur
		} else {
			committedTxDur += dur
		}
		if err != nil || rw == nil || !rw.Success {
			serialFail++
			continue
		}
		serialOK++
	}
	serialBaselineDur := time.Since(startSerialBaseline)
	// With --disk-commit, pay the real end-of-block trie flush so the serial
	// baseline reflects a real node's commit cost (IntermediateRoot + Commit +
	// leveldb flush), not just in-memory state accumulation.
	if diskCommit {
		commitDur, cerr := serialBaseline.CommitTrie()
		if cerr != nil {
			fmt.Printf("  WARN: serial baseline trie commit failed: %v\n", cerr)
		} else {
			serialBaselineDur += commitDur
			writer.WriteString(fmt.Sprintf("  trie commit (serial baseline): %v\n", commitDur))
		}
	}

	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.WriteString(fmt.Sprintf("Baseline: serial execution of all %d txs (commit between txs)\n", txCount))
	writer.WriteString(fmt.Sprintf("Time of serial execution: %v (ok=%d, fail=%d)\n",
		serialBaselineDur, serialOK, serialFail))
	// Diagnostic: per-subset breakdown to explain why serial-replay (abort
	// txs only) takes about as long as serial-baseline (all txs).
	abortCnt := len(abortTxSet)
	committedCnt := txCount - abortCnt
	abortPerTx := float64(0)
	if abortCnt > 0 {
		abortPerTx = float64(abortTxDur.Microseconds()) / float64(abortCnt) / 1000
	}
	committedPerTx := float64(0)
	if committedCnt > 0 {
		committedPerTx = float64(committedTxDur.Microseconds()) / float64(committedCnt) / 1000
	}
	writer.WriteString(fmt.Sprintf("  abort-tx subset   : %d txs, %v (~%.2f ms/tx)\n",
		abortCnt, abortTxDur, abortPerTx))
	writer.WriteString(fmt.Sprintf("  committed-tx subset: %d txs, %v (~%.2f ms/tx)\n",
		committedCnt, committedTxDur, committedPerTx))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Depurge: %v\n", depurgeDur))
	if serialBaselineDur > 0 {
		writer.WriteString(fmt.Sprintf("Speedup (serial / Depurge): %.2fx\n",
			float64(serialBaselineDur)/float64(depurgeDur)))
	}
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	return nil
}

// dumpAbortKeyDiff prints the full key diff (conservative vs real vs missing)
// for a key-exceed abort — used to review exactly which keys the LLM analysis
// (or EVM pre-exec fallback) missed, and whether the gap is a format-encoding
// mismatch or a genuine analysis miss.
func dumpAbortKeyDiff(txID string, txIdx int, isLLM bool, conservativeKeys []string,
	conservativeKeySet, realKeySet map[string]bool) {
	var missing []string
	for k := range realKeySet {
		if !conservativeKeySet[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	src := "preexec"
	if isLLM {
		src = "llm"
	}
	fmt.Printf("=== %s-ABORT DIFF (txID=%s, txIdx=%d) ===\n", src, txID, txIdx)
	fmt.Printf("--- conservative keys (%d) ---\n", len(conservativeKeys))
	for _, k := range conservativeKeys {
		fmt.Printf("    %s\n", k)
	}
	fmt.Printf("--- real keys (%d) ---\n", len(realKeySet))
	for k := range realKeySet {
		fmt.Printf("    %s\n", k)
	}
	fmt.Printf("--- MISSING keys (real but not in conservative, %d) ---\n", len(missing))
	for _, k := range missing {
		fmt.Printf("    %s\n", k)
	}
	fmt.Printf("=== END DIFF ===\n")
}

// runReplayVegetaMode runs the pure-levm Vegeta on a single mainnet block.
// No HTTP, no replayd. All execution happens in-process via LevmSpecFallback.
//
// Key differences from runReplayDepurgeMode:
//   - Conservative RW sets come from EVM PreExecute for ALL txs (no LLM).
//   - Scheduling follows the Vegeta DAG algorithm (not Depurge's ready/prune
//     queues): orderedTxs based on key→txID chains, predecessor conflict
//     detection for DAG edges.
//   - Validation is batch-level: all ready txs in a batch share the same
//     committedDelta snapshot, then successful WriteValues are merged after
//     the batch completes.
//   - No Prune step (Vegeta does not prune spurious conservative keys).
func runReplayVegetaMode(
	writer *bufio.Writer,
	datasetDir string,
	fromBlock, toBlock uint64,
	stateLatency time.Duration,
	diskCommit bool,
	trieDisk bool,
	trieCacheMB int,
) error {
	// --- Step 1: open dataset + load block range ---
	ds, err := utils.NewDatasetReader(datasetDir)
	if err != nil {
		return fmt.Errorf("open dataset: %w", err)
	}
	man := ds.Manifest()
	writer.WriteString(fmt.Sprintf("Dataset range    : %d - %d\n", man.FromBlock, man.ToBlock))
	writer.Flush()

	block, err := ds.LoadBlockRange(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("dataset.LoadBlockRange [%d,%d]: %w", fromBlock, toBlock, err)
	}
	txCount := block.TxCount
	if fromBlock == toBlock {
		writer.WriteString(fmt.Sprintf("Target block     : %d (%d txs)\n", fromBlock, txCount))
	} else {
		writer.WriteString(fmt.Sprintf("Target blocks    : %d - %d (%d txs)\n", fromBlock, toBlock, txCount))
	}
	writer.Flush()

	// --- Step 2: LevmSpecFallbackPool (NumCPU workers) ---
	// With --disk-trie the witness is encoded once as a REAL Merkle Patricia
	// Trie on a shared on-disk leveldb; every worker reads through its own
	// fresh (empty-cache) StateDB, so node loads miss the trie cache and hit
	// the disk (genuine cold reads, no simulation).
	var levmPool *utils.LevmSpecFallbackPool
	if trieDisk {
		levmPool, err = utils.NewLevmSpecFallbackPoolTrie(ds, fromBlock, toBlock, 0, trieCacheMB)
		if err != nil {
			return fmt.Errorf("NewLevmSpecFallbackPoolTrie: %w", err)
		}
	} else {
		levmPool, err = utils.NewLevmSpecFallbackPool(ds, fromBlock, toBlock, 0)
		if err != nil {
			return fmt.Errorf("NewLevmSpecFallbackPool: %w", err)
		}
		// Simulate trie cold-read latency for the parallel pool (own simulator,
		// so the serial baseline below starts from a cold cache as well).
		levmPool.SetAccessLatency(levm.NewAccessLatencySimulator(stateLatency))
	}
	defer levmPool.Close()
	writer.Flush()

	// --- Step 3: EVM PreExecute for ALL txs (no LLM, no fallback) ---
	// Every tx gets its conservative RW set from real EVM execution on the
	// witness base state. This is the key difference from Depurge, which
	// uses LLM static analysis + EVM fallback.
	startPreAnalysis := time.Now()
	txIndices := make([]int, txCount)
	for i := range txIndices {
		txIndices[i] = i
	}
	specs, errs := levmPool.PreExecuteBatch(txIndices)
	preAnalysisDur := time.Since(startPreAnalysis)

	preExecOK := 0
	for _, s := range specs {
		if s != nil && s.Success {
			preExecOK++
		}
	}
	// Check for batch errors — any non-nil error means the whole batch failed.
	var firstErr error
	for _, e := range errs {
		if e != nil && firstErr == nil {
			firstErr = e
		}
	}
	if firstErr != nil {
		return fmt.Errorf("PreExecuteBatch: %w", firstErr)
	}

	writer.WriteString(fmt.Sprintf("Time of pre-analysis (EVM PreExecute): %v\n", preAnalysisDur))
	writer.WriteString(fmt.Sprintf("  PreExecute OK  : %d / %d\n", preExecOK, txCount))
	writer.WriteString(fmt.Sprintf("  Concurrency    : %d workers\n", runtime.NumCPU()))
	writer.WriteString(fmt.Sprintf("  Wall time      : ~%.2f ms/tx amortized\n",
		float64(preAnalysisDur.Milliseconds())/float64(txCount)))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	// --- Step 4: build speculativeRS/speculativeWS maps + txToNodes ---
	// Mirrors TestVegeta Step 1: build read/write key sets per txID.
	speculativeRS := make(map[string]map[string]bool, txCount)
	speculativeWS := make(map[string]map[string]bool, txCount)
	txToNodes := make(map[string][]*core.RWNode, txCount)
	txIDToIdx := make(map[string]int, txCount)

	for i := range specs {
		ref := block.Refs[i]
		txID := ref.TxHash
		txIDToIdx[txID] = i

		spec := specs[i]
		if spec == nil {
			speculativeRS[txID] = make(map[string]bool)
			speculativeWS[txID] = make(map[string]bool)
			txToNodes[txID] = nil
			continue
		}

		rSet := make(map[string]bool, len(spec.ReadKeys))
		wSet := make(map[string]bool, len(spec.WriteKeys))
		for _, k := range spec.ReadKeys {
			rSet[k] = true
		}
		for _, k := range spec.WriteKeys {
			wSet[k] = true
		}
		speculativeRS[txID] = rSet
		speculativeWS[txID] = wSet

		// Build RWNodes for this tx (needed for potential future commit path).
		transInfo := core.TransInfo{
			ID:        txID,
			Timestamp: uint32(fromBlock),
		}
		nodes := make([]*core.RWNode, 0, len(spec.ReadKeys)+len(spec.WriteKeys))
		seen := make(map[string]bool, len(spec.ReadKeys)+len(spec.WriteKeys))
		for _, k := range spec.ReadKeys {
			if seen[k] {
				continue
			}
			seen[k] = true
			nodes = append(nodes, &core.RWNode{
				RWSet:        core.RWSet{Key: []byte(k), Value: []byte{}},
				TransInfo:    transInfo,
				Label:        "r",
				ContractName: utils.ReplayContractName,
			})
		}
		for _, k := range spec.WriteKeys {
			if seen[k] {
				for idx := range nodes {
					if string(nodes[idx].RWSet.Key) == k && nodes[idx].Label == "r" {
						nodes[idx].Label = "w"
						break
					}
				}
				continue
			}
			seen[k] = true
			nodes = append(nodes, &core.RWNode{
				RWSet:        core.RWSet{Key: []byte(k), Value: []byte{}},
				TransInfo:    transInfo,
				Label:        "w",
				ContractName: utils.ReplayContractName,
			})
		}
		txToNodes[txID] = nodes
	}

	// --- Step 5: Dependency chain construction (TestVegeta Step 2) ---
	// Build key → txIDs map, then chains, then orderedTxs.
	startChain := time.Now()
	keyToTxs := make(map[string][]string)
	for txID := range speculativeRS {
		for key := range speculativeRS[txID] {
			keyToTxs[key] = append(keyToTxs[key], txID)
		}
	}
	for txID := range speculativeWS {
		for key := range speculativeWS[txID] {
			keyToTxs[key] = append(keyToTxs[key], txID)
		}
	}

	var chains [][]string
	for _, txIDs := range keyToTxs {
		if len(txIDs) > 0 {
			seen := make(map[string]bool)
			var chain []string
			for _, id := range txIDs {
				if !seen[id] {
					seen[id] = true
					chain = append(chain, id)
				}
			}
			chains = append(chains, chain)
		}
	}

	sort.Slice(chains, func(i, j int) bool {
		return len(chains[i]) > len(chains[j])
	})

	orderedTxs := make([]string, 0, txCount)
	seenTxs := make(map[string]bool)
	for _, chain := range chains {
		for _, txID := range chain {
			if !seenTxs[txID] {
				orderedTxs = append(orderedTxs, txID)
				seenTxs[txID] = true
			}
		}
	}
	for _, ref := range block.Refs {
		if !seenTxs[ref.TxHash] {
			orderedTxs = append(orderedTxs, ref.TxHash)
			seenTxs[ref.TxHash] = true
		}
	}

	chainDur := time.Since(startChain)
	writer.WriteString(fmt.Sprintf("Time of speculation (chain ordering): %v\n", chainDur))
	writer.Flush()

	// --- Step 6: Build DAG (TestVegeta Step 3) ---
	// For each tx in orderedTxs, find predecessors by conflict detection.
	startDAG := time.Now()
	dag := make(map[string][]string, txCount)
	for i, txID := range orderedTxs {
		var predecessors []string
		for j := 0; j < i; j++ {
			prevTxID := orderedTxs[j]
			hasConflict := false

			// W-W conflict
			for key := range speculativeWS[txID] {
				if speculativeWS[prevTxID][key] {
					hasConflict = true
					break
				}
			}
			if !hasConflict {
				// R-W conflict (tx reads what prev writes)
				for key := range speculativeRS[txID] {
					if speculativeWS[prevTxID][key] {
						hasConflict = true
						break
					}
				}
			}
			if !hasConflict {
				// W-R conflict (tx writes what prev reads)
				for key := range speculativeWS[txID] {
					if speculativeRS[prevTxID][key] {
						hasConflict = true
						break
					}
				}
			}
			if hasConflict {
				predecessors = append(predecessors, prevTxID)
			}
		}
		dag[txID] = predecessors
	}
	dagDur := time.Since(startDAG)
	writer.WriteString(fmt.Sprintf("Time of DAG construction: %v\n", dagDur))
	writer.Flush()

	// --- Step 7: Validation loop (batch-level, TestVegeta Step 4) ---
	// committedDelta stores ONLY the keys written by previously-committed
	// txs (incremental overlay). The witness baseline lives in each worker's
	// stateDB (injected once at pool init).
	startExec := time.Now()
	committedDelta := make(map[string]string)
	var committedDeltaLock sync.RWMutex

	executed := make(map[string]bool, txCount)
	remaining := make(map[string]bool, txCount)
	for _, txID := range orderedTxs {
		remaining[txID] = true
	}

	var serialReplayList []string
	algorithmAborted := 0
	var noContextCount int32
	var reexecErrorCount int32

	for len(remaining) > 0 {
		// Find ready txs (all predecessors executed).
		var batch []string
		for txID := range remaining {
			ready := true
			for _, pred := range dag[txID] {
				if !executed[pred] {
					ready = false
					break
				}
			}
			if ready {
				batch = append(batch, txID)
			}
		}

		if len(batch) == 0 {
			break
		}

		// Deterministic batch ordering.
		sort.Strings(batch)

		// Batch-level snapshot: all txs in this batch see the same
		// committedDelta (witness baseline + prior committed txs' writes).
		committedDeltaLock.RLock()
		overlaySnapshot := cloneStringMap(committedDelta)
		committedDeltaLock.RUnlock()

		batchResults := make(map[string]bool)
		batchWriteValues := make(map[string]map[string]string)
		var resultsLock sync.Mutex
		var wg sync.WaitGroup

		validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
			txID := i.(string)
			defer wg.Done()

			txIdx, ok := txIDToIdx[txID]
			if !ok {
				atomic.AddInt32(&noContextCount, 1)
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				return
			}

			worker, err := levmPool.Acquire()
			if err != nil {
				atomic.AddInt32(&reexecErrorCount, 1)
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				fmt.Printf("  TX %s: aborted (worker acquire error: %v)\n", txID, err)
				return
			}
			defer levmPool.Release(worker)

			result, err := worker.ReExecute(fromBlock, txIdx, overlaySnapshot)
			if err != nil {
				atomic.AddInt32(&reexecErrorCount, 1)
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				serialReplayList = append(serialReplayList, txID)
				fmt.Printf("  TX %s: aborted (re-execution error: %v)\n", txID, err)
				return
			}

			// Check: real keys ⊆ speculative keys?
			preRS := speculativeRS[txID]
			preWS := speculativeWS[txID]

			match := true
			for _, key := range result.RealReadKeys {
				if !preRS[key] {
					match = false
					break
				}
			}
			if match {
				for _, key := range result.RealWriteKeys {
					if !preWS[key] {
						match = false
						break
					}
				}
			}

			if !match {
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				serialReplayList = append(serialReplayList, txID)
				fmt.Printf("  TX %s: aborted (real keys exceed conservative) - real=%d, conservative=%d\n",
					txID, len(result.RealReadKeys)+len(result.RealWriteKeys),
					len(preRS)+len(preWS))
				return
			}

			resultsLock.Lock()
			batchResults[txID] = true
			batchWriteValues[txID] = result.WriteValues
			resultsLock.Unlock()
		})

		for _, txID := range batch {
			wg.Add(1)
			_ = validatePool.Invoke(txID)
		}
		wg.Wait()
		validatePool.Release()

		// Merge successful txs' WriteValues into committedDelta.
		// Failed txs are removed from remaining but NOT merged — they
		// will be replayed serially in Step 8.
		for _, txID := range batch {
			delete(remaining, txID)
			executed[txID] = true
			if batchResults[txID] {
				committedDeltaLock.Lock()
				for k, v := range batchWriteValues[txID] {
					committedDelta[k] = v
				}
				committedDeltaLock.Unlock()
			} else {
				algorithmAborted++
			}
		}
	}

	execDur := time.Since(startExec)
	writer.WriteString(fmt.Sprintf("Time of replay (validation): %v\n", execDur))

	// --- Step 8: Serial replay of aborted txs (sorted by TxID) ---
	// Mirrors TestVegeta Step 5: aborted txs execute serially with state
	// ACCUMULATING between txs (ReExecuteCommit → PreExecuteCommit).
	//
	// Uses a dedicated levm from the pool (the validation loop is done).
	sort.Strings(serialReplayList)
	serialReplayed := 0

	// With --disk-commit, serial replay runs on a dedicated on-disk leveldb
	// levm so its end-of-block trie commit pays real disk-flush cost. With
	// --disk-trie the witness is a REAL trie on disk, so serial replay also
	// reads through the real trie path (no simulation).
	//
	// Constructed LAZILY and BEFORE the timer (same rationale as Depurge
	// Step 9): the --disk-trie replayer re-encodes the witness into its own
	// leveldb — a one-time preparation cost that the serial baseline likewise
	// excludes from its timer. With zero aborts we skip construction.
	var serialReplayer *utils.LevmSpecFallback
	if len(serialReplayList) > 0 {
		switch {
		case trieDisk:
			serialReplayer, err = utils.NewLevmSpecFallbackTrieDisk(ds, fromBlock, toBlock, trieCacheMB)
			if err != nil {
				return fmt.Errorf("NewLevmSpecFallbackTrieDisk (serial replay): %w", err)
			}
			defer serialReplayer.Close()
		case diskCommit:
			serialReplayer, err = utils.NewLevmSpecFallbackDisk(ds, fromBlock, toBlock)
			if err != nil {
				return fmt.Errorf("NewLevmSpecFallbackDisk (serial replay): %w", err)
			}
			defer serialReplayer.Close()
		default:
			serialReplayer, err = levmPool.Acquire()
			if err != nil {
				return fmt.Errorf("levmPool.Acquire (serial replay): %w", err)
			}
			defer levmPool.Release(serialReplayer)
		}
	}
	startSerial := time.Now()

	// First tx: apply committedDelta overlay via ReExecuteCommit.
	// Subsequent txs: PreExecuteCommit (stateDB already carries accumulated writes).
	var firstDone bool
	for _, txID := range serialReplayList {
		txIdx, ok := txIDToIdx[txID]
		if !ok {
			continue
		}

		if !firstDone {
			committedDeltaLock.RLock()
			override := cloneStringMap(committedDelta)
			committedDeltaLock.RUnlock()

			result, err := serialReplayer.ReExecuteCommit(fromBlock, txIdx, override)
			if err != nil {
				fmt.Printf("  TX %s: serial replay failed (%v)\n", txID, err)
				firstDone = true
				serialReplayed++
				continue
			}
			if result != nil {
				committedDeltaLock.Lock()
				for k, v := range result.WriteValues {
					committedDelta[k] = v
				}
				committedDeltaLock.Unlock()
			}
			firstDone = true
		} else {
			if _, err := serialReplayer.PreExecuteCommit(fromBlock, txIdx); err != nil {
				fmt.Printf("  TX %s: serial replay failed (%v)\n", txID, err)
			}
		}
		serialReplayed++
	}
	serialDur := time.Since(startSerial)
	// With --disk-commit, pay the real end-of-block trie flush so serial
	// replay reflects the same commit cost as the serial baseline.
	if diskCommit {
		commitDur, cerr := serialReplayer.CommitTrie()
		if cerr != nil {
			fmt.Printf("  WARN: serial replay trie commit failed: %v\n", cerr)
		} else {
			serialDur += commitDur
			writer.WriteString(fmt.Sprintf("  trie commit (serial replay): %v\n", commitDur))
		}
	}
	writer.WriteString(fmt.Sprintf("Time of serial replay: %v\n", serialDur))
	writer.WriteString(fmt.Sprintf("Serial replayed: %d\n", serialReplayed))

	writer.WriteString(fmt.Sprintf("Time of validation and execution: %v\n", time.Since(startExec)))

	// --- Step 9: Baseline serial execution + report ---
	// Mirrors Depurge's baseline: fresh levm, serial PreExecuteCommit for
	// every tx, measures Vegeta's speedup vs pure serial.
	vegetaDur := preAnalysisDur + execDur + serialDur + dagDur

	// Serial baseline: with --disk-commit use a real on-disk leveldb so the
	// end-of-block trie commit pays actual disk-flush cost; with --disk-trie
	// the witness is a REAL trie on disk so the baseline reads through the
	// real trie path; otherwise the in-memory levm (fast, no disk I/O).
	var serialBaseline *utils.LevmSpecFallback
	switch {
	case trieDisk:
		serialBaseline, err = utils.NewLevmSpecFallbackTrieDisk(ds, fromBlock, toBlock, trieCacheMB)
	case diskCommit:
		serialBaseline, err = utils.NewLevmSpecFallbackDisk(ds, fromBlock, toBlock)
	default:
		serialBaseline, err = utils.NewLevmSpecFallback(ds, fromBlock, toBlock)
	}
	if err != nil {
		return fmt.Errorf("NewLevmSpecFallback (serial baseline): %w", err)
	}
	// Serial baseline pays the same cold-read simulation from its OWN cold
	// cache (independent simulator), so both sides face identical state-access
	// costs and only the latency-overlap differs.
	serialBaseline.SetAccessLatency(levm.NewAccessLatencySimulator(stateLatency))
	defer serialBaseline.Close()

	startBaseline := time.Now()
	serialOK, serialFail := 0, 0
	for txIdx := 0; txIdx < txCount; txIdx++ {
		rw, err := serialBaseline.PreExecuteCommit(fromBlock, txIdx)
		if err != nil || rw == nil || !rw.Success {
			serialFail++
			continue
		}
		serialOK++
	}
	serialBaselineDur := time.Since(startBaseline)
	// With --disk-commit, pay the real end-of-block trie flush so the serial
	// baseline reflects a real node's commit cost (IntermediateRoot + Commit +
	// leveldb flush), not just in-memory state accumulation.
	if diskCommit {
		commitDur, cerr := serialBaseline.CommitTrie()
		if cerr != nil {
			fmt.Printf("  WARN: serial baseline trie commit failed: %v\n", cerr)
		} else {
			serialBaselineDur += commitDur
			writer.WriteString(fmt.Sprintf("  trie commit (serial baseline): %v\n", commitDur))
		}
	}

	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.WriteString(fmt.Sprintf("Baseline: serial execution of all %d txs (commit between txs)\n", txCount))
	writer.WriteString(fmt.Sprintf("Time of serial execution: %v (ok=%d, fail=%d)\n",
		serialBaselineDur, serialOK, serialFail))
	writer.WriteString(fmt.Sprintf("Algorithm aborted (total): %d\n", algorithmAborted))
	writer.WriteString(fmt.Sprintf("  - context not found: %d\n", atomic.LoadInt32(&noContextCount)))
	writer.WriteString(fmt.Sprintf("  - re-execution error: %d\n", atomic.LoadInt32(&reexecErrorCount)))
	writer.WriteString(fmt.Sprintf("  - key exceed (serial replayed): %d\n", serialReplayed))
	if txCount > 0 {
		writer.WriteString(fmt.Sprintf("Abort rate: %.3f\n",
			float64(algorithmAborted)/float64(txCount)))
	}
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Vegeta: %v\n", vegetaDur))
	if serialBaselineDur > 0 {
		writer.WriteString(fmt.Sprintf("Speedup (serial / Vegeta): %.2fx\n",
			float64(serialBaselineDur)/float64(vegetaDur)))
	}
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	return nil
}

// initCommittedStateFromWitness builds the initial committedState map from
// the block's witness. Keys use canonical format matching LLM augmentAccountKeys:
//
//	acct:<0xaddr_lowercase>:balance  = hex balance
//	acct:<0xaddr_lowercase>:nonce   = hex nonce
//	acct:<0xaddr_lowercase>:code    = hex code (if non-empty)
//	slot:<0xaddr_lowercase>:<0xslot> = hex value
func initCommittedStateFromWitness(witness *utils.ReplayBlockWitness) map[string]string {
	state := make(map[string]string)
	if witness == nil {
		return state
	}
	for addrHex, acct := range witness.Accounts {
		addr := strings.ToLower(addrHex)
		if acct.Balance != "" {
			state["acct:"+addr+":balance"] = normalizeHex(acct.Balance)
		}
		if acct.Nonce != "" {
			state["acct:"+addr+":nonce"] = normalizeHex(acct.Nonce)
		}
		if acct.Code != "" {
			state["acct:"+addr+":code"] = normalizeHex(acct.Code)
		}
		if acct.Storage != nil {
			for slot, val := range acct.Storage {
				state["slot:"+addr+":"+strings.ToLower(slot)] = normalizeHex(val)
			}
		}
	}
	return state
}

// normalizeHex ensures a hex value is 0x-prefixed (lowercase).
func normalizeHex(s string) string {
	if s == "" {
		return "0x0"
	}
	if !strings.HasPrefix(s, "0x") {
		return "0x" + strings.ToLower(s)
	}
	return strings.ToLower(s)
}

// cloneStringMap returns a shallow copy of m.
func cloneStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// filterOverlayByKeys copies only the entries of m whose key is in allowed.
// Returns nil when nothing matches so ReExecute skips applyStateOverride
// entirely (its overlay == nil check).
func filterOverlayByKeys(m map[string]string, allowed map[string]bool) map[string]string {
	if len(m) == 0 || len(allowed) == 0 {
		return nil
	}
	var out map[string]string
	for k, v := range m {
		if allowed[k] {
			if out == nil {
				out = make(map[string]string, len(allowed))
			}
			out[k] = v
		}
	}
	return out
}

// updateCommittedStateWithDeltas merges write deltas into committedState.
//
// For keys already in committedState:
//
//	newVal = (current + delta) mod 2^256
//	(values that become negative via underflow wrap to 2^256 + newVal)
//
// For keys NOT in committedState (first write):
//
//	use WriteValues[key] (absolute post-execution value) directly.
//	This is critical because delta + 0 ≠ witness_value + delta when the
//	witness baseline is non-zero.
func updateCommittedStateWithDeltas(
	state map[string]string,
	delta map[string]*big.Int,
	writeValues map[string]string,
) {
	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	for key, d := range delta {
		if existing, ok := state[key]; ok && existing != "" {
			current := new(big.Int)
			if _, ok := current.SetString(strings.TrimPrefix(existing, "0x"), 16); ok {
				newVal := new(big.Int).Add(current, d)
				if newVal.Sign() < 0 {
					newVal = new(big.Int).Add(newVal, two256)
				}
				state[key] = fmt.Sprintf("0x%x", newVal)
			} else {
				// Parse failed — fall back to absolute value.
				state[key] = writeValues[key]
			}
		} else {
			// New key — use absolute post-exec value (not delta + 0).
			state[key] = writeValues[key]
		}
	}
}

// listLLMCacheFileCount returns the number of contract directories under
// cache/mainnet_rw/. Each subdirectory is a 0x-prefixed contract address that
// has been pre-analyzed. Used by runReplayDepurgeMode to show how many mainnet
// contracts have LLM cache available.
func listLLMCacheFileCount() int {
	p := filepath.Join(".", "cache", "mainnet_rw")
	entries, err := os.ReadDir(p)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			n++
		}
	}
	return n
}
