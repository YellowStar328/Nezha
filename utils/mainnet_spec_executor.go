package utils

import (
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"Nezha/core"
)

// LLMSpecStats contains high-level counters from a LLMSpecExecuteAll run.
// Useful for performance reports to distinguish true LLM-cache hits from
// EVM fallback (which dominates the pre-analysis time when LLM cache is
// sparse, e.g. mainnet replay of unknown contracts).
type LLMSpecStats struct {
	Total         int // total txs processed
	LLMHit        int // specs produced purely from static analysis (no EVM)
	FallbackCount int // specs produced via SpecFallback.PreExecute (EVM)
	FallbackOK    int // FallbackCount that returned Success=true
	// FallbackReasons tallies why each fallback happened. Keys are short
	// labels derived from the error type:
	//   "no-cache"            — ErrNotPreAnalyzed (cache file missing)
	//   "unresolvable-acct"   — ErrUnresolvableAccount (global-var-as-key)
	//   "cross-call-miss"    — ErrCrossContractNotCached
	//   "creation"            — tx.To == "" (contract creation)
	//   "no-calldata"         — len(tx.Input) < 10 (plain transfer)
	//   "key-unresolvable"    — ErrKeyUnresolvable (mapping/struct key not derivable)
	//   "other"               — any other error
	FallbackReasons map[string]int
	// Sources is indexed by txIdx and labels the ORIGIN of each tx's
	// conservative RW set: "llm" (from LLM static-analysis cache) or
	// "preexec" (from EVM PreExecute fallback on witness base state).
	// Used by the validation loop to attribute key-exceed aborts to
	// either LLM-analysis gaps or pre-execution state-path divergence.
	Sources []string
}

// LLMSpecExecutor generates speculative RW sets using LLM static analysis
// instead of EVM pre-execution. Falls back to EVM PreExecute for unanalyzed txs.
type LLMSpecExecutor struct {
	mgr     *MainnetContractManager
	evmExec SpecFallback // for fallback (in-process levm)
}

// NewLLMSpecExecutor creates a new LLMSpecExecutor that uses the given contract
// manager for LLM-analyzed RW sets and the given EVM executor for fallback.
// evmExec is *LevmSpecFallback (in-process, no HTTP).
func NewLLMSpecExecutor(mgr *MainnetContractManager, evmExec SpecFallback) *LLMSpecExecutor {
	return &LLMSpecExecutor{
		mgr:     mgr,
		evmExec: evmExec,
	}
}

// classifyFallbackReason returns a short label explaining why a tx fell back
// to EVM PreExecute. Used for diagnostic stats in LLMSpecStats.FallbackReasons.
func classifyFallbackReason(tx RawTransaction, llmErr error) string {
	if tx.To == "" {
		return "creation"
	}
	if len(tx.Input) < 10 {
		return "no-calldata"
	}
	if llmErr == nil {
		return "unknown"
	}
	if errors.Is(llmErr, ErrNotPreAnalyzed) {
		return "no-cache"
	}
	if errors.Is(llmErr, ErrUnresolvableAccount) {
		return "unresolvable-acct"
	}
	if errors.Is(llmErr, ErrCrossContractNotCached) {
		return "cross-call-miss"
	}
	if errors.Is(llmErr, ErrKeyUnresolvable) {
		return "key-unresolvable"
	}
	return "other"
}

// LLMSpecExecuteAll is the main entry point. It generates speculative RW sets
// for all txs in a block using LLM static analysis, falling back to EVM
// PreExecute for contract creations, plain transfers, and unanalyzed functions.
// The returned slice is indexed by txIdx (position in the block), matching the
// PreExecuteAll contract.
func (e *LLMSpecExecutor) LLMSpecExecuteAll(ds *DatasetReader, blockNum uint64) ([]*core.ReplayRWSet, error) {
	txs, err := ds.LoadBlockTxs(blockNum)
	if err != nil {
		return nil, fmt.Errorf("load block txs %d: %w", blockNum, err)
	}
	out := make([]*core.ReplayRWSet, len(txs))
	for txIdx, tx := range txs {
		// Contract creation (empty To) or plain transfer / no calldata
		// (input shorter than a 4-byte selector) cannot be analyzed
		// statically — fall back to EVM.
		if tx.To == "" || len(tx.Input) < 10 { // "0x" + 8 hex = 10 chars
			rw, ferr := e.evmExec.PreExecute(blockNum, txIdx)
			if ferr != nil {
				return nil, fmt.Errorf("evm fallback %d:%d: %w", blockNum, txIdx, ferr)
			}
			// EVM PreExecute captures slot: storage keys via rwTracker,
			// but NOT account-metadata keys (balance / nonce / code /
			// exist). Augment them here so the conservative set covers
			// these — otherwise validation will see real keys ⊋
			// conservative and spuriously abort.
			args := MainnetTxArgs{MsgSender: strings.ToLower(tx.From)}
			rw.ReadKeys, rw.WriteKeys = augmentAccountKeys(rw.ReadKeys, rw.WriteKeys, tx, args)
			out[txIdx] = rw
			continue
		}

		selector := strings.ToLower(tx.Input[0:10])
		toAddr := strings.ToLower(tx.To)
		args := e.decodeArgs(toAddr, selector, tx.Input)
		args.MsgSender = strings.ToLower(tx.From)

		readKeys, writeKeys, gerr := e.mgr.GetConservativeRWSet(toAddr, selector, args)
		if gerr != nil {
			// LLM analysis unavailable (ErrNotPreAnalyzed,
			// ErrCrossContractNotCached, or any other failure) — fall
			// back to EVM PreExecute for this tx.
			rw, ferr := e.evmExec.PreExecute(blockNum, txIdx)
			if ferr != nil {
				return nil, fmt.Errorf("evm fallback %d:%d (llm err: %v): %w", blockNum, txIdx, gerr, ferr)
			}
			// Same augment as the creation/transfer fallback above.
			rw.ReadKeys, rw.WriteKeys = augmentAccountKeys(rw.ReadKeys, rw.WriteKeys, tx, args)
			out[txIdx] = rw
			continue
		}

		// Augment with account-level keys that EVM touches on every tx
		// (balance / nonce / code / exist). LLM static analysis only
		// produces slot: storage keys, so without this augmentation the
		// speculative set misses all account-metadata reads that the
		// serial re-execution captures via rwTracker — causing spurious
		// Vegeta aborts.
		readKeys, writeKeys = augmentAccountKeys(readKeys, writeKeys, tx, args)

		out[txIdx] = &core.ReplayRWSet{
			Ref: core.ReplayRef{
				BlockNum: blockNum,
				TxIndex:  txIdx,
				TxHash:   tx.Hash,
			},
			Success:   true, // LLM analysis always "succeeds" (no EVM execution)
			GasUsed:   0,    // No gas info from static analysis
			ReadKeys:  readKeys,
			WriteKeys: writeKeys,
		}
	}
	return out, nil
}

// BatchPreExecutor is an optional interface that SpecFallback implementations
// can implement to run PreExecute concurrently for a batch of tx indices.
// *LevmSpecFallbackPool implements it; single *LevmSpecFallback does not
// (it falls back to serial PreExecute).
type BatchPreExecutor interface {
	PreExecuteBatch(txIndices []int) ([]*core.ReplayRWSet, []error)
}

// LLMSpecExecuteAllWithStats generates speculative RW sets for all txs in
// two concurrent phases:
//
//  1. LLM cache lookups run CONCURRENTLY (NumCPU-semaphored goroutines).
//     This is I/O-bound (file reads + key conversion), typically <2ms.
//  2. LLM-miss txs are streamed to a pipelined EVM stage the moment Phase-1
//     goroutines discover them, so EVM PreExecute overlaps with the remaining
//     LLM lookups. Total pre-analysis wall time becomes ~max(LLM, EVM) instead
//     of LLM + EVM (measured: ~27ms → ~18ms on block 24000000).
//
// In pool mode (LevmSpecFallbackPool) the EVM work is streamed via missCh and
// runs CONCURRENTLY with Phase 1. In non-pool mode (single LevmSpecFallback)
// misses are collected and run after Phase 1.
func (e *LLMSpecExecutor) LLMSpecExecuteAllWithStats(ds *DatasetReader, fromBlock, toBlock uint64) ([]*core.ReplayRWSet, LLMSpecStats, error) {
	txs, err := ds.LoadBlockRangeTxs(fromBlock, toBlock)
	if err != nil {
		return nil, LLMSpecStats{}, fmt.Errorf("load block range txs [%d,%d]: %w", fromBlock, toBlock, err)
	}
	n := len(txs)
	stats := LLMSpecStats{
		Total:           n,
		FallbackReasons: make(map[string]int),
		Sources:         make([]string, n),
	}
	out := make([]*core.ReplayRWSet, n)

	// --- Phase 1: concurrent LLM cache lookups ---
	// Each goroutine writes to its own slot in llmOutcomes (distinct index →
	// no data race; visibility guaranteed by WaitGroup happens-before).
	type llmOutcome struct {
		hit       bool
		readKeys  []string
		writeKeys []string
		reason    string // fallback reason label (empty if hit)
	}
	llmOutcomes := make([]llmOutcome, n)
	var llmWg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())

	// --- Pipelined EVM consumers (pool mode) ---
	missCh := make(chan int, n)
	var misses []int
	var evmWg sync.WaitGroup
	var fallbackOK int64
	var evmErrMu sync.Mutex
	var evmErr error
	pool, isPool := e.evmExec.(*LevmSpecFallbackPool)
	if isPool {
		evmWorkers := pool.n
		if evmWorkers > n {
			evmWorkers = n
		}
		for w := 0; w < evmWorkers; w++ {
			evmWg.Add(1)
			go func() {
				defer evmWg.Done()
				for txIdx := range missCh {
					fb, aerr := pool.Acquire()
					if aerr != nil {
						evmErrMu.Lock()
						if evmErr == nil {
							evmErr = aerr
						}
						evmErrMu.Unlock()
						continue
					}
					rw, perr := fb.PreExecute(fromBlock, txIdx)
					pool.Release(fb)
					if perr != nil {
						evmErrMu.Lock()
						if evmErr == nil {
							evmErr = fmt.Errorf("evm fallback %d:%d: %w", fromBlock, txIdx, perr)
						}
						evmErrMu.Unlock()
						continue
					}
					if rw != nil && rw.Success {
						atomic.AddInt64(&fallbackOK, 1)
					}
					out[txIdx] = rw
				}
			}()
		}
	}
	// submitMiss hands a LLM-miss txIdx to the EVM stage: streamed to the
	// pipelined consumers in pool mode. Non-pool misses are collected later
	// in the serial outcome-processing loop (misses below).
	submitMiss := func(txIdx int) {
		if isPool {
			missCh <- txIdx
		}
	}

	for txIdx, tx := range txs {
		llmWg.Add(1)
		sem <- struct{}{}
		go func(txIdx int, tx RawTransaction) {
			defer llmWg.Done()
			defer func() { <-sem }()

			if tx.To == "" || len(tx.Input) < 10 {
				// Contract creation / plain transfer — no static analysis,
				// fall back to EVM.
				llmOutcomes[txIdx] = llmOutcome{
					hit:    false,
					reason: classifyFallbackReason(tx, nil),
				}
				submitMiss(txIdx)
				return
			}
			selector := strings.ToLower(tx.Input[0:10])
			toAddr := strings.ToLower(tx.To)

			// Fast-path: if no analysis exists for this (addr, selector), skip
			// the expensive decodeArgs (ABI selector matching) and the cache
			// lookup / failed disk open entirely. decodeArgs is only paid for
			// txs that are actually going to hit the LLM cache.
			if !e.mgr.IsRWSetCached(toAddr, selector) {
				llmOutcomes[txIdx] = llmOutcome{
					hit:    false,
					reason: classifyFallbackReason(tx, ErrNotPreAnalyzed),
				}
				submitMiss(txIdx)
				return
			}

			args := e.decodeArgs(toAddr, selector, tx.Input)
			args.MsgSender = strings.ToLower(tx.From)

			readKeys, writeKeys, gerr := e.mgr.GetConservativeRWSet(toAddr, selector, args)
			if gerr != nil {
				llmOutcomes[txIdx] = llmOutcome{
					hit:    false,
					reason: classifyFallbackReason(tx, gerr),
				}
				submitMiss(txIdx)
				return
			}
			// LLM hit — augment account keys (LLM emits slot keys only).
			readKeys, writeKeys = augmentAccountKeys(readKeys, writeKeys, tx, args)
			llmOutcomes[txIdx] = llmOutcome{
				hit:       true,
				readKeys:  readKeys,
				writeKeys: writeKeys,
			}
		}(txIdx, tx)
	}
	llmWg.Wait()

	// --- Process LLM outcomes (serial stats tracking, no races) ---
	for i := 0; i < n; i++ {
		oc := llmOutcomes[i]
		if oc.hit {
			stats.LLMHit++
			stats.Sources[i] = "llm"
			out[i] = &core.ReplayRWSet{
				Ref: core.ReplayRef{
					BlockNum: fromBlock,
					TxIndex:  i,
					TxHash:   txs[i].Hash,
				},
				Success:   true,
				GasUsed:   0,
				ReadKeys:  oc.readKeys,
				WriteKeys: oc.writeKeys,
			}
		} else {
			stats.FallbackCount++
			stats.FallbackReasons[oc.reason]++
			stats.Sources[i] = "preexec"
			misses = append(misses, i)
		}
	}

	// Close the miss channel only after every Phase-1 goroutine has finished
	// (llmWg.Wait above), then wait for the pipelined EVM consumers to drain.
	if isPool {
		close(missCh)
	}
	evmWg.Wait()

	if isPool {
		// Pipelined EVM PreExecute ran CONCURRENTLY with the LLM lookups, so
		// the results are already in out[]; only fold the stats and errors.
		stats.FallbackOK = int(atomic.LoadInt64(&fallbackOK))
		evmErrMu.Lock()
		err := evmErr
		evmErrMu.Unlock()
		if err != nil {
			return nil, stats, err
		}
		return out, stats, nil
	}

	if len(misses) == 0 {
		return out, stats, nil
	}

	// --- Non-pool fallback path (single LevmSpecFallback): run PreExecute
	// after the LLM phase ---
	batcher, hasBatch := e.evmExec.(BatchPreExecutor)
	if hasBatch {
		results, errs := batcher.PreExecuteBatch(misses)
		for i, txIdx := range misses {
			if err := errs[i]; err != nil {
				return nil, stats, fmt.Errorf("evm fallback %d:%d: %w",
					fromBlock, txIdx, err)
			}
			rw := results[i]
			if rw != nil && rw.Success {
				stats.FallbackOK++
			}
			// PreExecute produced concrete slot keys from real EVM execution
			// on the witness base state — no account-key augmentation needed
			// (ReExecute also captures slot keys only via storageKeysToStrings,
			// so the subset check is consistent without acct: keys).
			out[txIdx] = rw
		}
	} else {
		// Serial fallback path (single LevmSpecFallback).
		for _, txIdx := range misses {
			rw, ferr := e.evmExec.PreExecute(fromBlock, txIdx)
			if ferr != nil {
				return nil, stats, fmt.Errorf("evm fallback %d:%d: %w",
					fromBlock, txIdx, ferr)
			}
			if rw != nil && rw.Success {
				stats.FallbackOK++
			}
			out[txIdx] = rw
		}
	}
	return out, stats, nil
}

// augmentAccountKeys adds the account-level (acct:addr:balance/nonce/code/exist)
// keys that EVM touches on every transaction but LLM static analysis cannot
// predict. These keys are captured by rwTracker during serial re-execution
// (see cmd/eth-replayd/replayd/rw_tracker.go) and MUST be present in the
// speculative set to avoid spurious Vegeta aborts.
//
// Conservative rules applied:
//   - Sender (tx.From): always reads balance+nonce (gas check), writes both
//     (gas deduction + nonce increment).
//   - Target (tx.To): always reads code (load bytecode), exist (account
//     check), balance; writes balance if tx.Value > 0.
//   - Any contract address that appears in a slot: key (either as Addr1,
//     Addr2, MsgSender, or the target contract itself): conservatively read
//     its balance, code, exist — because EVM may touch these when the
//     contract is involved in a call chain.
//
// All additions go through dedupMaps at the end so the slice stays clean.
func augmentAccountKeys(readKeys, writeKeys []string, tx RawTransaction, args MainnetTxArgs) ([]string, []string) {
	fromAddr := strings.ToLower(tx.From)
	toAddr := strings.ToLower(tx.To)

	// Collect every contract address we know is involved in this tx.
	involved := make(map[string]bool)
	if fromAddr != "" {
		involved[fromAddr] = true
	}
	if toAddr != "" {
		involved[toAddr] = true
	}
	if args.Addr1 != "" {
		involved[args.Addr1] = true
	}
	if args.Addr2 != "" {
		involved[args.Addr2] = true
	}
	if args.MsgSender != "" {
		involved[args.MsgSender] = true
	}
	// Also scan existing slot: keys — their contract portion is involved too.
	for _, k := range readKeys {
		if addr, ok := slotKeyContractAddr(k); ok {
			involved[addr] = true
		}
	}
	for _, k := range writeKeys {
		if addr, ok := slotKeyContractAddr(k); ok {
			involved[addr] = true
		}
	}

	readSet := make(map[string]bool, len(readKeys))
	for _, k := range readKeys {
		readSet[k] = true
	}
	writeSet := make(map[string]bool, len(writeKeys))
	for _, k := range writeKeys {
		writeSet[k] = true
	}

	addRead := func(key string) {
		if !readSet[key] {
			readSet[key] = true
			readKeys = append(readKeys, key)
		}
	}
	addWrite := func(key string) {
		if !writeSet[key] {
			writeSet[key] = true
			writeKeys = append(writeKeys, key)
		}
	}

	// --- EVM tx-execution invariants (always happen) ---
	if fromAddr != "" {
		// Sender pays gas: reads balance+nonce, writes both.
		addRead("acct:" + fromAddr + ":balance")
		addRead("acct:" + fromAddr + ":nonce")
		addWrite("acct:" + fromAddr + ":balance")
		addWrite("acct:" + fromAddr + ":nonce")
	}
	if toAddr != "" {
		// Target account existence + code load (for contract calls) + balance.
		addRead("acct:" + toAddr + ":exist")
		addRead("acct:" + toAddr + ":code")
		addRead("acct:" + toAddr + ":balance")
		// Native ETH transfer writes target balance.
		if tx.Value != "" && tx.Value != "0x0" && tx.Value != "0" {
			addWrite("acct:" + toAddr + ":balance")
		}
	}

	// --- Conservative: every other involved address may have its
	// balance/code/exist touched by a sub-call chain. Add as reads to be
	// safe (extra reads cannot cause aborts; missing reads can). ---
	for addr := range involved {
		if addr == fromAddr || addr == toAddr {
			continue
		}
		addRead("acct:" + addr + ":balance")
		addRead("acct:" + addr + ":exist")
		addRead("acct:" + addr + ":code")
	}

	return readKeys, writeKeys
}

// slotKeyContractAddr extracts the contract address portion from a
// "slot:<0xaddr>:<0xslot>" key. Returns ("", false) if the key is not a slot
// key or the address portion is malformed.
func slotKeyContractAddr(key string) (string, bool) {
	// Format: "slot:<0xaddr>:<0xslot>"
	// Split into exactly 3 parts: "slot", addr, slot.
	// Note: the slot portion itself contains no ':' (it's a 0x-prefixed hex).
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 || parts[0] != "slot" {
		return "", false
	}
	addr := parts[1]
	if len(addr) != 42 || !strings.HasPrefix(addr, "0x") {
		return "", false
	}
	return strings.ToLower(addr), true
}

// decodeArgs decodes address arguments from calldata.
//
// It first consults the contract's ABI (via the contract manager cache) to
// identify which parameters are address-typed, then extracts those addresses
// from the calldata words. If no ABI is available, it falls back to a
// heuristic: the first two 32-byte words after the selector are inspected, and
// any word whose top 12 bytes are zero is treated as a (left-padded) address.
//
// Calldata layout:
//
//	input = "0x" + selector(8 hex) + word0(64 hex) + word1(64 hex) + ...
//
// For an address at word index wordIdx (0-based):
//
//	addr = "0x" + input[10 + wordIdx*64 + 24 : 10 + wordIdx*64 + 64]
func (e *LLMSpecExecutor) decodeArgs(address, selector, input string) MainnetTxArgs {
	var args MainnetTxArgs
	args.Selector = strings.ToLower(selector)
	if rawHex := strings.TrimPrefix(input, "0x"); rawHex != "" && len(rawHex)%2 == 0 {
		if b, derr := hex.DecodeString(rawHex); derr == nil {
			args.RawCalldata = b
		}
	}

	entries := e.mgr.GetABIInputs(address, selector)
	if len(entries) > 0 {
		// ABI available: walk inputs in order, track the word index across
		// all (top-level, static) params, and extract addresses.
		wordIdx := 0
		addrCount := 0
		for _, in := range entries[0].Inputs {
			if in.Type == "address" && addrCount < 2 {
				addr := extractAddress(input, wordIdx)
				if addr != "" {
					if addrCount == 0 {
						args.Addr1 = addr
					} else {
						args.Addr2 = addr
					}
					addrCount++
				}
			}
			wordIdx++
		}
		return args
	}

	// No ABI available: heuristic — extract the first two words that look
	// like valid (zero-padded) addresses.
	if addr := extractAddress(input, 0); addr != "" {
		args.Addr1 = addr
		if addr2 := extractAddress(input, 1); addr2 != "" {
			args.Addr2 = addr2
		}
	}
	return args
}

// extractAddress extracts the address at the given 32-byte word index from
// calldata. Returns "" if the word is not present in the calldata or the top
// 12 bytes (24 hex chars) are non-zero — i.e., the word does not look like a
// valid left-padded 20-byte address.
//
// input layout: "0x" + selector(8 hex) + word0(64 hex) + word1(64 hex) + ...
// The address occupies the last 40 hex chars (20 bytes) of its 32-byte word.
func extractAddress(input string, wordIdx int) string {
	const (
		prefixLen      = 2  // "0x"
		selectorHexLen = 8  // 4-byte selector
		wordHexLen     = 64 // 32 bytes
		zeroPadHexLen  = 24 // 12 bytes of zero padding
	)
	wordStart := prefixLen + selectorHexLen + wordIdx*wordHexLen
	wordEnd := wordStart + wordHexLen
	if len(input) < wordEnd {
		return ""
	}
	// The top 12 bytes (24 hex chars) of the word must be zero for a
	// valid left-padded address.
	if input[wordStart:wordStart+zeroPadHexLen] != zeroPad24 {
		return ""
	}
	addrHex := input[wordStart+zeroPadHexLen : wordEnd]
	return "0x" + strings.ToLower(addrHex)
}

// zeroPad24 is 24 hex chars ('0' * 24) representing 12 zero bytes — the
// expected high-order padding of a 20-byte address in a 32-byte EVM word.
var zeroPad24 = strings.Repeat("0", 24)
