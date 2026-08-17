package utils

import (
	"fmt"
	"strings"

	"Nezha/core"
)

// LLMSpecExecutor generates speculative RW sets using LLM static analysis
// instead of EVM pre-execution. Falls back to EVM PreExecute for unanalyzed txs.
type LLMSpecExecutor struct {
	mgr     *MainnetContractManager
	evmExec *HTTPReplayExecutor // for fallback
}

// NewLLMSpecExecutor creates a new LLMSpecExecutor that uses the given contract
// manager for LLM-analyzed RW sets and the given EVM executor for fallback.
func NewLLMSpecExecutor(mgr *MainnetContractManager, evmExec *HTTPReplayExecutor) *LLMSpecExecutor {
	return &LLMSpecExecutor{
		mgr:     mgr,
		evmExec: evmExec,
	}
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
