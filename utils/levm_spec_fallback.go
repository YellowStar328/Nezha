package utils

import (
	"fmt"
	"math/big"
	"os"
	"runtime"
	"strings"
	"sync"

	"Nezha/core"
	levm "Nezha/evm/levm"

	"Nezha/ethereum/go-ethereum/common"
	ecore "Nezha/ethereum/go-ethereum/core"
	"Nezha/ethereum/go-ethereum/core/state"
)

// LevmSpecFallback is an in-process SpecFallback implementation that uses the
// local levm (Nezha/ethereum/go-ethereum fork) to execute raw mainnet
// transactions for the LLM spec executor's fallback path and for Depurge's
// re-execution/validation phase.
//
// It holds a single LEVM instance pre-initialized with the block's witness
// state (code + storage + balance + nonce for every account). Each PreExecute
// / ReExecute call snapshots the stateDB, optionally injects a committed-state
// overlay, runs ReplayTransaction, captures the read/write keys, then reverts
// to the snapshot so the next call starts from the same witness baseline.
//
// This avoids the secp256k1 cgo duplicate-symbol link error that occurs when
// the root module also imports the cmd/eth-replayd module (which depends on a
// different go-ethereum version). levm uses the same local fork as the root
// module, so there is only one go-ethereum copy in the final binary.
type LevmSpecFallback struct {
	lvm *levm.LEVM
	// txs is the raw transaction list for the current block, indexed by txIdx.
	// Loaded once in NewLevmSpecFallback; reused by PreExecute / ReExecute.
	txs []RawTransaction
	// blockNum is the block being replayed (used for the EVM block context).
	blockNum uint64
	// tmpDir is the temporary leveldb directory backing the levm. Removed on
	// Close to avoid leaking state across test runs.
	tmpDir string
}

// NewLevmSpecFallback creates a fallback executor backed by an in-process
// levm using an IN-MEMORY database (no leveldb on disk). The witness is
// loaded from the dataset and injected into the EVM stateDB so that contract
// code and storage are available for execution.
//
// Memory backing is critical for concurrent pools: N workers each with their
// own leveldb would cause IOPS contention; in-memory StateDB has no disk I/O
// at all, so snapshot/revert is pure journal manipulation.
func NewLevmSpecFallback(ds *DatasetReader, blockNum uint64) (*LevmSpecFallback, error) {
	txs, err := ds.LoadBlockTxs(blockNum)
	if err != nil {
		return nil, fmt.Errorf("load block txs %d: %w", blockNum, err)
	}
	witness, err := ds.LoadBlockWitness(blockNum)
	if err != nil {
		return nil, fmt.Errorf("load block witness %d: %w", blockNum, err)
	}

	// In-memory levm — no tmpDir, no leveldb, no disk I/O.
	lvm := levm.NewMemory(new(big.Int).SetUint64(blockNum), common.Address{})

	sdb := lvm.GetStateDB()
	if sdb == nil {
		return nil, fmt.Errorf("levm stateDB is nil")
	}
	if err := injectWitnessIntoStateDB(sdb, witness); err != nil {
		return nil, fmt.Errorf("inject witness: %w", err)
	}

	return &LevmSpecFallback{
		lvm:      lvm,
		txs:      txs,
		blockNum: blockNum,
	}, nil
}

// SetTransactions is a no-op kept for API symmetry with the old direct
// executor; transactions are already loaded in NewLevmSpecFallback.
func (f *LevmSpecFallback) SetTransactions(txs []RawTransaction) { f.txs = txs }

// injectWitnessIntoStateDB writes every witness account (balance, nonce,
// code, storage) into the given stateDB. This makes the EVM behave as if the
// block's pre-state is already committed, so ReplayTransaction can execute
// against real on-chain state.
func injectWitnessIntoStateDB(sdb *state.StateDB, witness *ReplayBlockWitness) error {
	if witness == nil {
		return nil
	}
	for addrHex, acct := range witness.Accounts {
		if !common.IsHexAddress(addrHex) {
			continue
		}
		addr := common.HexToAddress(addrHex)
		sdb.CreateAccount(addr)

		if acct.Balance != "" {
			bal := new(big.Int)
			if _, ok := bal.SetString(acct.Balance, 0); ok {
				sdb.SetBalance(addr, bal)
			}
		}
		if acct.Nonce != "" {
			nonce := new(big.Int)
			if _, ok := nonce.SetString(acct.Nonce, 0); ok {
				sdb.SetNonce(addr, nonce.Uint64())
			}
		}
		if acct.Code != "" {
			sdb.SetCode(addr, common.FromHex(acct.Code))
		}
		if acct.Storage != nil {
			for slot, val := range acct.Storage {
				sdb.SetState(addr, common.HexToHash(slot), common.HexToHash(val))
			}
		}
	}
	return nil
}

// buildEthTxFromRaw constructs a *core.EthTransaction from a raw mainnet tx.
// Shared by PreExecute and ReExecute.
func buildEthTxFromRaw(tx RawTransaction) (from common.Address, toPtr *common.Address, ethTx *core.EthTransaction) {
	from = common.HexToAddress(tx.From)
	if tx.To != "" && tx.To != "0x" {
		t := common.HexToAddress(tx.To)
		toPtr = &t
	}
	value := new(big.Int)
	if tx.Value != "" {
		value.SetString(tx.Value, 0)
	}
	data := common.FromHex(tx.Input)
	ethTx = core.NewEthTransaction(0, &from, toPtr, value, 50_000_000, big.NewInt(0), data)
	return from, toPtr, ethTx
}

// PreExecute runs a single mainnet tx through the in-process levm and returns
// the captured read/write keys as a *core.ReplayRWSet. Implements SpecFallback.
//
// State changes are reverted after execution so the next call starts from the
// witness baseline (PreExecute is speculative — it should not accumulate
// state across txs).
func (f *LevmSpecFallback) PreExecute(blockNum uint64, txIdx int) (*core.ReplayRWSet, error) {
	if f.lvm == nil {
		return nil, fmt.Errorf("LevmSpecFallback: lvm not initialized")
	}
	if txIdx < 0 || txIdx >= len(f.txs) {
		return nil, fmt.Errorf("LevmSpecFallback: txIdx %d out of range [0,%d)", txIdx, len(f.txs))
	}
	tx := f.txs[txIdx]

	from, toPtr, ethTx := buildEthTxFromRaw(tx)

	sdb := f.lvm.GetStateDB()
	snap := sdb.Snapshot()

	f.lvm.NewEVMNoTrace(new(big.Int).SetUint64(blockNum), from)
	gasPool := new(ecore.GasPool).AddGas(1_000_000_000)
	rMap, wMap, _, err := f.lvm.ReplayTransaction(*ethTx, gasPool)

	// Always revert to keep the witness baseline for the next PreExecute call.
	sdb.RevertToSnapshot(snap)

	// Canonical contract address for slot-key prefixing. Contract creation txs
	// have no "to"; their writes go to the new contract address, which is
	// derived from (from, nonce). For now, treat creation writes as having an
	// empty address prefix — they are rare in mainnet replay and don't affect
	// the depurge conflict logic (creation storage is isolated).
	var contractAddr common.Address
	if toPtr != nil {
		contractAddr = *toPtr
	}

	ref := core.ReplayRef{
		BlockNum: blockNum,
		TxIndex:  txIdx,
		TxHash:   tx.Hash,
	}
	if err != nil {
		return &core.ReplayRWSet{
			Ref:       ref,
			Success:   false,
			Error:     err.Error(),
			ReadKeys:  storageKeysToStrings(rMap, contractAddr),
			WriteKeys: storageKeysToStrings(wMap, contractAddr),
		}, nil
	}
	return &core.ReplayRWSet{
		Ref:       ref,
		Success:   true,
		ReadKeys:  storageKeysToStrings(rMap, contractAddr),
		WriteKeys: storageKeysToStrings(wMap, contractAddr),
	}, nil
}

// PreExecuteCommit runs a single mainnet tx through the in-process levm and
// returns the captured read/write keys — same as PreExecute, BUT does NOT
// revert state. State changes are committed to the stateDB so the next tx
// sees them.
//
// This is the SERIAL BASELINE path: mirrors test.go's TestSerialExecution,
// which executes all txs in order with state accumulating between txs (no
// snapshot isolation, no aborts, no parallelism). Used to measure Depurge's
// speedup vs the serial baseline.
func (f *LevmSpecFallback) PreExecuteCommit(blockNum uint64, txIdx int) (*core.ReplayRWSet, error) {
	if f.lvm == nil {
		return nil, fmt.Errorf("LevmSpecFallback: lvm not initialized")
	}
	if txIdx < 0 || txIdx >= len(f.txs) {
		return nil, fmt.Errorf("LevmSpecFallback: txIdx %d out of range [0,%d)", txIdx, len(f.txs))
	}
	tx := f.txs[txIdx]

	from, toPtr, ethTx := buildEthTxFromRaw(tx)

	// NO snapshot — let state changes persist for the next tx.
	f.lvm.NewEVMNoTrace(new(big.Int).SetUint64(blockNum), from)
	gasPool := new(ecore.GasPool).AddGas(1_000_000_000)
	rMap, wMap, _, err := f.lvm.ReplayTransaction(*ethTx, gasPool)

	var contractAddr common.Address
	if toPtr != nil {
		contractAddr = *toPtr
	}

	ref := core.ReplayRef{
		BlockNum: blockNum,
		TxIndex:  txIdx,
		TxHash:   tx.Hash,
	}
	if err != nil {
		return &core.ReplayRWSet{
			Ref:       ref,
			Success:   false,
			Error:     err.Error(),
			ReadKeys:  storageKeysToStrings(rMap, contractAddr),
			WriteKeys: storageKeysToStrings(wMap, contractAddr),
		}, nil
	}
	return &core.ReplayRWSet{
		Ref:       ref,
		Success:   true,
		ReadKeys:  storageKeysToStrings(rMap, contractAddr),
		WriteKeys: storageKeysToStrings(wMap, contractAddr),
	}, nil
}

// ReExecuteResult is the output of ReExecute. It contains the real read/write
// keys captured during execution, plus the write delta (new - old) for every
// written key, computed using Solidity uint256 underflow semantics.
type ReExecuteResult struct {
	RealReadKeys  []string
	RealWriteKeys []string
	// WriteDelta maps canonical key → delta (new - old, with 2^256 wrap-around
	// converted to a negative Go int for underflow cases).
	WriteDelta map[string]*big.Int
	// WriteValues maps canonical key → absolute post-execution hex value
	// (0x-prefixed). Used to initialize committedState for keys that haven't
	// been seen before (delta + 0 ≠ witness_value + delta).
	WriteValues map[string]string
}

// ReExecute runs a single mainnet tx on top of the witness baseline + an
// incremental overlay of committed deltas from prior txs, then reverts so the
// witness baseline is preserved.
//
// IMPORTANT (optimization): overlay should contain ONLY the keys written by
// previously-committed txs (the "committed delta"), NOT the full committedState.
// The witness baseline (4875+ keys for mainnet blocks) is already injected into
// sdb during NewLevmSpecFallback and is preserved across calls via
// Snapshot/Revert. Re-applying it every call would cost ~500µs/tx (clone +
// SetState for every witness key); passing only the incremental overlay reduces
// this to ~10-50µs/tx (a few dozen to a few hundred keys).
//
// Delta computation uses a dual-snapshot technique to read pre/post-execution
// values at the correct points in time:
//  1. snapWitness = Snapshot()                     // witness baseline
//  2. applyStateOverride(sdb, overlay)             // inject prior-tx deltas
//  3. snapPre = Snapshot()                         // pre-execution state
//  4. ReplayTransaction → rMap, wMap, realWriteKeys
//  5. read writeBig (post-execution, from wMap or sdb.GetState)
//  6. RevertToSnapshot(snapPre)                    // back to pre-execution
//  7. read readBig (pre-execution, from rMap or sdb.GetState)
//  8. delta = writeBig - readBig (with uint256 underflow handling)
//  9. RevertToSnapshot(snapWitness)                // back to witness baseline
//
// Pass nil overlay to execute purely against the witness baseline.
func (f *LevmSpecFallback) ReExecute(blockNum uint64, txIdx int, overlay map[string]string) (*ReExecuteResult, error) {
	if f.lvm == nil {
		return nil, fmt.Errorf("LevmSpecFallback: lvm not initialized")
	}
	if txIdx < 0 || txIdx >= len(f.txs) {
		return nil, fmt.Errorf("LevmSpecFallback: txIdx %d out of range [0,%d)", txIdx, len(f.txs))
	}
	tx := f.txs[txIdx]

	from, toPtr, ethTx := buildEthTxFromRaw(tx)

	sdb := f.lvm.GetStateDB()
	snapWitness := sdb.Snapshot()

	// Apply the incremental overlay (prior-tx committed deltas) on top of the
	// witness baseline.  This is small (only keys written by prior txs).
	if overlay != nil {
		if err := applyStateOverride(sdb, overlay); err != nil {
			sdb.RevertToSnapshot(snapWitness)
			return nil, fmt.Errorf("apply state overlay: %w", err)
		}
	}
	snapPre := sdb.Snapshot() // pre-execution state = witness + overlay

	f.lvm.NewEVMNoTrace(new(big.Int).SetUint64(blockNum), from)
	gasPool := new(ecore.GasPool).AddGas(1_000_000_000)
	rMap, wMap, _, err := f.lvm.ReplayTransaction(*ethTx, gasPool)

	var contractAddr common.Address
	if toPtr != nil {
		contractAddr = *toPtr
	}
	realReadKeys := storageKeysToStrings(rMap, contractAddr)
	realWriteKeys := storageKeysToStrings(wMap, contractAddr)

	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	two255 := new(big.Int).Rsh(two256, 1)

	writeDelta := make(map[string]*big.Int, len(realWriteKeys))
	writeValues := make(map[string]string, len(realWriteKeys))

	// Step 5: read writeBig (post-execution) BEFORE reverting.
	for _, keyStr := range realWriteKeys {
		_, addr, slot, _, ok := parseLevmCanonicalKey(keyStr)
		if !ok {
			continue
		}
		writeBig := new(big.Int)
		if wMap != nil {
			if v, ok := wMap[slot]; ok {
				writeBig.SetBytes(v.Bytes())
			} else {
				writeBig.SetBytes(sdb.GetState(addr, slot).Bytes())
			}
		} else {
			writeBig.SetBytes(sdb.GetState(addr, slot).Bytes())
		}
		writeValues[keyStr] = fmt.Sprintf("0x%x", writeBig)
	}

	// Step 6: revert to pre-execution state to read readBig.
	sdb.RevertToSnapshot(snapPre)

	// Step 7-8: read readBig (pre-execution) and compute delta.
	for _, keyStr := range realWriteKeys {
		_, addr, slot, _, ok := parseLevmCanonicalKey(keyStr)
		if !ok {
			continue
		}
		writeBig, _ := new(big.Int).SetString(strings.TrimPrefix(writeValues[keyStr], "0x"), 16)

		// readBig (pre-execution): rMap (tx actually read) → sdb.GetState (pre).
		readBig := big.NewInt(0)
		if rMap != nil {
			if v, ok := rMap[slot]; ok {
				readBig.SetBytes(v.Bytes())
			} else {
				readBig.SetBytes(sdb.GetState(addr, slot).Bytes())
			}
		} else {
			readBig.SetBytes(sdb.GetState(addr, slot).Bytes())
		}

		delta := new(big.Int).Sub(writeBig, readBig)
		if delta.Sign() < 0 {
			delta = new(big.Int).Add(delta, two256)
		}
		if delta.Cmp(two255) >= 0 {
			delta = new(big.Int).Sub(delta, two256)
		}
		writeDelta[keyStr] = delta
	}

	// Step 9: revert to witness baseline for the next call.
	sdb.RevertToSnapshot(snapWitness)

	if err != nil {
		return &ReExecuteResult{
			RealReadKeys:  realReadKeys,
			RealWriteKeys: realWriteKeys,
			WriteDelta:    writeDelta,
			WriteValues:   writeValues,
		}, fmt.Errorf("levm ReplayTransaction: %w", err)
	}

	return &ReExecuteResult{
		RealReadKeys:  realReadKeys,
		RealWriteKeys: realWriteKeys,
		WriteDelta:    writeDelta,
		WriteValues:   writeValues,
	}, nil
}

// ReExecuteCommit runs a single mainnet tx like ReExecute, BUT does NOT
// revert state — state changes are committed to the stateDB so the next tx
// sees them. This is the SERIAL REPLAY path for aborted txs: they execute
// sequentially with state accumulating, mirroring the serial baseline.
//
// The stateOverride passed in is the committedState snapshot BEFORE this tx;
// it is applied once on top of the witness baseline, and then this tx's
// writes are committed on top. The caller does NOT need to merge deltas back
// into committedState because the stateDB itself is the source of truth
// (unlike ReExecute where the stateDB is reverted and committedState must
// track deltas externally).
//
// For this reason, ReExecuteCommit is only safe to call serially (one tx at
// a time on a given levm instance).
func (f *LevmSpecFallback) ReExecuteCommit(blockNum uint64, txIdx int, stateOverride map[string]string) (*ReExecuteResult, error) {
	if f.lvm == nil {
		return nil, fmt.Errorf("LevmSpecFallback: lvm not initialized")
	}
	if txIdx < 0 || txIdx >= len(f.txs) {
		return nil, fmt.Errorf("LevmSpecFallback: txIdx %d out of range [0,%d)", txIdx, len(f.txs))
	}
	tx := f.txs[txIdx]

	from, toPtr, ethTx := buildEthTxFromRaw(tx)

	sdb := f.lvm.GetStateDB()

	// Apply the committed-state overlay ONCE on top of the witness baseline.
	// After the first tx in the serial replay, the stateDB already carries
	// accumulated writes from previous txs, so stateOverride is only needed
	// for the FIRST tx (or when stateOverride is non-nil). To keep it simple
	// and correct, we apply the overlay every time — the overhead is small
	// (map iteration + SetState per key) compared to the EVM execution itself.
	// DO NOT snapshot/revert — let writes persist.
	if stateOverride != nil {
		if err := applyStateOverride(sdb, stateOverride); err != nil {
			return nil, fmt.Errorf("apply state override: %w", err)
		}
	}

	f.lvm.NewEVMNoTrace(new(big.Int).SetUint64(blockNum), from)
	gasPool := new(ecore.GasPool).AddGas(1_000_000_000)
	rMap, wMap, _, err := f.lvm.ReplayTransaction(*ethTx, gasPool)

	var contractAddr common.Address
	if toPtr != nil {
		contractAddr = *toPtr
	}
	realReadKeys := storageKeysToStrings(rMap, contractAddr)
	realWriteKeys := storageKeysToStrings(wMap, contractAddr)

	// Deltas are computed for caller-side reporting only; the stateDB has
	// already been updated in place by ReplayTransaction.
	writeDelta := make(map[string]*big.Int, len(realWriteKeys))
	writeValues := make(map[string]string, len(realWriteKeys))
	collectDeltasAndValues(sdb, rMap, wMap, contractAddr, realWriteKeys, stateOverride, writeDelta, writeValues)

	if err != nil {
		return &ReExecuteResult{
			RealReadKeys:  realReadKeys,
			RealWriteKeys: realWriteKeys,
			WriteDelta:    writeDelta,
			WriteValues:   writeValues,
		}, fmt.Errorf("levm ReplayTransaction: %w", err)
	}
	return &ReExecuteResult{
		RealReadKeys:  realReadKeys,
		RealWriteKeys: realWriteKeys,
		WriteDelta:    writeDelta,
		WriteValues:   writeValues,
	}, nil
}

// applyStateOverride injects a canonical-key → hex-value map into sdb.
// Recognized key formats (matching augmentAccountKeys / collectWriteValues):
//
//	acct:<0xaddr>:balance
//	acct:<0xaddr>:nonce
//	acct:<0xaddr>:code
//	acct:<0xaddr>:exist
//	slot:<0xaddr>:<0xslot>
//
// Unrecognized keys are silently skipped.
func applyStateOverride(sdb *state.StateDB, stateOverride map[string]string) error {
	for key, valHex := range stateOverride {
		prefix, addr, slot, field, ok := parseLevmCanonicalKey(key)
		if !ok {
			continue
		}
		switch prefix {
		case "acct":
			switch field {
			case "balance":
				bal := new(big.Int)
				if _, ok := bal.SetString(strings.TrimPrefix(valHex, "0x"), 16); ok {
					sdb.SetBalance(addr, bal)
				}
			case "nonce":
				n := new(big.Int)
				if _, ok := n.SetString(strings.TrimPrefix(valHex, "0x"), 16); ok {
					sdb.SetNonce(addr, n.Uint64())
				}
			case "code":
				sdb.SetCode(addr, common.FromHex(valHex))
			case "exist":
				if valHex != "" && valHex != "0x0" && valHex != "0" {
					if !sdb.Exist(addr) {
						sdb.CreateAccount(addr)
					}
				}
			}
		case "slot":
			sdb.SetState(addr, slot, common.HexToHash(valHex))
		}
	}
	return nil
}

// collectDeltasAndValues computes the write delta (new - old) for every write
// key, using Solidity uint256 underflow semantics:
//
//	delta = (write_value - read_value) mod 2^256
//	values >= 2^255 are converted to negative deltas
//
// It also captures the absolute post-execution value for each write key
// (read directly from sdb BEFORE the snapshot is reverted).
//
// readBig (old value) priority:
//  1. value from rMap[key] (if the tx read the same slot it wrote)
//  2. value from stateOverride[canonicalKey] (committed state)
//  3. 0
//
// writeBig (new value) priority:
//  1. value from wMap[key]
//  2. value read directly from sdb.GetState (post-execution)
func collectDeltasAndValues(
	sdb *state.StateDB,
	rMap, wMap map[common.Hash]common.Hash,
	contractAddr common.Address,
	realWriteKeys []string,
	stateOverride map[string]string,
	writeDelta map[string]*big.Int,
	writeValues map[string]string,
) {
	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	two255 := new(big.Int).Rsh(two256, 1)

	for _, keyStr := range realWriteKeys {
		_, addr, slot, _, ok := parseLevmCanonicalKey(keyStr)
		if !ok {
			continue
		}

		// new (post-execution) value: prefer wMap, fall back to sdb read.
		writeBig := new(big.Int)
		if wMap != nil {
			if v, ok := wMap[slot]; ok {
				writeBig.SetBytes(v.Bytes())
			} else {
				writeBig.SetBytes(sdb.GetState(addr, slot).Bytes())
			}
		} else {
			writeBig.SetBytes(sdb.GetState(addr, slot).Bytes())
		}

		// old (pre-execution) value: rMap → override → 0
		readBig := big.NewInt(0)
		if rMap != nil {
			if v, ok := rMap[slot]; ok {
				readBig.SetBytes(v.Bytes())
			} else if ov, ok := stateOverride[keyStr]; ok {
				if ob, ok := new(big.Int).SetString(strings.TrimPrefix(ov, "0x"), 16); ok {
					readBig.Set(ob)
				}
			}
		} else if ov, ok := stateOverride[keyStr]; ok {
			if ob, ok := new(big.Int).SetString(strings.TrimPrefix(ov, "0x"), 16); ok {
				readBig.Set(ob)
			}
		}

		delta := new(big.Int).Sub(writeBig, readBig)
		if delta.Sign() < 0 {
			delta = new(big.Int).Add(delta, two256)
		}
		if delta.Cmp(two255) >= 0 {
			delta = new(big.Int).Sub(delta, two256)
		}

		writeDelta[keyStr] = delta
		writeValues[keyStr] = fmt.Sprintf("0x%x", writeBig)
	}
}

// parseLevmCanonicalKey parses a canonical key string into its components.
// Mirrors the format produced by augmentAccountKeys in mainnet_spec_executor.go:
//
//	acct:<0xaddr>:<balance|nonce|code|exist>
//	slot:<0xaddr>:<0xslot>
//
// Returns ok=false if the key is not in one of these formats.
func parseLevmCanonicalKey(key string) (prefix string, addr common.Address, slot common.Hash, field string, ok bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) < 2 {
		return "", common.Address{}, common.Hash{}, "", false
	}
	prefix = parts[0]
	switch prefix {
	case "acct":
		if len(parts) != 3 {
			return "", common.Address{}, common.Hash{}, "", false
		}
		addr = common.HexToAddress(parts[1])
		field = parts[2]
		return prefix, addr, common.Hash{}, field, true
	case "slot":
		if len(parts) != 3 {
			return "", common.Address{}, common.Hash{}, "", false
		}
		addr = common.HexToAddress(parts[1])
		slot = common.HexToHash(parts[2])
		return prefix, addr, slot, "", true
	}
	return "", common.Address{}, common.Hash{}, "", false
}

// storageKeysToStrings converts a levm vm.Storage (map of common.Hash → value)
// into a slice of canonical key strings, prefixed with the contract address
// so they match the format produced by LLM augmentAccountKeys.
//
// Format: "slot:<0xaddr_lowercase>:<0xslot_lowercase>"
// Returns nil for a nil map.
//
// NOTE: common.Address.Hex() returns the EIP-55 checksummed (mixed-case) form,
// but LLM analysis / augmentAccountKeys use lowercase. We lowercase the address
// here so levm-captured keys match LLM-produced conservative keys.
func storageKeysToStrings(m map[common.Hash]common.Hash, contractAddr common.Address) []string {
	if m == nil {
		return nil
	}
	addrLower := strings.ToLower(contractAddr.Hex())
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, fmt.Sprintf("slot:%s:%s", addrLower, k.Hex()))
	}
	return out
}

// Close releases the levm and removes the temporary backing database.
func (f *LevmSpecFallback) Close() error {
	if f.lvm != nil {
		f.lvm.Close()
	}
	if f.tmpDir != "" {
		_ = os.RemoveAll(f.tmpDir)
		f.tmpDir = ""
	}
	return nil
}

// ---------------------------------------------------------------------------
// LevmSpecFallbackPool — concurrent worker pool of independent levm instances.
//
// Each worker has its own levm + leveldb + witness-injected stateDB, so there
// is NO shared state and NO lock contention between workers. This is safe to
// run concurrently because PreExecute / ReExecute both snapshot → execute →
// revert, never committing state to the shared baseline.
//
// Unlike the Vegeta baseState scenario (where sharing one trie.Database gives
// cache hits that outweigh lock contention), mainnet replay uses leveldb (no
// shared trie cache) + independent witness injection, so the per-worker cost
// is constant and parallelism scales linearly with NumCPU.
//
// ---------------------------------------------------------------------------

// LevmSpecFallbackPool is a fixed-size pool of LevmSpecFallback workers that
// executes PreExecute / ReExecute concurrently. The number of workers defaults
// to runtime.NumCPU() (matching the project-memory EVM instance concurrency
// limit).
//
// Two usage modes:
//   - Batch (PreExecuteBatch / ReExecuteBatch): fan-out N txs across workers.
//   - Single-tx concurrent (Acquire / Release): each goroutine grabs an idle
//     worker via the idle channel, runs ReExecute/PreExecute, then returns it.
//     This mirrors test.go's InitEVMPool + evmPool.Get/Put pattern so the
//     Depurge validation loop can dispatch one tx per goroutine.
type LevmSpecFallbackPool struct {
	workers  []*LevmSpecFallback
	idle     chan *LevmSpecFallback
	blockNum uint64
	n        int
}

// NewLevmSpecFallbackPool creates a pool of n independent LevmSpecFallback
// instances. If n <= 0, defaults to runtime.NumCPU().
//
// Each worker pays the one-time cost of levm.New + witness injection (~100ms
// each); this is amortized across all PreExecute/ReExecute calls in the block.
func NewLevmSpecFallbackPool(ds *DatasetReader, blockNum uint64, n int) (*LevmSpecFallbackPool, error) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	pool := &LevmSpecFallbackPool{
		workers:  make([]*LevmSpecFallback, n),
		idle:     make(chan *LevmSpecFallback, n),
		blockNum: blockNum,
		n:        n,
	}
	for i := 0; i < n; i++ {
		fb, err := NewLevmSpecFallback(ds, blockNum)
		if err != nil {
			// cleanup already-created workers
			for j := 0; j < i; j++ {
				_ = pool.workers[j].Close()
			}
			return nil, fmt.Errorf("create pool worker %d/%d: %w", i, n, err)
		}
		pool.workers[i] = fb
		pool.idle <- fb
	}
	return pool, nil
}

// Acquire returns an idle LevmSpecFallback worker, blocking until one is
// available. Use Release() to return it. Safe for concurrent use.
//
// Typical usage in an ants worker:
//
//	w, err := pool.Acquire()
//	if err != nil { ... }
//	defer pool.Release(w)
//	result, err := w.ReExecute(...)
func (p *LevmSpecFallbackPool) Acquire() (*LevmSpecFallback, error) {
	if len(p.workers) == 0 {
		return nil, fmt.Errorf("LevmSpecFallbackPool: no workers")
	}
	w := <-p.idle
	if w == nil {
		return nil, fmt.Errorf("LevmSpecFallbackPool: acquired nil worker")
	}
	return w, nil
}

// Release returns a worker to the idle channel. Safe for concurrent use.
// Passing a nil worker is a no-op.
func (p *LevmSpecFallbackPool) Release(w *LevmSpecFallback) {
	if w == nil {
		return
	}
	p.idle <- w
}

// Close releases every worker's levm + temp leveldb.
func (p *LevmSpecFallbackPool) Close() error {
	var firstErr error
	for _, w := range p.workers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PreExecute implements SpecFallback so the pool can be used anywhere a
// SpecFallback is expected (single-tx serial path; mainly for fallback safety).
// Uses worker 0.
func (p *LevmSpecFallbackPool) PreExecute(blockNum uint64, txIdx int) (*core.ReplayRWSet, error) {
	if len(p.workers) == 0 {
		return nil, fmt.Errorf("LevmSpecFallbackPool: no workers")
	}
	return p.workers[0].PreExecute(blockNum, txIdx)
}

// ReExecute single-tx path (uses worker 0). Mainly for serial safety.
func (p *LevmSpecFallbackPool) ReExecute(blockNum uint64, txIdx int, stateOverride map[string]string) (*ReExecuteResult, error) {
	if len(p.workers) == 0 {
		return nil, fmt.Errorf("LevmSpecFallbackPool: no workers")
	}
	return p.workers[0].ReExecute(blockNum, txIdx, stateOverride)
}

// PreExecuteBatch concurrently runs PreExecute for every txIdx in txIndices.
// Returns results and errors indexed by POSITION in txIndices (NOT by txIdx).
// If a worker's PreExecute returns an error, the corresponding result is nil
// and the error is set in errs[pos].
//
// Concurrency = min(len(txIndices), p.n). Each worker processes jobs from a
// shared channel; there is no per-tx goroutine spawn overhead.
func (p *LevmSpecFallbackPool) PreExecuteBatch(txIndices []int) ([]*core.ReplayRWSet, []error) {
	out := make([]*core.ReplayRWSet, len(txIndices))
	errs := make([]error, len(txIndices))
	if len(txIndices) == 0 {
		return out, errs
	}

	type job struct {
		pos   int
		txIdx int
	}
	type result struct {
		pos int
		rw  *core.ReplayRWSet
		err error
	}

	jobs := make(chan job, len(txIndices))
	results := make(chan result, len(txIndices))

	// producer
	go func() {
		for i, txIdx := range txIndices {
			jobs <- job{i, txIdx}
		}
		close(jobs)
	}()

	// workers
	var wg sync.WaitGroup
	workers := p.n
	if workers > len(txIndices) {
		workers = len(txIndices)
	}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		worker := p.workers[w]
		go func() {
			defer wg.Done()
			for j := range jobs {
				rw, err := worker.PreExecute(p.blockNum, j.txIdx)
				results <- result{j.pos, rw, err}
			}
		}()
	}

	// collector (closes results when all workers done)
	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		out[r.pos] = r.rw
		errs[r.pos] = r.err
	}
	return out, errs
}

// ReExecuteBatch concurrently runs ReExecute for every (txIdx, stateOverride)
// pair. stateOverrides[pos] is the override map passed to the worker for
// txIndices[pos]; pass nil to execute against the pure witness baseline.
//
// All workers in a batch typically receive the SAME stateOverride snapshot
// (batch-level snapshot isolation, per project memory: "all transactions in a
// batch must read from the same committedState snapshot and merge writes after
// all complete"). The caller is responsible for merging deltas serially
// after the batch completes.
func (p *LevmSpecFallbackPool) ReExecuteBatch(txIndices []int, stateOverrides []map[string]string) ([]*ReExecuteResult, []error) {
	if len(stateOverrides) != len(txIndices) {
		return nil, []error{fmt.Errorf("ReExecuteBatch: len(txIndices)=%d != len(stateOverrides)=%d", len(txIndices), len(stateOverrides))}
	}
	out := make([]*ReExecuteResult, len(txIndices))
	errs := make([]error, len(txIndices))
	if len(txIndices) == 0 {
		return out, errs
	}

	type job struct {
		pos           int
		txIdx         int
		stateOverride map[string]string
	}
	type result struct {
		pos    int
		result *ReExecuteResult
		err    error
	}

	jobs := make(chan job, len(txIndices))
	results := make(chan result, len(txIndices))

	go func() {
		for i, txIdx := range txIndices {
			jobs <- job{i, txIdx, stateOverrides[i]}
		}
		close(jobs)
	}()

	var wg sync.WaitGroup
	workers := p.n
	if workers > len(txIndices) {
		workers = len(txIndices)
	}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		worker := p.workers[w]
		go func() {
			defer wg.Done()
			for j := range jobs {
				res, err := worker.ReExecute(p.blockNum, j.txIdx, j.stateOverride)
				results <- result{j.pos, res, err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		out[r.pos] = r.result
		errs[r.pos] = r.err
	}
	return out, errs
}

// SetTransactions forwards to every worker (kept for API symmetry).
func (p *LevmSpecFallbackPool) SetTransactions(txs []RawTransaction) {
	for _, w := range p.workers {
		w.SetTransactions(txs)
	}
}
