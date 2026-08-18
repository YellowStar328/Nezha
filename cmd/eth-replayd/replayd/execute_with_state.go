package replayd

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/holiman/uint256"
)

// ExecWithStateResult is the result of executing a single tx on top of an
// injected state overlay. It contains the captured RW keys (via rwTracker)
// and the absolute post-execution values for every written key, so the
// client can update its committedState by direct overwrite (no delta math
// needed on the server side).
type ExecWithStateResult struct {
	TxIndex     int               `json:"txIndex"`
	TxHash      string            `json:"txHash"`
	Success     bool              `json:"success"`
	GasUsed     uint64            `json:"gasUsed"`
	ReadKeys    []string          `json:"readKeys"`
	WriteKeys   []string          `json:"writeKeys"`
	WriteValues map[string]string `json:"writeValues"` // canonical key -> hex value (post-exec)
	Error       string            `json:"error,omitempty"`
}

// parseCanonicalKey parses a canonical key string into its components.
//
// Formats:
//
//	acct:<0xaddr>:balance
//	acct:<0xaddr>:nonce
//	acct:<0xaddr>:code
//	acct:<0xaddr>:exist
//	slot:<0xaddr>:<0xslot>
func parseCanonicalKey(key string) (prefix string, addr common.Address, slot common.Hash, field string, ok bool) {
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

// injectStateIntoStateDB writes the given canonical key→hex-value map into a
// StateDB. This is the "state injection" step that lets us re-execute a tx on
// top of the client-supplied committed state snapshot.
//
// Keys not present in the map fall back to the base state's existing values
// (from witness injection during LoadBlock).
func injectStateIntoStateDB(sdb *state.StateDB, stateMap map[string]string) error {
	for key, valHex := range stateMap {
		prefix, addr, slot, field, ok := parseCanonicalKey(key)
		if !ok {
			continue // skip unrecognized keys
		}
		switch prefix {
		case "acct":
			switch field {
			case "balance":
				bal := new(uint256.Int)
				if err := bal.SetFromHex(valHex); err != nil {
					return fmt.Errorf("invalid balance hex %q for key %s: %w", valHex, key, err)
				}
				sdb.SetBalance(addr, bal, tracing.BalanceChangeReason(0))
			case "nonce":
				n, ok := new(big.Int).SetString(valHex, 0)
				if !ok {
					return fmt.Errorf("invalid nonce %q for key %s", valHex, key)
				}
				sdb.SetNonce(addr, n.Uint64(), tracing.NonceChangeReason(0))
			case "code":
				sdb.SetCode(addr, common.FromHex(valHex), tracing.CodeChangeReason(0))
			case "exist":
				// "0x1" or non-empty → ensure account exists; "0x0"/empty → skip
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

// collectWriteValues reads the post-execution value for every write key from
// the StateDB. Must be called on a state that has NOT been reverted (i.e. after
// a successful execution). Reads the raw StateDB directly, not through any
// tracker wrapper, to avoid polluting read sets.
func collectWriteValues(sdb *state.StateDB, writeKeys []string) map[string]string {
	out := make(map[string]string, len(writeKeys))
	for _, key := range writeKeys {
		prefix, addr, slot, field, ok := parseCanonicalKey(key)
		if !ok {
			continue
		}
		switch prefix {
		case "acct":
			switch field {
			case "balance":
				out[key] = sdb.GetBalance(addr).Hex()
			case "nonce":
				out[key] = fmt.Sprintf("0x%x", sdb.GetNonce(addr))
			case "code":
				out[key] = fmt.Sprintf("0x%x", sdb.GetCode(addr))
			case "exist":
				if sdb.Exist(addr) {
					out[key] = "0x1"
				} else {
					out[key] = "0x0"
				}
			}
		case "slot":
			out[key] = sdb.GetState(addr, slot).Hex()
		}
	}
	return out
}

// TxWithDeltaResult is the result of executing a tx with delta-based state
// tracking. Contains the captured RW keys, write deltas (new - old for each
// write key), and absolute post-execution write values.
//
// WriteValues stores the absolute post-execution value for each write key.
// This is needed by the caller to initialize committedState for keys that
// haven't been seen before (delta + 0 ≠ witness_value + delta).
type TxWithDeltaResult struct {
	TxIndex     int
	TxHash      string
	Success     bool
	GasUsed     uint64
	ReadKeys    []string
	WriteKeys   []string
	WriteDelta  map[string]*big.Int // canonical key → delta (new - old)
	WriteValues map[string]string   // canonical key → absolute post-execution hex value
	Error       string
}

// ExecuteTxWithState runs a single tx on a fresh clone of the base state,
// after injecting the client-supplied state overlay. Returns the captured RW
// keys (via rwTracker) and post-execution write values.
//
// This is the stateful re-execution primitive used by Depurge's validation
// phase: the client supplies the current committed state snapshot, and the
// server executes the tx on top of it and returns the real RW keys + absolute
// post-exec write values. The client then overwrites its committedState with
// those values (no delta computation needed server-side).
//
// Concurrency: each call clones the base state, so concurrent calls are fully
// isolated. The client is responsible for ensuring that concurrently-executed
// txs have non-overlapping write keys (guaranteed by the Depurge scheduler).
func (te *TxExecutor) ExecuteTxWithState(
	baseState *state.StateDB,
	blockEnv *BlockEnv,
	txRaw interface{},
	txIdx int,
	injectedState map[string]string,
) (*ExecWithStateResult, error) {
	if baseState == nil {
		return nil, fmt.Errorf("baseState is nil")
	}

	// Clone the base state so this call is fully isolated from concurrent calls.
	cloneState := baseState.Copy()

	// Inject the client-supplied committed state overlay on top of the clone.
	if err := injectStateIntoStateDB(cloneState, injectedState); err != nil {
		return nil, fmt.Errorf("inject state: %w", err)
	}

	// Execute on the injected state. PreExecuteTx wraps with rwTracker and
	// handles gas/nonce/value-transfer semantics.
	preResult, err := te.PreExecuteTx(cloneState, blockEnv, txRaw, txIdx)
	if err != nil {
		return nil, fmt.Errorf("PreExecuteTx: %w", err)
	}

	result := &ExecWithStateResult{
		TxIndex:   preResult.TxIndex,
		TxHash:    preResult.TxHash,
		Success:   preResult.Success,
		GasUsed:   preResult.GasUsed,
		ReadKeys:  preResult.ReadKeys,
		WriteKeys: preResult.WriteKeys,
		Error:     preResult.Error,
	}

	// Collect post-execution write values for the client to update its
	// committedState. Only meaningful when execution succeeded (state was not
	// reverted); on failure the client will abort the tx anyway.
	if preResult.Success {
		result.WriteValues = collectWriteValues(cloneState, preResult.WriteKeys)
	} else {
		result.WriteValues = make(map[string]string)
	}

	return result, nil
}

// ExecuteTxWithDelta runs a single tx on a fresh clone of the base state,
// after injecting the client-supplied state overlay. Returns the captured RW
// keys and write deltas (new - old for each write key).
//
// Uses two clones: refState (post-injection, pre-execution) for reading old
// values, and cloneState (post-execution) for reading new values. Delta is
// computed as new - old with proper underflow handling.
func (te *TxExecutor) ExecuteTxWithDelta(
	baseState *state.StateDB,
	blockEnv *BlockEnv,
	txRaw interface{},
	txIdx int,
	injectedState map[string]string,
) (*TxWithDeltaResult, error) {
	if baseState == nil {
		return nil, fmt.Errorf("baseState is nil")
	}

	cloneState := baseState.Copy()
	if err := injectStateIntoStateDB(cloneState, injectedState); err != nil {
		return nil, fmt.Errorf("inject state: %w", err)
	}

	refState := cloneState.Copy()

	preResult, err := te.PreExecuteTx(cloneState, blockEnv, txRaw, txIdx)
	if err != nil {
		return nil, fmt.Errorf("PreExecuteTx: %w", err)
	}

	result := &TxWithDeltaResult{
		TxIndex:   preResult.TxIndex,
		TxHash:    preResult.TxHash,
		Success:   preResult.Success,
		GasUsed:   preResult.GasUsed,
		ReadKeys:  preResult.ReadKeys,
		WriteKeys: preResult.WriteKeys,
		Error:     preResult.Error,
	}

	if preResult.Success {
		newValues := collectWriteValues(cloneState, preResult.WriteKeys)
		oldValues := collectWriteValues(refState, preResult.WriteKeys)
		result.WriteDelta = make(map[string]*big.Int, len(preResult.WriteKeys))
		result.WriteValues = newValues
		for key, newVal := range newValues {
			oldVal := oldValues[key]
			result.WriteDelta[key] = computeWriteDelta(oldVal, newVal)
		}
	} else {
		result.WriteDelta = make(map[string]*big.Int)
		result.WriteValues = make(map[string]string)
	}

	return result, nil
}

// computeWriteDelta computes newVal - oldVal as a big.Int, handling
// underflow by converting to the Solidity signed-delta convention:
// values >= 2^255 are converted to negative (2's complement).
func computeWriteDelta(oldValHex, newValHex string) *big.Int {
	oldVal := hexToBigInt(oldValHex)
	newVal := hexToBigInt(newValHex)

	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	two255 := new(big.Int).Rsh(two256, 1)

	delta := new(big.Int).Sub(newVal, oldVal)
	if delta.Sign() < 0 {
		delta.Add(delta, two256)
	}
	if delta.Cmp(two255) >= 0 {
		delta.Sub(delta, two256)
	}
	return delta
}

func hexToBigInt(hex string) *big.Int {
	if hex == "" || hex == "0x0" || hex == "0" {
		return big.NewInt(0)
	}
	val, ok := new(big.Int).SetString(hex, 0)
	if !ok {
		return big.NewInt(0)
	}
	return val
}

// ExecuteTxWithDeltaDirect runs a single tx using the executor's stored
// baseState and blockEnv (set via SetBaseState/SetBlockEnv). This avoids
// passing *state.StateDB across module boundaries, which would cause type
// mismatches when the caller uses a different go-ethereum version.
func (te *TxExecutor) ExecuteTxWithDeltaDirect(
	txRaw interface{},
	txIdx int,
	injectedState map[string]string,
) (*TxWithDeltaResult, error) {
	if te.baseState == nil {
		return nil, fmt.Errorf("executor baseState is not set")
	}
	if te.blockEnv == nil {
		return nil, fmt.Errorf("executor blockEnv is not set")
	}
	return te.ExecuteTxWithDelta(te.baseState, te.blockEnv, txRaw, txIdx, injectedState)
}

// ExecuteTxWithStateDirect runs a single tx using the executor's stored
// baseState and blockEnv. See ExecuteTxWithDeltaDirect for rationale.
func (te *TxExecutor) ExecuteTxWithStateDirect(
	txRaw interface{},
	txIdx int,
	injectedState map[string]string,
) (*ExecWithStateResult, error) {
	if te.baseState == nil {
		return nil, fmt.Errorf("executor baseState is not set")
	}
	if te.blockEnv == nil {
		return nil, fmt.Errorf("executor blockEnv is not set")
	}
	return te.ExecuteTxWithState(te.baseState, te.blockEnv, txRaw, txIdx, injectedState)
}
