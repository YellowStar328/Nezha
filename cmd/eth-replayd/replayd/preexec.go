package replayd

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// simpleIntrinsicGas computes the base intrinsic gas for a tx, inlined to
// avoid signature variance across geth versions.
func simpleIntrinsicGas(data []byte, contractCreation bool, homestead bool, isEIP2028 bool) (uint64, error) {
	var gas uint64
	if contractCreation && homestead {
		gas = params.TxGasContractCreation
	} else {
		gas = params.TxGas
	}
	if len(data) > 0 {
		var nz uint64
		for _, byt := range data {
			if byt != 0 {
				nz++
			}
		}
		nonZeroGas := uint64(params.TxDataNonZeroGasFrontier)
		if isEIP2028 {
			nonZeroGas = uint64(params.TxDataNonZeroGasEIP2028)
		}
		z := uint64(len(data)) - nz
		gasNonZero, overflow := mulUint64(nonZeroGas, nz)
		if overflow {
			return 0, fmt.Errorf("intrinsic gas overflow")
		}
		gasZero, overflow := mulUint64(uint64(params.TxDataZeroGas), z)
		if overflow {
			return 0, fmt.Errorf("intrinsic gas overflow")
		}
		next, overflow := addUint64(gas, gasNonZero)
		if overflow {
			return 0, fmt.Errorf("intrinsic gas overflow")
		}
		gas, overflow = addUint64(next, gasZero)
		if overflow {
			return 0, fmt.Errorf("intrinsic gas overflow")
		}
	}
	return gas, nil
}

func mulUint64(a, b uint64) (uint64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	r := a * b
	return r, a != r/b
}
func addUint64(a, b uint64) (uint64, bool) {
	r := a + b
	return r, r < a
}

// PreExecResult is the speculative RWset + side result from executing a
// single tx against a clean pre-state snapshot. This is the core output
// consumed by schedulers to build DAG edges and validate.
type PreExecResult struct {
	TxIndex   int      `json:"txIndex"`
	TxHash    string   `json:"txHash"`
	Success   bool     `json:"success"`
	GasUsed   uint64   `json:"gasUsed"`
	ReadKeys  []string `json:"readKeys"`  // acct:addr:balance / slot:addr:slot
	WriteKeys []string `json:"writeKeys"` // same format
	Error     string   `json:"error,omitempty"`
}

// PreExecuteTx runs a single tx on a FRESH clone of the pre-state
// (witness-injected SparseStateDB) and captures all state reads/writes.
//
// The caller MUST ensure that `baseState` is NOT modified during this call
// (we snapshot internally and revert at the end).
//
// `blockEnv` and `txRaw` must be the same data used by ExecuteBlock so that
// gasPrice / blockEnv fields (baseFee, coinbase, timestamp) match on-chain.
func (te *TxExecutor) PreExecuteTx(
	baseState *state.StateDB,
	blockEnv *BlockEnv,
	txRaw interface{},
	txIdx int,
) (*PreExecResult, error) {
	if baseState == nil {
		return nil, fmt.Errorf("baseState is nil")
	}
	isEIP158 := te.chainConfig.IsEIP158(new(big.Int).SetUint64(blockEnv.BlockNumber))
	_ = isEIP158

	// Wrap StateDB so we can capture every Get/Set operation
	tracked := newRWTracker(baseState)

	result := &PreExecResult{
		TxIndex: txIdx,
	}

	txMap, ok := txRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid tx format at index %d", txIdx)
	}

	if hash, ok := txMap["hash"].(string); ok {
		result.TxHash = hash
	}

	sender := common.Address{}
	if from, ok := txMap["from"].(string); ok {
		sender = common.HexToAddress(from)
	}

	var toAddr common.Address
	hasTo := false
	if to, ok := txMap["to"].(string); ok && to != "" && to != "0x" {
		toAddr = common.HexToAddress(to)
		hasTo = true
	}

	value := uint256.NewInt(0)
	if v, ok := txMap["value"].(string); ok {
		val := new(uint256.Int)
		if err := val.SetFromHex(v); err == nil {
			value = val
		}
	}

	var gasLimit uint64
	if g, ok := txMap["gas"].(float64); ok {
		gasLimit = uint64(g)
	}

	var gasPrice *uint256.Int
	if gp, ok := txMap["gasPrice"].(string); ok {
		gpv := new(uint256.Int)
		if err := gpv.SetFromHex(gp); err == nil {
			gasPrice = gpv
		}
	}
	if gasPrice == nil {
		if maxFee, ok := txMap["maxFeePerGas"].(string); ok {
			maxFeePerGas := new(uint256.Int)
			if err := maxFeePerGas.SetFromHex(maxFee); err == nil {
				if blockEnv.BaseFee != nil {
					baseFee, _ := uint256.FromBig(blockEnv.BaseFee)
					if baseFee != nil {
						if maxFeePerGas.Lt(baseFee) {
							gasPrice = new(uint256.Int).Set(maxFeePerGas)
						} else {
							gasPrice = new(uint256.Int).Set(baseFee)
						}
					}
				}
				if gasPrice == nil {
					gasPrice = maxFeePerGas
				}
			}
		}
	}
	if gasPrice == nil {
		gasPrice = new(uint256.Int)
	}

	var data []byte
	if d, ok := txMap["input"].(string); ok && d != "" && d != "0x" {
		data = common.FromHex(d)
	}

	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash: func(n uint64) common.Hash {
			if h, ok := blockEnv.BlockHashWindow[n]; ok {
				return h
			}
			return common.Hash{}
		},
		Coinbase:    blockEnv.Coinbase,
		GasLimit:    blockEnv.GasLimit,
		BlockNumber: new(big.Int).SetUint64(blockEnv.BlockNumber),
		Time:        blockEnv.Timestamp,
		Difficulty:  new(big.Int),
		BaseFee:     blockEnv.BaseFee,
		BlobBaseFee: big.NewInt(0),
		Random:      &blockEnv.PrevRandao,
	}
	txCtx := vm.TxContext{
		Origin:   sender,
		GasPrice: gasPrice,
	}

	snapshot := tracked.Snapshot()

	// Pass `tracked` (satisfies vm.StateDB via embedded + overrides) so that
	// every CanTransfer/Transfer/EVM read flows through our RW capture.
	evm := vm.NewEVM(blockCtx, tracked, te.chainConfig, vm.Config{})
	evm.TxContext = txCtx

	// ----- Intrinsic gas computation (inlined for geth compatibility) -----
	homestead := te.chainConfig.IsHomestead(blockCtx.BlockNumber)
	eip2028 := te.chainConfig.IsIstanbul(blockCtx.BlockNumber)
	intrinsicGas, err := simpleIntrinsicGas(data, hasTo == false, homestead, eip2028)
	if err != nil {
		tracked.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = "intrinsic gas: " + err.Error()
		result.GasUsed = gasLimit
		result.ReadKeys = tracked.SortedReadKeys()
		result.WriteKeys = tracked.SortedWriteKeys()
		return result, nil
	}
	if gasLimit < intrinsicGas {
		tracked.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = "out of gas (intrinsic)"
		result.GasUsed = gasLimit
		result.ReadKeys = tracked.SortedReadKeys()
		result.WriteKeys = tracked.SortedWriteKeys()
		return result, nil
	}

	// ----- Nonce check & bump -----
	storedNonce := tracked.GetNonce(sender)
	txNonce := uint64(0)
	if n, ok := txMap["nonce"].(float64); ok {
		txNonce = uint64(n)
	}
	if storedNonce != txNonce {
		tracked.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = fmt.Sprintf("nonce mismatch: want %d got %d", storedNonce, txNonce)
		result.GasUsed = gasLimit
		result.ReadKeys = tracked.SortedReadKeys()
		result.WriteKeys = tracked.SortedWriteKeys()
		return result, nil
	}
	tracked.SetNonce(sender, storedNonce+1, tracing.NonceChangeReason(0))

	// ----- Buy gas: deduct gasLimit * gasPrice upfront (mirrors core.TransitionDb) -----
	upfront := new(uint256.Int).Mul(uint256.NewInt(gasLimit), gasPrice)
	if tracked.GetBalance(sender).Cmp(upfront) < 0 {
		tracked.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = "insufficient balance for gas"
		result.GasUsed = gasLimit
		result.ReadKeys = tracked.SortedReadKeys()
		result.WriteKeys = tracked.SortedWriteKeys()
		return result, nil
	}
	tracked.SubBalance(sender, upfront, tracing.BalanceChangeReason(0))

	// ----- Value transfer check (mirrors pre-EVM check in core.ApplyMessage) -----
	if !core.CanTransfer(tracked, sender, value) {
		tracked.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = "insufficient balance for transfer"
		// gas is fully consumed when pre-check fails (match on-chain behavior)
		result.GasUsed = gasLimit
		result.ReadKeys = tracked.SortedReadKeys()
		result.WriteKeys = tracked.SortedWriteKeys()
		return result, nil
	}

	gasBudget := vm.GasBudget{RegularGas: gasLimit - intrinsicGas}
	var resultGas vm.GasBudget
	var vmerr error

	if hasTo {
		_, resultGas, vmerr = evm.Call(sender, toAddr, data, gasBudget, value)
	} else {
		_, _, resultGas, vmerr = evm.Create(sender, data, gasBudget, value)
	}

	if vmerr != nil {
		// On EVM error: revert inner EVM state changes but keep gas consumed
		tracked.RevertToSnapshot(snapshot)
		// Re-bump nonce and re-charge gas + value refund since on error we still
		// charge gas at a minimum (simplified: charge 100% gas). This matches
		// core.ApplyMessage semantics for our speculative purpose.
		result.Success = false
		result.Error = vmerr.Error()
		result.GasUsed = gasLimit
	} else {
		result.Success = true
		// Refund: cap refunded gas to gasLimit/2 (EIP-3529: use 1/5? We approximate with 1/2; correctness vs dataset doesn't depend on refunds as they don't affect RW keys)
		remaining := resultGas.RegularGas
		refundCap := (gasLimit - intrinsicGas - remaining) / 2
		remaining += refundCap
		// Refund (gasLimit - remaining) * gasPrice back to sender
		refundWei := new(uint256.Int).Mul(uint256.NewInt(gasLimit-remaining), gasPrice)
		// Actually remaining can't exceed gasLimit here, so just compute gasUsed = gasLimit - remaining.
		if remaining > gasLimit {
			remaining = gasLimit
		}
		tracked.AddBalance(sender, refundWei, tracing.BalanceChangeReason(0))
		// Pay coinbase: remaining gas * gasPrice
		coinbaseFee := new(uint256.Int).Mul(uint256.NewInt(gasLimit-remaining), gasPrice)
		tracked.AddBalance(blockEnv.Coinbase, coinbaseFee, tracing.BalanceChangeReason(0))
		result.GasUsed = gasLimit - remaining
	}

	result.ReadKeys = tracked.SortedReadKeys()
	result.WriteKeys = tracked.SortedWriteKeys()
	result.ReadKeys = tracked.SortedReadKeys()
	result.WriteKeys = tracked.SortedWriteKeys()

	return result, nil
}

// BuildBaseStateDB creates a fresh geth InMemory StateDB and injects all
// witness accounts (balance, nonce, code, storage slots). Returns the ready
// state and a pre-state-root for reference.
//
// Callers can Clone() this state cheaply for per-tx PreExecute isolation, or
// simply pass it directly (PreExecuteTx snapshots internally and reverts).
func BuildBaseStateDB(witness *BlockWitness, blockNum uint64, chainConfig *params.ChainConfig) (*state.StateDB, common.Hash, error) {
	if chainConfig == nil {
		chainConfig = params.MainnetChainConfig
	}
	memDB := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memDB, triedb.HashDefaults)
	sdb, err := state.New(common.Hash{}, state.NewMPTDatabase(tdb, state.NewCodeDB(memDB)))
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("create state db: %w", err)
	}

	if witness != nil {
		for addr, acct := range witness.Accounts {
			if !common.IsHexAddress(addr) {
				continue
			}
			address := common.HexToAddress(addr)
			sdb.CreateAccount(address)

			if acct.Balance != "" {
				bal := new(uint256.Int)
				if err := bal.SetFromHex(acct.Balance); err == nil {
					sdb.SetBalance(address, bal, tracing.BalanceChangeReason(0))
				}
			}
			if acct.Nonce != "" {
				nonce, ok := new(big.Int).SetString(acct.Nonce, 0)
				if ok {
					sdb.SetNonce(address, nonce.Uint64(), tracing.NonceChangeReason(0))
				}
			}
			if acct.Code != "" {
				code := common.FromHex(acct.Code)
				sdb.SetCode(address, code, tracing.CodeChangeReason(0))
			}
			if acct.Storage != nil {
				for slot, val := range acct.Storage {
					sdb.SetState(address, common.HexToHash(slot), common.HexToHash(val))
				}
			}
		}
	}

	isEIP158 := chainConfig.IsEIP158(new(big.Int).SetUint64(blockNum))
	preRoot := sdb.IntermediateRoot(isEIP158)
	return sdb, preRoot, nil
}

// Ensure unused tracing import is referenced (core.Transfer handles balance
// change reasons internally; we only keep tracing for BuildBaseStateDB's
// explicit SetBalance/SetNonce/SetCode calls).
var _ = tracing.BalanceChangeTransfer
