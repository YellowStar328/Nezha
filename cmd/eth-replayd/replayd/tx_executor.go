package replayd

import (
	"encoding/json"
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

type TxExecResult struct {
	TxHash     string
	TxIndex    int
	Success    bool
	GasUsed    uint64
	Output     []byte
	Error      string
	MissedKeys []string
	StateRoot  common.Hash
}

type BlockExecResult struct {
	BlockNumber   uint64
	BlockHash     common.Hash
	StateRoot     common.Hash
	PreStateRoot  common.Hash
	PostStateRoot common.Hash
	TxResults     []TxExecResult
	AllSuccess    bool
	MissesFound   int
}

type TxExecutor struct {
	chainConfig *params.ChainConfig
}

func NewTxExecutor() *TxExecutor {
	return &TxExecutor{
		chainConfig: params.MainnetChainConfig,
	}
}

// ChainConfig exposes the executor's chain config for state construction.
func (te *TxExecutor) ChainConfig() *params.ChainConfig {
	return te.chainConfig
}

func (te *TxExecutor) ExecuteBlock(blockData *BlockDataset, blockHashWindow map[uint64]common.Hash) (*BlockExecResult, error) {
	memDB := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memDB, triedb.HashDefaults)
	sdb, err := state.New(common.Hash{}, state.NewMPTDatabase(tdb, state.NewCodeDB(memDB)))
	if err != nil {
		return nil, fmt.Errorf("create state db: %w", err)
	}

	blockNum := 0
	if v, ok := blockData.Header["number"].(float64); ok {
		blockNum = int(v)
	}

	result := &BlockExecResult{
		BlockNumber: uint64(blockNum),
		TxResults:   make([]TxExecResult, 0),
		AllSuccess:  true,
	}

	if blockData.Witness != nil {
		for addr, acct := range blockData.Witness.Accounts {
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

	isEIP158 := te.chainConfig.IsEIP158(new(big.Int).SetUint64(uint64(blockNum)))
	preStateRoot := sdb.IntermediateRoot(isEIP158)
	result.PreStateRoot = preStateRoot

	blockEnv := BuildBlockEnv(blockData.Header, blockHashWindow)

	for txIdx, txRaw := range blockData.Transactions {
		txResult, err := te.executeTx(sdb, blockEnv, txRaw, txIdx)
		if err != nil {
			txResult.Error = err.Error()
			txResult.Success = false
			result.AllSuccess = false
			result.MissesFound++
		}
		result.TxResults = append(result.TxResults, *txResult)
	}

	postStateRoot := sdb.IntermediateRoot(isEIP158)
	result.PostStateRoot = postStateRoot
	result.StateRoot = postStateRoot

	return result, nil
}

func (te *TxExecutor) executeTx(sdb *state.StateDB, blockEnv *BlockEnv, txRaw interface{}, txIdx int) (*TxExecResult, error) {
	txMap, ok := txRaw.(map[string]interface{})
	if !ok {
		return &TxExecResult{
			TxIndex: txIdx,
			Success: false,
			Error:   "invalid tx format",
		}, fmt.Errorf("invalid tx format at index %d", txIdx)
	}

	result := &TxExecResult{
		TxIndex: txIdx,
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

	evm := vm.NewEVM(blockCtx, sdb, te.chainConfig, vm.Config{})
	evm.TxContext = txCtx

	snapshot := sdb.Snapshot()

	gasBudget := vm.GasBudget{RegularGas: gasLimit}

	var ret []byte
	var resultGas vm.GasBudget
	var contractAddr common.Address
	var vmerr error

	if hasTo {
		ret, resultGas, vmerr = evm.Call(sender, toAddr, data, gasBudget, value)
	} else {
		ret, contractAddr, resultGas, vmerr = evm.Create(sender, data, gasBudget, value)
		_ = contractAddr
	}

	if vmerr != nil {
		sdb.RevertToSnapshot(snapshot)
		result.Success = false
		result.Error = vmerr.Error()
	} else {
		result.Success = true
	}

	result.Output = ret
	result.GasUsed = gasLimit - resultGas.RegularGas

	return result, nil
}

func ParseTransactions(rawData json.RawMessage) ([]interface{}, error) {
	var txs []interface{}
	if err := json.Unmarshal(rawData, &txs); err != nil {
		return nil, err
	}
	return txs, nil
}
