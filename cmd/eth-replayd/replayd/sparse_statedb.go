package replayd

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
)

type WitnessMiss struct {
	Keys []string
}

func (w *WitnessMiss) Error() string {
	return "witness miss"
}

func IsWitnessMiss(err error) bool {
	_, ok := err.(*WitnessMiss)
	return ok
}

type SparseStateDB struct {
	stateDB    *state.StateDB
	witness    map[string]*WitnessAccount
	codeStore  map[string][]byte
	missedKeys map[string]bool
}

func NewSparseStateDB() *SparseStateDB {
	memDB := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memDB, triedb.HashDefaults)
	stateDB, err := state.New(common.Hash{}, state.NewMPTDatabase(tdb, state.NewCodeDB(memDB)))
	if err != nil {
		panic(err)
	}
	return &SparseStateDB{
		stateDB:    stateDB,
		witness:    make(map[string]*WitnessAccount),
		codeStore:  make(map[string][]byte),
		missedKeys: make(map[string]bool),
	}
}

func (s *SparseStateDB) LoadWitness(witness *BlockWitness) {
	s.witness = make(map[string]*WitnessAccount)
	if witness != nil {
		for addr, acct := range witness.Accounts {
			s.witness[addr] = acct
		}
	}
	s.missedKeys = make(map[string]bool)
}

func (s *SparseStateDB) GetBalance(addr common.Address) *big.Int {
	acct, ok := s.witness[addr.Hex()]
	if !ok {
		s.missedKeys[AcctKey(addr.Hex(), "balance")] = true
		return nil
	}
	bal, _ := new(big.Int).SetString(acct.Balance, 0)
	return bal
}

func (s *SparseStateDB) GetNonce(addr common.Address) uint64 {
	acct, ok := s.witness[addr.Hex()]
	if !ok {
		s.missedKeys[AcctKey(addr.Hex(), "nonce")] = true
		return 0
	}
	nonce, _ := new(big.Int).SetString(acct.Nonce, 0)
	return nonce.Uint64()
}

func (s *SparseStateDB) GetCode(addr common.Address) []byte {
	acct, ok := s.witness[addr.Hex()]
	if !ok {
		s.missedKeys[AcctKey(addr.Hex(), "code")] = true
		return nil
	}
	if code, exists := s.codeStore[addr.Hex()]; exists {
		return code
	}
	if acct.Code != "" {
		code := common.FromHex(acct.Code)
		s.codeStore[addr.Hex()] = code
		return code
	}
	return nil
}

func (s *SparseStateDB) GetState(addr common.Address, slot common.Hash) common.Hash {
	acct, ok := s.witness[addr.Hex()]
	if !ok || acct.Storage == nil {
		s.missedKeys[SlotKey(addr.Hex(), slot.Hex())] = true
		return common.Hash{}
	}
	val, exists := acct.Storage[slot.Hex()]
	if !exists {
		s.missedKeys[SlotKey(addr.Hex(), slot.Hex())] = true
		return common.Hash{}
	}
	return common.HexToHash(val)
}

func (s *SparseStateDB) Exist(addr common.Address) bool {
	_, ok := s.witness[addr.Hex()]
	if !ok {
		s.missedKeys[AcctKey(addr.Hex(), "exist")] = true
	}
	return ok
}

func (s *SparseStateDB) GetMissedKeys() []string {
	keys := make([]string, 0, len(s.missedKeys))
	for k := range s.missedKeys {
		keys = append(keys, k)
	}
	return keys
}

func (s *SparseStateDB) ResetMissed() {
	s.missedKeys = make(map[string]bool)
}

type BlockEnv struct {
	BlockNumber     uint64
	BlockHash       common.Hash
	ParentHash      common.Hash
	Timestamp       uint64
	GasLimit        uint64
	BaseFee         *big.Int
	PrevRandao      common.Hash
	Coinbase        common.Address
	BlobBaseFee     *big.Int
	ExcessBlobGas   uint64
	BlockHashWindow map[uint64]common.Hash
	ChainConfig     *params.ChainConfig
}

func BuildBlockEnv(header map[string]interface{}, blockHashWindow map[uint64]common.Hash) *BlockEnv {
	env := &BlockEnv{
		ChainConfig:     params.MainnetChainConfig,
		BlockHashWindow: blockHashWindow,
	}

	if v, ok := header["number"].(float64); ok {
		env.BlockNumber = uint64(v)
	}
	if v, ok := header["hash"].(string); ok {
		env.BlockHash = common.HexToHash(v)
	}
	if v, ok := header["parentHash"].(string); ok {
		env.ParentHash = common.HexToHash(v)
	}
	if v, ok := header["timestamp"].(float64); ok {
		env.Timestamp = uint64(v)
	}
	if v, ok := header["gasLimit"].(float64); ok {
		env.GasLimit = uint64(v)
	}
	if v, ok := header["baseFeePerGas"].(string); ok {
		env.BaseFee = new(big.Int)
		env.BaseFee.SetString(v, 0)
	}
	if v, ok := header["prevRandao"].(string); ok {
		env.PrevRandao = common.HexToHash(v)
	}
	if v, ok := header["beneficiary"].(string); ok {
		env.Coinbase = common.HexToAddress(v)
	}

	return env
}

func AcctKey(addr string, field string) string {
	addr = strings.ToLower(addr)
	if !strings.HasPrefix(addr, "0x") {
		addr = "0x" + addr
	}
	return "acct:" + addr + ":" + field
}

func SlotKey(addr string, slot string) string {
	addr = strings.ToLower(addr)
	slot = strings.ToLower(slot)
	if !strings.HasPrefix(addr, "0x") {
		addr = "0x" + addr
	}
	if !strings.HasPrefix(slot, "0x") {
		slot = "0x" + slot
	}
	return "slot:" + addr + ":" + slot
}
