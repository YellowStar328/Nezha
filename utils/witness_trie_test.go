package utils

import (
	"math/big"
	"testing"

	"Nezha/ethereum/go-ethereum/common"
	"Nezha/ethereum/go-ethereum/core/rawdb"
	"Nezha/ethereum/go-ethereum/core/state"
	"Nezha/ethereum/go-ethereum/crypto"
)

// TestBuildWitnessTrieRoundTrip encodes a small synthetic witness as a REAL
// MPT, reopens it through a fresh StateDB and asserts every field matches.
func TestBuildWitnessTrieRoundTrip(t *testing.T) {
	eoa := common.HexToAddress("0x1111111111111111111111111111111111111111")
	contract := common.HexToAddress("0x2222222222222222222222222222222222222222")

	code := common.FromHex("0x6001600101600055") // PUSH1 1 PUSH1 1 ADD PUSH1 0 SSTORE
	witness := map[string]*ReplayWitnessAccount{
		eoa.Hex(): {
			Balance:  "0x64",
			Nonce:    "0x2",
			CodeHash: crypto.Keccak256Hash(nil).Hex(),
			Code:     "",
			Storage:  nil,
		},
		contract.Hex(): {
			Balance:  "0x1",
			Nonce:    "0x1",
			CodeHash: crypto.Keccak256Hash(code).Hex(),
			Code:     "0x6001600101600055",
			Storage: map[string]string{
				"0x0": "0x2",
				"0x5": "0x1234",
			},
		},
	}

	// Build on a memory db, then reopen with a FRESH (empty-cache) StateDB.
	edb := rawdb.NewMemoryDatabase()
	root, err := BuildWitnessTrie(witness, edb)
	if err != nil {
		t.Fatalf("build witness trie: %v", err)
	}
	if root == (common.Hash{}) {
		t.Fatal("root is zero hash")
	}
	t.Logf("state root: %s", root.Hex())

	sdb, err := state.New(root, state.NewDatabase(edb))
	if err != nil {
		t.Fatalf("open statedb: %v", err)
	}

	// EOA: balance + nonce, no code, no storage.
	if got := sdb.GetBalance(eoa); got.Cmp(big.NewInt(0x64)) != 0 {
		t.Errorf("eoa balance = %v, want 100", got)
	}
	if got := sdb.GetNonce(eoa); got != 2 {
		t.Errorf("eoa nonce = %d, want 2", got)
	}
	if got := sdb.GetCode(eoa); len(got) != 0 {
		t.Errorf("eoa code = %x, want empty", got)
	}
	if got := sdb.GetState(eoa, common.HexToHash("0x0")); got != (common.Hash{}) {
		t.Errorf("eoa storage[0] = %v, want zero", got)
	}

	// Contract: balance, nonce, code and storage slots.
	if got := sdb.GetBalance(contract); got.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("contract balance = %v, want 1", got)
	}
	if got := sdb.GetNonce(contract); got != 1 {
		t.Errorf("contract nonce = %d, want 1", got)
	}
	if got := sdb.GetCode(contract); string(got) != string(code) {
		t.Errorf("contract code = %x, want %x", got, code)
	}
	if got := sdb.GetState(contract, common.HexToHash("0x0")); got != common.HexToHash("0x2") {
		t.Errorf("contract storage[0] = %v, want 2", got)
	}
	if got := sdb.GetState(contract, common.HexToHash("0x5")); got != common.HexToHash("0x1234") {
		t.Errorf("contract storage[5] = %v, want 0x1234", got)
	}
	if got := sdb.GetState(contract, common.HexToHash("0xffff")); got != (common.Hash{}) {
		t.Errorf("contract storage[0xffff] = %v, want zero (not in witness)", got)
	}

	// Non-existent account.
	ghost := common.HexToAddress("0x9999999999999999999999999999999999999999")
	if sdb.Exist(ghost) {
		t.Errorf("ghost account should not exist")
	}
}

// TestBuildWitnessTrieDiskColdRead builds the trie into a REAL leveldb, then
// opens a second fresh StateDB over the same store. The second open must
// serve reads from the on-disk nodes (i.e. the builder's in-memory dirty set
// was flushed), proving the cold-read path works end-to-end.
func TestBuildWitnessTrieDiskColdRead(t *testing.T) {
	contract := common.HexToAddress("0x2222222222222222222222222222222222222222")
	witness := map[string]*ReplayWitnessAccount{
		contract.Hex(): {
			Balance: "0x1234567890abcdef",
			Nonce:   "0x7",
			Code:    "0x6001600101600055",
			Storage: map[string]string{"0x0": "0x2"},
		},
	}

	tmp := t.TempDir()
	edb, err := rawdb.NewLevelDBDatabase(tmp, 0, 1, "")
	if err != nil {
		t.Fatalf("open leveldb: %v", err)
	}
	defer edb.Close()

	root, err := BuildWitnessTrie(witness, edb)
	if err != nil {
		t.Fatalf("build witness trie: %v", err)
	}

	// Second, independent StateDB — empty trie caches, reads come from disk.
	sdb, err := state.New(root, state.NewDatabase(edb))
	if err != nil {
		t.Fatalf("reopen statedb: %v", err)
	}
	want, _ := new(big.Int).SetString("0x1234567890abcdef", 0)
	if got := sdb.GetBalance(contract); got.Cmp(want) != 0 {
		t.Errorf("balance = %v, want 0x1234567890abcdef", got)
	}
	if got := sdb.GetNonce(contract); got != 7 {
		t.Errorf("nonce = %d, want 7", got)
	}
	if got := sdb.GetState(contract, common.HexToHash("0x0")); got != common.HexToHash("0x2") {
		t.Errorf("storage[0] = %v, want 2", got)
	}
}
