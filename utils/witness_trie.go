package utils

import (
	"fmt"
	"math/big"

	"Nezha/ethereum/go-ethereum/common"
	"Nezha/ethereum/go-ethereum/core/state"
	"Nezha/ethereum/go-ethereum/crypto"
	"Nezha/ethereum/go-ethereum/ethdb"
	"Nezha/ethereum/go-ethereum/rlp"
	"Nezha/ethereum/go-ethereum/trie"
)

// emptyRoot is the canonical empty storage-trie root shared by every account
// with no storage. Mirrors state.emptyRoot (private in the go-ethereum fork).
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// emptyCodeHash is keccak256(nil); accounts without code carry this as their
// code hash and the code-table lookup short-circuits in stateObject.GetCode.
var emptyCodeHash = crypto.Keccak256(nil)

// BuildWitnessTrie encodes a flat block witness into REAL Merkle Patricia
// Tries (one account trie + one storage trie per contract) on top of edb, the
// same way a full node stores state:
//
//   - account trie key   = address (further hashed by the secure trie)
//   - account trie value = RLP(Account{Nonce, Balance, Root, CodeHash})
//   - storage trie key   = slot   (further hashed by the secure trie)
//   - storage trie value = RLP(slot value) — same format stateObject writes
//   - code blobs stored in the code table under keccak(code)
//
// All trie nodes are flushed to edb via trie.Database.Commit. A StateDB opened
// later from the returned root with a FRESH (empty) trie cache therefore reads
// nodes on demand from edb — the vegeta-upstream access form: real trie
// traversal, real node loads from the backing store on first touch, no
// simulated latency.
//
// edb must be provided by the caller (memorydb for RAM-backed access, leveldb
// for genuine disk cold reads) and must stay open for the lifetime of the
// StateDB opened from the returned root.
func BuildWitnessTrie(accounts map[string]*ReplayWitnessAccount, edb ethdb.Database) (common.Hash, error) {
	tdb := trie.NewDatabase(edb)
	accTrie, err := trie.NewSecure(common.Hash{}, tdb)
	if err != nil {
		return common.Hash{}, fmt.Errorf("new account trie: %w", err)
	}

	// storageTries collects every committed storage-trie root so their nodes
	// can be flushed to the backing store separately: a storage trie is NOT
	// reachable from the account trie's children, so committing only the
	// account root would leave all storage nodes in the builder's memory.
	var storageTries []common.Hash

	for addrHex, acct := range accounts {
		if !common.IsHexAddress(addrHex) {
			continue
		}
		addr := common.HexToAddress(addrHex)

		// 1) storage trie for this contract (only when it has slots)
		storageRoot := emptyRoot
		if len(acct.Storage) > 0 {
			st, err := trie.NewSecure(common.Hash{}, tdb)
			if err != nil {
				return common.Hash{}, fmt.Errorf("new storage trie for %s: %w", addrHex, err)
			}
			for slotHex, valHex := range acct.Storage {
				slot := common.HexToHash(slotHex)
				// Same value format stateObject.GetCommittedState expects:
				// RLP(content), content = slot value with leading zeroes trimmed.
				v := common.TrimLeftZeroes(common.HexToHash(valHex).Bytes())
				if len(v) == 0 {
					continue // zero values are not persisted (geth semantics)
				}
				enc, err := rlp.EncodeToBytes(v)
				if err != nil {
					return common.Hash{}, fmt.Errorf("rlp storage value %s: %w", valHex, err)
				}
				if err := st.TryUpdate(slot.Bytes(), enc); err != nil {
					return common.Hash{}, fmt.Errorf("storage update %s/%s: %w", addrHex, slotHex, err)
				}
			}
			storageRoot, err = st.Commit(nil)
			if err != nil {
				return common.Hash{}, fmt.Errorf("commit storage trie %s: %w", addrHex, err)
			}
			storageTries = append(storageTries, storageRoot)
		}

		// 2) code blob under keccak(code). stateObject.GetCode reads it via
		// trie.Database.Node(codeHash) → edb.Get(codeHash[:]).
		codeHash := common.BytesToHash(emptyCodeHash)
		if code := common.FromHex(acct.Code); len(code) > 0 {
			codeHash = crypto.Keccak256Hash(code)
			if err := edb.Put(codeHash.Bytes(), code); err != nil {
				return common.Hash{}, fmt.Errorf("put code %s: %w", addrHex, err)
			}
		}

		// 3) account record in the account trie. Same RLP shape loadObj decodes.
		balance := new(big.Int)
		if acct.Balance != "" {
			if _, ok := balance.SetString(acct.Balance, 0); !ok {
				balance = new(big.Int)
			}
		}
		nonce := uint64(0)
		if acct.Nonce != "" {
			if n, ok := new(big.Int).SetString(acct.Nonce, 0); ok {
				nonce = n.Uint64()
			}
		}
		account := state.Account{Nonce: nonce, Balance: balance, Root: storageRoot, CodeHash: codeHash.Bytes()}
		enc, err := rlp.EncodeToBytes(&account)
		if err != nil {
			return common.Hash{}, fmt.Errorf("rlp account %s: %w", addrHex, err)
		}
		if err := accTrie.TryUpdate(addr.Bytes(), enc); err != nil {
			return common.Hash{}, fmt.Errorf("account update %s: %w", addrHex, err)
		}
	}

	root, err := accTrie.Commit(nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("commit account trie: %w", err)
	}
	// Flush every node to the backing store so a fresh StateDB reads them on
	// demand instead of from this builder's in-memory dirty set. Storage tries
	// are independent of the account trie, so each must be committed on its
	// own (same pattern as statedb.Commit: commitTrie per object first).
	for _, sr := range storageTries {
		if err := tdb.Commit(sr, false); err != nil {
			return common.Hash{}, fmt.Errorf("flush storage trie nodes: %w", err)
		}
	}
	if err := tdb.Commit(root, false); err != nil {
		return common.Hash{}, fmt.Errorf("flush account trie nodes: %w", err)
	}
	return root, nil
}
