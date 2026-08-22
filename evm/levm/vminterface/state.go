package vminterface

import (
	"Nezha/ethereum/go-ethereum/common"
	"Nezha/ethereum/go-ethereum/core/rawdb"
	"Nezha/ethereum/go-ethereum/core/state"
	"Nezha/ethereum/go-ethereum/ethdb"
	com "Nezha/evm/levm/common"
)

// NewStateDB - Create a new StateDB using levelDB instead of RAM
func NewStateDB(root common.Hash, dbPath string) (*state.StateDB, ethdb.Database) {

	// open ethdb
	/*edb, err := ethdb.NewLDBDatabase(dbPath, 100, 100)
	db := state.NewDatabase(edb)
	com.PanicErr(err)
	*/

	edb, _ := rawdb.NewLevelDBDatabase(dbPath, 100, 100, "")
	//edb := rawdb.NewMemoryDatabase()
	db := state.NewDatabase(edb)

	// make statedb
	stateDB, err := state.New(root, db)
	com.PanicErr(err)

	return stateDB, edb
}

// NewMemoryStateDB creates a StateDB backed by an in-memory ethdb.
// Use this for short-lived EVM instances that only need snapshot/revert
// semantics (no persistence) — e.g. mainnet replay's LevmSpecFallbackPool,
// where N workers each run PreExecute/ReExecute and never commit to disk.
//
// Memory backing eliminates leveldb I/O contention across concurrent workers,
// which was the bottleneck when 10+ workers each hit their own leveldb for
// every SetState/GetState call.
func NewMemoryStateDB() (*state.StateDB, ethdb.Database) {
	edb := rawdb.NewMemoryDatabase()
	db := state.NewDatabase(edb)
	stateDB, err := state.New(common.Hash{}, db)
	com.PanicErr(err)
	return stateDB, edb
}

// NewStateDBWithTrie creates a StateDB over an EXISTING backing store whose
// account trie is rooted at `root` (typically built by utils.BuildWitnessTrie).
//
// The trie cache is brand-new (empty): the first access to every node goes to
// the backing store (real disk I/O for leveldb, memory lookup for memorydb).
// This is the vegeta-upstream access form — state reads traverse a real MPT
// and load nodes on demand — with no simulated latency.
func NewStateDBWithTrie(root common.Hash, edb ethdb.Database) (*state.StateDB, ethdb.Database) {
	db := state.NewDatabase(edb)
	stateDB, err := state.New(root, db)
	com.PanicErr(err)
	return stateDB, edb
}

// NewSharedTrieDatabase creates a state.Database that is safe for concurrent
// use (see state.NewDatabaseWithCache) and keeps recently loaded trie nodes in
// memory. cacheMB is the shared node-cache size in MB; 0 disables the node
// cache, making every node load go to the backing store (the old cold-cache
// semantics). Sharing ONE instance across all pool workers mirrors a full
// node's shared trie cache: a node loaded by any worker is warm for every other
// worker, so a given slot is cold-read at most once per block.
func NewSharedTrieDatabase(edb ethdb.Database, cacheMB int) state.Database {
	return state.NewDatabaseWithCache(edb, cacheMB)
}

// NewStateDBWithSharedTrie creates a StateDB over a caller-supplied (possibly
// shared) state.Database. The trie node cache is the shared database's, so a
// node loaded by one StateDB is warm for all other StateDBs that share the same
// database instance. The caller owns the backing store.
func NewStateDBWithSharedTrie(root common.Hash, db state.Database) *state.StateDB {
	stateDB, err := state.New(root, db)
	com.PanicErr(err)
	return stateDB
}

// NewStateDBsWithSharedTrie creates n StateDBs that share ONE exact trie
// instance (opened once from root) in addition to the shared state.Database.
// Because the trie instance itself is shared, writes flushed by one StateDB
// via IntermediateRoot / CommitDirtyStorage are visible to all others without
// any root propagation — the vegeta upstream "copyStateDB []*StateDB all view
// the same Trie" layout. Callers must use the resulting StateDBs concurrently
// only through lock-guarded trie access (trie.Trie is lock-protected).
func NewStateDBsWithSharedTrie(root common.Hash, db state.Database, n int) ([]*state.StateDB, error) {
	tr, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	sdbs := make([]*state.StateDB, n)
	for i := 0; i < n; i++ {
		sdbs[i], err = state.NewWithTrie(db, tr)
		if err != nil {
			return nil, err
		}
	}
	return sdbs, nil
}
