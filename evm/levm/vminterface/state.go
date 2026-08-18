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
