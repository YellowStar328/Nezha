package levm

import (
	"math/big"
	"sync"
	"time"

	"Nezha/ethereum/go-ethereum/common"
	"Nezha/ethereum/go-ethereum/core/state"
)

// AccessLatencySimulator mimics the cold-read behavior of a real trie-backed
// state database (e.g. geth's leveldb + trie cache): the first access to an
// (account, slot) pair pays a fixed latency (disk read), and every later
// access hits the "cache" for free.
//
// The simulator is deliberately SHARED across all worker EVMs of one run mode
// so that a key read by any worker is warm for every other worker — exactly
// like a shared leveldb block cache in a real node. The serial baseline and
// the parallel pool get SEPARATE simulators so both sides start from a cold
// cache and pay the same total cold-read cost; only the ability to overlap
// those latencies differs (serial sums them, parallel overlaps them).
type AccessLatencySimulator struct {
	mu      sync.Mutex
	loaded  map[string]struct{}
	latency time.Duration
}

// NewAccessLatencySimulator returns a simulator that sleeps `latency` on the
// first access of each (addr, slot). Returns nil when latency <= 0 (disabled).
func NewAccessLatencySimulator(latency time.Duration) *AccessLatencySimulator {
	if latency <= 0 {
		return nil
	}
	return &AccessLatencySimulator{
		loaded:  make(map[string]struct{}),
		latency: latency,
	}
}

// Access simulates one state read of (addr, slot).
//
// The mutex is only held for the map check/mark; the sleep happens OUTSIDE
// the lock so parallel workers do not serialize their cold-read latencies.
func (s *AccessLatencySimulator) Access(addr common.Address, slot common.Hash) {
	if s == nil {
		return
	}
	key := addr.Hex() + "/" + slot.Hex()
	s.mu.Lock()
	if _, ok := s.loaded[key]; ok {
		s.mu.Unlock()
		return
	}
	s.loaded[key] = struct{}{}
	s.mu.Unlock()
	time.Sleep(s.latency)
}

// latencyStateDB wraps a *state.StateDB and charges the simulated cold-read
// latency on every state read performed through the EVM interpreter
// (SLOAD/BALANCE/EXTCODESIZE/...). Writes and the non-EVM direct calls
// (witness injection, tx-level nonce/coinbase bookkeeping) keep using the
// underlying StateDB untouched.
//
// It implements vm.StateDB by embedding *state.StateDB (all remaining methods
// are forwarded automatically) and overriding only the read accessors.
type latencyStateDB struct {
	*state.StateDB
	sim *AccessLatencySimulator
}

func (d *latencyStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	d.sim.Access(addr, key)
	return d.StateDB.GetState(addr, key)
}

func (d *latencyStateDB) GetCommittedState(addr common.Address, key common.Hash) common.Hash {
	d.sim.Access(addr, key)
	return d.StateDB.GetCommittedState(addr, key)
}

func (d *latencyStateDB) GetBalance(addr common.Address) *big.Int {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.GetBalance(addr)
}

func (d *latencyStateDB) GetNonce(addr common.Address) uint64 {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.GetNonce(addr)
}

func (d *latencyStateDB) GetCode(addr common.Address) []byte {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.GetCode(addr)
}

func (d *latencyStateDB) GetCodeSize(addr common.Address) int {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.GetCodeSize(addr)
}

func (d *latencyStateDB) GetCodeHash(addr common.Address) common.Hash {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.GetCodeHash(addr)
}

func (d *latencyStateDB) Exist(addr common.Address) bool {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.Exist(addr)
}

func (d *latencyStateDB) Empty(addr common.Address) bool {
	d.sim.Access(addr, common.Hash{})
	return d.StateDB.Empty(addr)
}
