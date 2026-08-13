package replayd

import (
	"math/big"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/holiman/uint256"
)

// rwTracker wraps *state.StateDB with read/write key capture.
// All methods forward to the inner DB; reads and writes are logged into
// sets using the canonical key format:
//
//	acct:<0xaddr_lower>:balance
//	acct:<0xaddr_lower>:nonce
//	acct:<0xaddr_lower>:code
//	slot:<0xaddr_lower>:<0xslot_lower>
//
// Thread-safety: NOT safe for concurrent use on a single tracker instance.
// (Each PreExecute call gets its own snapshot/clone.)
type rwTracker struct {
	*state.StateDB
	mu     sync.RWMutex
	reads  map[string]bool
	writes map[string]bool
}

func newRWTracker(inner *state.StateDB) *rwTracker {
	return &rwTracker{
		StateDB: inner,
		reads:   make(map[string]bool),
		writes:  make(map[string]bool),
	}
}

func (t *rwTracker) SortedReadKeys() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return sortedStringKeys(t.reads)
}

func (t *rwTracker) SortedWriteKeys() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return sortedStringKeys(t.writes)
}

func sortedStringKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (t *rwTracker) addRead(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reads[key] = true
}

func (t *rwTracker) addWrite(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes[key] = true
}

// --- overrides for account-level access ---

func (t *rwTracker) GetBalance(addr common.Address) *uint256.Int {
	t.addRead(AcctKey(addr.Hex(), "balance"))
	return t.StateDB.GetBalance(addr)
}

func (t *rwTracker) SetBalance(addr common.Address, value *uint256.Int, reason tracing.BalanceChangeReason) {
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	t.StateDB.SetBalance(addr, value, reason)
}

func (t *rwTracker) SubBalance(addr common.Address, value *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	t.addRead(AcctKey(addr.Hex(), "balance"))
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	return t.StateDB.SubBalance(addr, value, reason)
}

func (t *rwTracker) AddBalance(addr common.Address, value *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	t.addRead(AcctKey(addr.Hex(), "balance"))
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	return t.StateDB.AddBalance(addr, value, reason)
}

func (t *rwTracker) GetNonce(addr common.Address) uint64 {
	t.addRead(AcctKey(addr.Hex(), "nonce"))
	return t.StateDB.GetNonce(addr)
}

func (t *rwTracker) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	t.addRead(AcctKey(addr.Hex(), "nonce"))
	t.addWrite(AcctKey(addr.Hex(), "nonce"))
	t.StateDB.SetNonce(addr, nonce, reason)
}

func (t *rwTracker) GetCode(addr common.Address) []byte {
	t.addRead(AcctKey(addr.Hex(), "code"))
	return t.StateDB.GetCode(addr)
}

func (t *rwTracker) SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason) []byte {
	t.addWrite(AcctKey(addr.Hex(), "code"))
	return t.StateDB.SetCode(addr, code, reason)
}

func (t *rwTracker) GetCodeSize(addr common.Address) int {
	t.addRead(AcctKey(addr.Hex(), "code"))
	return t.StateDB.GetCodeSize(addr)
}

func (t *rwTracker) GetCodeHash(addr common.Address) common.Hash {
	t.addRead(AcctKey(addr.Hex(), "code"))
	return t.StateDB.GetCodeHash(addr)
}

// --- storage slots ---

func (t *rwTracker) GetState(addr common.Address, slot common.Hash) common.Hash {
	t.addRead(SlotKey(addr.Hex(), slot.Hex()))
	return t.StateDB.GetState(addr, slot)
}

func (t *rwTracker) SetState(addr common.Address, slot common.Hash, val common.Hash) common.Hash {
	t.addRead(SlotKey(addr.Hex(), slot.Hex()))
	t.addWrite(SlotKey(addr.Hex(), slot.Hex()))
	return t.StateDB.SetState(addr, slot, val)
}

func (t *rwTracker) GetCommittedState(addr common.Address, slot common.Hash) common.Hash {
	t.addRead(SlotKey(addr.Hex(), slot.Hex()))
	return t.StateDB.GetCommittedState(addr, slot)
}

// --- existence / account creation ---

func (t *rwTracker) Exist(addr common.Address) bool {
	t.addRead(AcctKey(addr.Hex(), "exist"))
	return t.StateDB.Exist(addr)
}

func (t *rwTracker) Empty(addr common.Address) bool {
	t.addRead(AcctKey(addr.Hex(), "exist"))
	t.addRead(AcctKey(addr.Hex(), "balance"))
	t.addRead(AcctKey(addr.Hex(), "nonce"))
	t.addRead(AcctKey(addr.Hex(), "code"))
	return t.StateDB.Empty(addr)
}

func (t *rwTracker) CreateAccount(addr common.Address) {
	t.addWrite(AcctKey(addr.Hex(), "exist"))
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	t.addWrite(AcctKey(addr.Hex(), "nonce"))
	t.addWrite(AcctKey(addr.Hex(), "code"))
	t.StateDB.CreateAccount(addr)
}

func (t *rwTracker) SelfDestruct(addr common.Address) {
	t.addWrite(AcctKey(addr.Hex(), "exist"))
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	t.addWrite(AcctKey(addr.Hex(), "code"))
	t.StateDB.SelfDestruct(addr)
}

func (t *rwTracker) Selfdestruct6780(addr common.Address) {
	t.addWrite(AcctKey(addr.Hex(), "exist"))
	t.addWrite(AcctKey(addr.Hex(), "balance"))
	t.addWrite(AcctKey(addr.Hex(), "code"))
	// Selfdestruct6780 may not exist on all geth versions; fall back to
	// SelfDestruct (same semantics for tracking).
	t.StateDB.SelfDestruct(addr)
}

func (t *rwTracker) HasSelfDestructed(addr common.Address) bool {
	t.addRead(AcctKey(addr.Hex(), "exist"))
	return t.StateDB.HasSelfDestructed(addr)
}

// --- AddrHash/account accessors for completeness ---

func (t *rwTracker) GetStorageRoot(addr common.Address) common.Hash {
	t.addRead(AcctKey(addr.Hex(), "code"))
	return t.StateDB.GetStorageRoot(addr)
}

func (t *rwTracker) CreateContract(addr common.Address) {
	t.addWrite(AcctKey(addr.Hex(), "exist"))
	t.addWrite(AcctKey(addr.Hex(), "code"))
	t.StateDB.CreateContract(addr)
}

// --- snapshot/revert: inherited but shadow the return type ---

func (t *rwTracker) Snapshot() int {
	return t.StateDB.Snapshot()
}

func (t *rwTracker) RevertToSnapshot(id int) {
	t.StateDB.RevertToSnapshot(id)
}

// Ensure the *rwTracker still satisfies vm.StateDB (extra methods are
// inherited from the embedded *state.StateDB; only key-access methods above
// need shadowing for read/write capture).
var _ = (*rwTracker)(nil)

// import stub for unused packages when tracker has no big.Int math
var _ = big.NewInt
