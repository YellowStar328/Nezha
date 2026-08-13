package core

// ReplayRef is a lightweight pointer to a specific dataset block + tx index
// used throughout the scheduler pipeline to resolve back to canonical data.
type ReplayRef struct {
	BlockNum  uint64 `json:"blockNum"`
	BlockHash string `json:"blockHash,omitempty"`
	TxIndex   int    `json:"txIndex"`
	TxHash    string `json:"txHash,omitempty"`
}

// ReplayRWSet is the speculative read/write set produced by PreExecute.
// Keys use the canonical string format:
//
//	acct:<0xaddr_lower>:balance
//	acct:<0xaddr_lower>:nonce
//	acct:<0xaddr_lower>:code
//	slot:<0xaddr_lower>:<0xslot_lower>
type ReplayRWSet struct {
	Ref       ReplayRef `json:"ref"`
	Success   bool      `json:"success"`
	GasUsed   uint64    `json:"gasUsed"`
	ReadKeys  []string  `json:"readKeys"`
	WriteKeys []string  `json:"writeKeys"`
	Error     string    `json:"error,omitempty"`
}

// CanonicalRWSet holds the dataset-provided "ground truth" read/write keys
// captured from mainnet debug_traceTransaction (prestateTracer + diffMode).
// Schedulers do NOT consume these during execution (to avoid cheating).
// They are only used in Validate() to compute false-positive/negative and
// abort rates for benchmark reports.
type CanonicalRWSet struct {
	Ref       ReplayRef `json:"ref"`
	ReadKeys  []string  `json:"readKeys"`
	WriteKeys []string  `json:"writeKeys"`
}

// ReplayExecResult describes the outcome of executing a single tx in either
// serial, ClassicalGraph (CG), or Depurge schedule. Executors return this
// so the harness can collect abort reasons and RW coverage.
type ReplayExecResult struct {
	Ref       ReplayRef `json:"ref"`
	Committed bool      `json:"committed"` // false if aborted / retried & still failed
	AbortMsg  string    `json:"abortMsg,omitempty"`
	Retries   int       `json:"retries"`
	// Speculative (pre-execution) and post-validation keys for reporting.
	SpecReadKeys  []string `json:"specReadKeys,omitempty"`
	SpecWriteKeys []string `json:"specWriteKeys,omitempty"`
}

// ReplayBlock is the scheduler-agnostic view of a block loaded from the
// dataset. It contains everything a harness needs to run a scheduling
// algorithm: an ordered list of tx references, the ground-truth RW sets
// (for validation only), and the total size.
type ReplayBlock struct {
	BlockNum   uint64          `json:"blockNum"`
	BlockHash  string          `json:"blockHash,omitempty"`
	TxCount    int             `json:"txCount"`
	Refs       []ReplayRef     `json:"refs"`       // ordered list [0..TxCount-1]
	Canonical  []CanonicalRWSet `json:"canonical"`  // same length as Refs
	Timestamp  uint64          `json:"timestamp"`
}

// ReplayExecutor is the pluggable interface the schedulers depend on.
// Implementations (e.g. replayd HTTP client) provide speculative PreExecute
// of individual txs against the current committed pre-state.
//
// Scheduling algorithms call PreExecute on a fresh block clone for every tx.
type ReplayExecutor interface {
	// LoadBlock asks the executor to load & warm up the pre-state for blockNum.
	// Returns a ReplayBlock with TxCount.
	LoadBlock(blockNum uint64) (*ReplayBlock, error)

	// PreExecute returns the speculative read/write set for tx at index txIdx
	// within the last LoadBlock'd block. The returned set is allowed to be
	// an over-approximation (false positives); Validate will catch misses.
	PreExecute(blockNum uint64, txIdx int) (*ReplayRWSet, error)

	// PreExecuteAll returns speculative RW sets for all txs in parallel.
	// The returned slice is indexed by txIdx.
	PreExecuteAll(blockNum uint64) ([]*ReplayRWSet, error)

	// Close releases any open connections to the replay service.
	Close() error
}
