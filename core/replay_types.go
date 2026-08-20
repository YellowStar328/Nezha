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

// ReplayBlock is the scheduler-agnostic view of a block loaded from the
// dataset. It contains everything a harness needs to run a scheduling
// algorithm: an ordered list of tx references, the ground-truth RW sets
// (for validation only), and the total size.
type ReplayBlock struct {
	BlockNum  uint64           `json:"blockNum"`
	BlockHash string           `json:"blockHash,omitempty"`
	TxCount   int              `json:"txCount"`
	Refs      []ReplayRef      `json:"refs"`      // ordered list [0..TxCount-1]
	Canonical []CanonicalRWSet `json:"canonical"` // same length as Refs
	Timestamp uint64           `json:"timestamp"`
}
