package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"

	"Nezha/core"
)

// RawTransaction holds the minimal tx fields needed for spec generation.
type RawTransaction struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"`
	Input string `json:"input"`
	Value string `json:"value"`
}

// DatasetManifest mirrors eth-dataset-exporter's `manifest.json` so the
// scheduler knows what block range is available locally.
type DatasetManifest struct {
	FromBlock int    `json:"fromBlock"`
	ToBlock   int    `json:"toBlock"`
	ChainID   uint64 `json:"chainID"`
	Note      string `json:"note,omitempty"`
}

// BlockDatasetRaw is the JSON schema stored inside `<blockNum>.json.zst`.
// We copy only the fields needed by the harness; everything else is opaque
// and forwarded to the replay executor (which understands the full format).
type BlockDatasetRaw struct {
	BlockNumber uint64 `json:"blockNumber"`
	BlockHash   string `json:"blockHash,omitempty"`
	Timestamp   uint64 `json:"timestamp,omitempty"`
	// Ordered list of mainnet rwsets. Used for Validate/abort reports only
	// (never consumed as input to scheduling).
	RWSets []struct {
		TxIndex   int      `json:"txIndex"`
		TxHash    string   `json:"txHash"`
		ReadKeys  []string `json:"readKeys"`
		WriteKeys []string `json:"writeKeys"`
	} `json:"rwsets"`
}

// DatasetReader reads .zst-compressed block dataset files from disk and
// exposes them as core.ReplayBlock. It caches decoded blocks in-memory so
// repeated runs of multiple schedulers don't re-read disk.
type DatasetReader struct {
	dir      string
	manifest DatasetManifest

	mu      sync.RWMutex
	cache   map[uint64]*core.ReplayBlock
	txCache map[uint64][]RawTransaction
	dec     *zstd.Decoder
}

// NewDatasetReader opens a dataset directory and validates the manifest.
func NewDatasetReader(dir string) (*DatasetReader, error) {
	manPath := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(manPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", manPath, err)
	}
	var m DatasetManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("init zstd: %w", err)
	}
	return &DatasetReader{
		dir:      dir,
		manifest: m,
		cache:    make(map[uint64]*core.ReplayBlock),
		txCache:  make(map[uint64][]RawTransaction),
		dec:      dec,
	}, nil
}

// Manifest returns the loaded manifest for range inspection.
func (dr *DatasetReader) Manifest() DatasetManifest { return dr.manifest }

// LoadBlock returns a scheduler-compatible ReplayBlock for blockNum.
// The block is cached after the first successful decode.
func (dr *DatasetReader) LoadBlock(blockNum uint64) (*core.ReplayBlock, error) {
	dr.mu.RLock()
	if blk, ok := dr.cache[blockNum]; ok {
		dr.mu.RUnlock()
		return blk, nil
	}
	dr.mu.RUnlock()

	dr.mu.Lock()
	defer dr.mu.Unlock()
	// double-check after lock
	if blk, ok := dr.cache[blockNum]; ok {
		return blk, nil
	}
	if blockNum < uint64(dr.manifest.FromBlock) || blockNum > uint64(dr.manifest.ToBlock) {
		return nil, fmt.Errorf("block %d outside manifest [%d,%d]", blockNum, dr.manifest.FromBlock, dr.manifest.ToBlock)
	}
	p := filepath.Join(dr.dir, "blocks", fmt.Sprintf("%d.json.zst", blockNum))
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("open block %d: %w", blockNum, err)
	}
	decompressed, err := dr.dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", blockNum, err)
	}
	var rawBlock BlockDatasetRaw
	if err := json.Unmarshal(decompressed, &rawBlock); err != nil {
		return nil, fmt.Errorf("parse block %d json: %w", blockNum, err)
	}

	n := len(rawBlock.RWSets)
	refs := make([]core.ReplayRef, n)
	canonical := make([]core.CanonicalRWSet, n)
	for i, tr := range rawBlock.RWSets {
		ref := core.ReplayRef{
			BlockNum:  blockNum,
			BlockHash: rawBlock.BlockHash,
			TxIndex:   i,
			TxHash:    tr.TxHash,
		}
		refs[i] = ref
		canonical[i] = core.CanonicalRWSet{
			Ref:       ref,
			ReadKeys:  append([]string(nil), tr.ReadKeys...),
			WriteKeys: append([]string(nil), tr.WriteKeys...),
		}
	}
	blk := &core.ReplayBlock{
		BlockNum:  blockNum,
		BlockHash: rawBlock.BlockHash,
		Timestamp: rawBlock.Timestamp,
		TxCount:   n,
		Refs:      refs,
		Canonical: canonical,
	}
	dr.cache[blockNum] = blk
	return blk, nil
}

// blockTxsRaw is the minimal JSON schema for parsing the "transactions" array
// from a block dataset file, without decoding the full block.
type blockTxsRaw struct {
	Transactions []RawTransaction `json:"transactions"`
}

// ReplayWitnessAccount mirrors the witness account schema stored in the block
// dataset file. Defined here (in utils) so the root module can read witness
// state without importing the cmd/eth-replayd module.
type ReplayWitnessAccount struct {
	Balance  string            `json:"balance"`
	Nonce    string            `json:"nonce"`
	CodeHash string            `json:"codeHash"`
	Code     string            `json:"code,omitempty"`
	Storage  map[string]string `json:"storage"`
}

// ReplayBlockWitness is the witness section of a block dataset.
type ReplayBlockWitness struct {
	Accounts map[string]*ReplayWitnessAccount `json:"accounts"`
}

// blockWitnessRaw is the minimal JSON schema for parsing the "witness" section
// from a block dataset file, without decoding the full block.
type blockWitnessRaw struct {
	Witness *ReplayBlockWitness `json:"witness"`
}

// LoadBlockTxs reads raw transaction data (to, input, hash) for a block.
// This is used by the LLM spec executor to decode selectors and args.
// The result is cached in txCache, mirroring LoadBlock's caching strategy.
func (dr *DatasetReader) LoadBlockTxs(blockNum uint64) ([]RawTransaction, error) {
	dr.mu.RLock()
	if txs, ok := dr.txCache[blockNum]; ok {
		dr.mu.RUnlock()
		return txs, nil
	}
	dr.mu.RUnlock()

	dr.mu.Lock()
	defer dr.mu.Unlock()
	// double-check after lock
	if txs, ok := dr.txCache[blockNum]; ok {
		return txs, nil
	}
	if blockNum < uint64(dr.manifest.FromBlock) || blockNum > uint64(dr.manifest.ToBlock) {
		return nil, fmt.Errorf("block %d outside manifest [%d,%d]", blockNum, dr.manifest.FromBlock, dr.manifest.ToBlock)
	}
	p := filepath.Join(dr.dir, "blocks", fmt.Sprintf("%d.json.zst", blockNum))
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("open block %d: %w", blockNum, err)
	}
	decompressed, err := dr.dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", blockNum, err)
	}
	var blk blockTxsRaw
	if err := json.Unmarshal(decompressed, &blk); err != nil {
		return nil, fmt.Errorf("parse block %d transactions: %w", blockNum, err)
	}
	txs := make([]RawTransaction, len(blk.Transactions))
	copy(txs, blk.Transactions)
	dr.txCache[blockNum] = txs
	return txs, nil
}

// LoadBlockWitness reads the witness (pre-state accounts + storage) for a
// block. Used to initialize committedState before parallel re-execution.
// The witness is read from the same .json.zst block file and is NOT cached
// (it can be large); callers should hold onto the returned value.
func (dr *DatasetReader) LoadBlockWitness(blockNum uint64) (*ReplayBlockWitness, error) {
	if blockNum < uint64(dr.manifest.FromBlock) || blockNum > uint64(dr.manifest.ToBlock) {
		return nil, fmt.Errorf("block %d outside manifest [%d,%d]", blockNum, dr.manifest.FromBlock, dr.manifest.ToBlock)
	}
	p := filepath.Join(dr.dir, "blocks", fmt.Sprintf("%d.json.zst", blockNum))
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("open block %d: %w", blockNum, err)
	}
	decompressed, err := dr.dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", blockNum, err)
	}
	var blk blockWitnessRaw
	if err := json.Unmarshal(decompressed, &blk); err != nil {
		return nil, fmt.Errorf("parse block %d witness: %w", blockNum, err)
	}
	if blk.Witness == nil {
		return nil, fmt.Errorf("block %d has no witness section", blockNum)
	}
	return blk.Witness, nil
}

// LoadBlockRange loads blocks [fromBlock, toBlock] and concatenates their
// Refs + Canonical into a single synthetic ReplayBlock. This is used by the
// multi-block replay mode where txs from a range of blocks are scheduled
// together against a single baseline state (the witness of fromBlock).
//
// The returned ReplayBlock has:
//   - BlockNum = fromBlock
//   - BlockHash = fromBlock's hash
//   - TxCount = total txs across all blocks
//   - Refs = concatenated refs (each preserves its original BlockNum + TxIndex)
//   - Canonical = concatenated canonical RW sets
func (dr *DatasetReader) LoadBlockRange(fromBlock, toBlock uint64) (*core.ReplayBlock, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("fromBlock %d > toBlock %d", fromBlock, toBlock)
	}
	if fromBlock < uint64(dr.manifest.FromBlock) || toBlock > uint64(dr.manifest.ToBlock) {
		return nil, fmt.Errorf("range [%d,%d] outside manifest [%d,%d]",
			fromBlock, toBlock, dr.manifest.FromBlock, dr.manifest.ToBlock)
	}

	var allRefs []core.ReplayRef
	var allCanonical []core.CanonicalRWSet
	var firstBlockHash string
	var firstTimestamp uint64

	for bn := fromBlock; bn <= toBlock; bn++ {
		blk, err := dr.LoadBlock(bn)
		if err != nil {
			return nil, fmt.Errorf("LoadBlock %d: %w", bn, err)
		}
		if bn == fromBlock {
			firstBlockHash = blk.BlockHash
			firstTimestamp = blk.Timestamp
		}
		allRefs = append(allRefs, blk.Refs...)
		allCanonical = append(allCanonical, blk.Canonical...)
	}

	return &core.ReplayBlock{
		BlockNum:  fromBlock,
		BlockHash: firstBlockHash,
		Timestamp: firstTimestamp,
		TxCount:   len(allRefs),
		Refs:      allRefs,
		Canonical: allCanonical,
	}, nil
}

// LoadBlockRangeTxs loads raw transactions from blocks [fromBlock, toBlock]
// and concatenates them into a single slice. The txs are in block order then
// intra-block order. Used by LevmSpecFallback to execute multi-block ranges.
func (dr *DatasetReader) LoadBlockRangeTxs(fromBlock, toBlock uint64) ([]RawTransaction, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("fromBlock %d > toBlock %d", fromBlock, toBlock)
	}
	var allTxs []RawTransaction
	for bn := fromBlock; bn <= toBlock; bn++ {
		txs, err := dr.LoadBlockTxs(bn)
		if err != nil {
			return nil, fmt.Errorf("LoadBlockTxs %d: %w", bn, err)
		}
		allTxs = append(allTxs, txs...)
	}
	return allTxs, nil
}
