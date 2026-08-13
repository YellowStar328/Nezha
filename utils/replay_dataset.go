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

	mu     sync.RWMutex
	cache  map[uint64]*core.ReplayBlock
	dec    *zstd.Decoder
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
