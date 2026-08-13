package replayd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

type BlockDataset struct {
	Header       map[string]interface{} `json:"header"`
	Transactions []interface{}          `json:"transactions"`
	Witness      *BlockWitness          `json:"witness"`
	Canonical    *CanonicalBlock        `json:"canonical"`
	RWSets       []TxRWSets             `json:"rwsets"`
}

type BlockWitness struct {
	Accounts map[string]*WitnessAccount `json:"accounts"`
}

type WitnessAccount struct {
	Balance  string            `json:"balance"`
	Nonce    string            `json:"nonce"`
	CodeHash string            `json:"codeHash"`
	Code     string            `json:"code,omitempty"`
	Storage  map[string]string `json:"storage"`
}

type TxRWSets struct {
	TxHash    string   `json:"txHash"`
	TxIndex   int      `json:"txIndex"`
	ReadKeys  []string `json:"readKeys"`
	WriteKeys []string `json:"writeKeys"`
}

type CanonicalReceipt struct {
	TxHash    string `json:"txHash"`
	TxIndex   int    `json:"txIndex"`
	Status    uint64 `json:"status"`
	GasUsed   uint64 `json:"gasUsed"`
	LogsCount int    `json:"logsCount"`
}

type CanonicalBlock struct {
	Receipts []CanonicalReceipt `json:"receipts"`
}

type DatasetManifest struct {
	FormatVersion int    `json:"formatVersion"`
	ChainID       int    `json:"chainId"`
	FromBlock     int    `json:"fromBlock"`
	ToBlock       int    `json:"toBlock"`
	ExportedAt    string `json:"exportedAt"`
	StateAnchor   string `json:"stateAnchor"`
	ExecutionMode string `json:"executionMode"`
	SourceClient  string `json:"sourceClient"`
	HashWindow    int    `json:"hashWindow"`
}

type DatasetReader struct {
	baseDir  string
	manifest *DatasetManifest
	cache    map[uint64]*BlockDataset
}

func NewDatasetReader(baseDir string) (*DatasetReader, error) {
	manifestPath := filepath.Join(baseDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest DatasetManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}

	return &DatasetReader{
		baseDir:  baseDir,
		manifest: &manifest,
		cache:    make(map[uint64]*BlockDataset),
	}, nil
}

func (dr *DatasetReader) LoadBlock(blockNum uint64) (*BlockDataset, error) {
	if cached, ok := dr.cache[blockNum]; ok {
		return cached, nil
	}

	blockPath := filepath.Join(dr.baseDir, "blocks", fmt.Sprintf("%d.json.zst", blockNum))
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, fmt.Errorf("read block %d: %w", blockNum, err)
	}

	decoder, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create zstd reader for block %d: %w", blockNum, err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", blockNum, err)
	}

	var dataset BlockDataset
	if err := json.Unmarshal(decompressed, &dataset); err != nil {
		return nil, fmt.Errorf("parse block %d: %w", blockNum, err)
	}

	dr.cache[blockNum] = &dataset
	return &dataset, nil
}

func (dr *DatasetReader) LoadCode(codeHash string) ([]byte, error) {
	codePath := filepath.Join(dr.baseDir, "code", codeHash+".bin.zst")
	data, err := os.ReadFile(codePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read code %s: %w", codeHash, err)
	}

	decoder, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create zstd reader for code %s: %w", codeHash, err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress code %s: %w", codeHash, err)
	}

	return decompressed, nil
}

func (dr *DatasetReader) ClearCache() {
	dr.cache = make(map[uint64]*BlockDataset)
}

func (dr *DatasetReader) Manifest() *DatasetManifest {
	return dr.manifest
}
