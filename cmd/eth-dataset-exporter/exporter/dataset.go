package exporter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type DatasetWriter struct {
	baseDir  string
	mu       sync.Mutex
	encoders map[string]*zstd.Encoder
	codeSet  *CodeStore
}

func NewDatasetWriter(baseDir string) (*DatasetWriter, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("create base dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(baseDir, "blocks"), 0755); err != nil {
		return nil, fmt.Errorf("create blocks dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(baseDir, "code"), 0755); err != nil {
		return nil, fmt.Errorf("create code dir: %w", err)
	}

	return &DatasetWriter{
		baseDir:  baseDir,
		encoders: make(map[string]*zstd.Encoder),
		codeSet:  NewCodeStore(),
	}, nil
}

func (dw *DatasetWriter) WriteManifest(manifest *DatasetManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return os.WriteFile(filepath.Join(dw.baseDir, "manifest.json"), data, 0644)
}

func (dw *DatasetWriter) WriteBlock(blockNum uint64, dataset *BlockDataset) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	blockPath := filepath.Join(dw.baseDir, "blocks", fmt.Sprintf("%d.json.zst", blockNum))

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return fmt.Errorf("create zstd encoder for block %d: %w", blockNum, err)
	}

	data, err := json.MarshalIndent(dataset, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal block %d: %w", blockNum, err)
	}

	compressed := encoder.EncodeAll(data, nil)

	if err := os.WriteFile(blockPath, compressed, 0644); err != nil {
		return fmt.Errorf("write block %d: %w", blockNum, err)
	}

	encoder.Close()

	return nil
}

func (dw *DatasetWriter) WriteCode(codeHash string, codeBytes []byte) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if !dw.codeSet.Add(codeHash) {
		return nil
	}

	codePath := filepath.Join(dw.baseDir, "code", codeHash+".bin.zst")

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return fmt.Errorf("create zstd encoder for code %s: %w", codeHash, err)
	}

	compressed := encoder.EncodeAll(codeBytes, nil)

	if err := os.WriteFile(codePath, compressed, 0644); err != nil {
		return fmt.Errorf("write code %s: %w", codeHash, err)
	}

	encoder.Close()

	return nil
}

func (dw *DatasetWriter) Close() {
	for _, enc := range dw.encoders {
		enc.Close()
	}
}
