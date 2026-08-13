package exporter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Checkpoint struct {
	LastBlock    uint64 `json:"lastBlock"`
	UpdatedAt    string `json:"updatedAt"`
	TotalBlocks  int    `json:"totalBlocks"`
}

func ReadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func WriteCheckpoint(path string, cp *Checkpoint) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}
