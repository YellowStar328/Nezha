package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"Nezha/core"
)

// HTTPReplayExecutor is a core.ReplayExecutor implementation that talks to a
// remote eth-replayd HTTP server (see cmd/eth-replayd/server.go).
//
// Schedulers use this to obtain speculative read/write sets without taking
// a direct dependency on geth.
type HTTPReplayExecutor struct {
	baseURL   string
	client    *http.Client
	lastBlock uint64 // last LoadBlock'd number (for sanity checks)
}

// NewHTTPReplayExecutor connects to an already-running eth-replayd instance.
func NewHTTPReplayExecutor(endpoint string) (*HTTPReplayExecutor, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("replayd endpoint is empty")
	}
	return &HTTPReplayExecutor{
		baseURL: endpoint,
		client:  &http.Client{Timeout: 10 * time.Minute},
	}, nil
}

// --- HTTP DTOs (mirrors server.go) ---

type infoResp struct {
	FromBlock    int    `json:"fromBlock"`
	ToBlock      int    `json:"toBlock"`
	CurrentBlock uint64 `json:"currentBlock"`
	TxCount      int    `json:"txCount"`
}

type loadBlockReq struct {
	BlockNum uint64 `json:"blockNum"`
}
type loadBlockResp struct {
	Ok           bool     `json:"ok"`
	Error        string   `json:"error,omitempty"`
	TxCount      int      `json:"txCount"`
	PreStateRoot string   `json:"preStateRoot"`
	BlockNumber  uint64   `json:"blockNumber"`
	WitnessAccts int      `json:"witnessAccounts"`
	TxHashes     []string `json:"txHashes"`
}

type preExecReq struct {
	BlockNum uint64 `json:"blockNum"`
	TxIdx    int    `json:"txIdx"`
}
type preExecResp struct {
	Ok        bool     `json:"ok"`
	Error     string   `json:"error,omitempty"`
	TxIdx     int      `json:"txIdx"`
	TxHash    string   `json:"txHash"`
	Success   bool     `json:"success"`
	GasUsed   uint64   `json:"gasUsed"`
	ReadKeys  []string `json:"readKeys"`
	WriteKeys []string `json:"writeKeys"`
}

type preExecAllReq struct {
	BlockNum uint64 `json:"blockNum"`
}
type preExecAllResp struct {
	Ok         bool          `json:"ok"`
	Error      string        `json:"error,omitempty"`
	Results    []preExecResp `json:"results"`
	DurationMs int64         `json:"durationMs"`
}

type serialExecAllReq struct {
	BlockNum uint64 `json:"blockNum"`
}

type serialExecAllResp struct {
	Ok         bool          `json:"ok"`
	Error      string        `json:"error,omitempty"`
	Results    []preExecResp `json:"results"`
	DurationMs int64         `json:"durationMs"`
}

// --- core.ReplayExecutor impl ---

// LoadBlock asks replayd to build the pre-state witness for a block.
// It MUST be called before PreExecute / PreExecuteAll on the same block.
//
// Note: this returns metadata (tx count, hashes) only — the full canonical
// ground-truth RW sets are loaded from the dataset via DatasetReader.LoadBlock.
func (h *HTTPReplayExecutor) LoadBlock(blockNum uint64) (*core.ReplayBlock, error) {
	var lb loadBlockResp
	if err := h.doJSON(http.MethodPost, "/load_block", loadBlockReq{BlockNum: blockNum}, &lb); err != nil {
		return nil, err
	}
	if !lb.Ok {
		return nil, fmt.Errorf("replayd /load_block (%d) error: %s", blockNum, lb.Error)
	}
	h.lastBlock = blockNum
	refs := make([]core.ReplayRef, lb.TxCount)
	canonical := make([]core.CanonicalRWSet, lb.TxCount)
	for i, hx := range lb.TxHashes {
		ref := core.ReplayRef{
			BlockNum: blockNum,
			TxIndex:  i,
			TxHash:   hx,
		}
		refs[i] = ref
		canonical[i] = core.CanonicalRWSet{Ref: ref}
	}
	return &core.ReplayBlock{
		BlockNum:  blockNum,
		TxCount:   lb.TxCount,
		Refs:      refs,
		Canonical: canonical,
	}, nil
}

// PreExecute asks replayd to speculatively run a single tx from the last
// loaded block and return the captured RW keys.
func (h *HTTPReplayExecutor) PreExecute(blockNum uint64, txIdx int) (*core.ReplayRWSet, error) {
	var p preExecResp
	if err := h.doJSON(http.MethodPost, "/pre_execute", preExecReq{BlockNum: blockNum, TxIdx: txIdx}, &p); err != nil {
		return nil, err
	}
	if !p.Ok {
		return nil, fmt.Errorf("replayd /pre_execute (%d:%d) error: %s", blockNum, txIdx, p.Error)
	}
	return &core.ReplayRWSet{
		Ref: core.ReplayRef{
			BlockNum: blockNum,
			TxIndex:  p.TxIdx,
			TxHash:   p.TxHash,
		},
		Success:   p.Success,
		GasUsed:   p.GasUsed,
		ReadKeys:  append([]string(nil), p.ReadKeys...),
		WriteKeys: append([]string(nil), p.WriteKeys...),
		Error:     p.Error,
	}, nil
}

// PreExecuteAll runs all txs of a block in parallel on replayd.
func (h *HTTPReplayExecutor) PreExecuteAll(blockNum uint64) ([]*core.ReplayRWSet, error) {
	var p preExecAllResp
	if err := h.doJSON(http.MethodPost, "/pre_execute_all", preExecAllReq{BlockNum: blockNum}, &p); err != nil {
		return nil, err
	}
	if !p.Ok {
		return nil, fmt.Errorf("replayd /pre_execute_all (%d) error: %s", blockNum, p.Error)
	}
	out := make([]*core.ReplayRWSet, len(p.Results))
	for i, r := range p.Results {
		out[i] = &core.ReplayRWSet{
			Ref: core.ReplayRef{
				BlockNum: blockNum,
				TxIndex:  i,
				TxHash:   r.TxHash,
			},
			Success:   r.Success,
			GasUsed:   r.GasUsed,
			ReadKeys:  append([]string(nil), r.ReadKeys...),
			WriteKeys: append([]string(nil), r.WriteKeys...),
			Error:     r.Error,
		}
	}
	return out, nil
}

// SerialExecuteAll runs all txs of a block serially on replayd with state
// accumulation, capturing the actual read/write sets for each tx. This is
// used to determine whether the speculative (PreExecute) RWset is a safe
// over-approximation for parallel scheduling.
func (h *HTTPReplayExecutor) SerialExecuteAll(blockNum uint64) ([]*core.ReplayRWSet, error) {
	var p serialExecAllResp
	if err := h.doJSON(http.MethodPost, "/serial_execute_all", serialExecAllReq{BlockNum: blockNum}, &p); err != nil {
		return nil, err
	}
	if !p.Ok {
		return nil, fmt.Errorf("replayd /serial_execute_all (%d) error: %s", blockNum, p.Error)
	}
	out := make([]*core.ReplayRWSet, len(p.Results))
	for i, r := range p.Results {
		out[i] = &core.ReplayRWSet{
			Ref: core.ReplayRef{
				BlockNum: blockNum,
				TxIndex:  i,
				TxHash:   r.TxHash,
			},
			Success:   r.Success,
			GasUsed:   r.GasUsed,
			ReadKeys:  append([]string(nil), r.ReadKeys...),
			WriteKeys: append([]string(nil), r.WriteKeys...),
			Error:     r.Error,
		}
	}
	return out, nil
}

// Close is a no-op for HTTP (keeps interface symmetry with future gRPC impls).
func (h *HTTPReplayExecutor) Close() error { return nil }

// --- helpers ---

func (h *HTTPReplayExecutor) doJSON(method, path string, body interface{}, into interface{}) error {
	var payload io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal req: %w", err)
		}
		payload = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, h.baseURL+path, payload)
	if err != nil {
		return fmt.Errorf("build req %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, string(raw))
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s resp: %w", path, err)
	}
	if into != nil {
		if err := json.Unmarshal(raw, into); err != nil {
			return fmt.Errorf("parse %s resp: %w  body=%s", path, err, truncate(raw, 300))
		}
	}
	return nil
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
