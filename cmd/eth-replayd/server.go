package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"

	"Nezha/cmd/eth-replayd/replayd"
)

type replayServer struct {
	reader   *replayd.DatasetReader
	executor *replayd.TxExecutor

	mu               sync.RWMutex
	currentBlock     uint64
	currentBlockData *replayd.BlockDataset
	baseState        *state.StateDB
	blockEnv         *replayd.BlockEnv
	preStateRoot     common.Hash
	concurrency      int
}

// --- HTTP request / response types ---

type LoadBlockReq struct {
	BlockNum uint64 `json:"blockNum"`
}

type LoadBlockResp struct {
	Ok           bool     `json:"ok"`
	TxCount      int      `json:"txCount"`
	PreStateRoot string   `json:"preStateRoot"`
	BlockNumber  uint64   `json:"blockNumber"`
	WitnessAccts int      `json:"witnessAccounts"`
	TxHashes     []string `json:"txHashes,omitempty"`
}

type PreExecuteReq struct {
	BlockNum uint64 `json:"blockNum"`
	TxIdx    int    `json:"txIdx"`
}

type PreExecuteResp struct {
	Ok        bool     `json:"ok"`
	TxIdx     int      `json:"txIdx"`
	TxHash    string   `json:"txHash"`
	Success   bool     `json:"success"`
	GasUsed   uint64   `json:"gasUsed"`
	ReadKeys  []string `json:"readKeys"`
	WriteKeys []string `json:"writeKeys"`
	Error     string   `json:"error,omitempty"`
}

type PreExecuteAllReq struct {
	BlockNum uint64 `json:"blockNum"`
}

type PreExecuteAllResp struct {
	Ok         bool             `json:"ok"`
	Results    []PreExecuteResp `json:"results"`
	DurationMs int64            `json:"durationMs"`
}

type SerialExecuteAllReq struct {
	BlockNum uint64 `json:"blockNum"`
}

type SerialExecuteAllResp struct {
	Ok         bool             `json:"ok"`
	Results    []PreExecuteResp `json:"results"`
	DurationMs int64            `json:"durationMs"`
}

type InfoResp struct {
	FromBlock    int    `json:"fromBlock"`
	ToBlock      int    `json:"toBlock"`
	CurrentBlock uint64 `json:"currentBlock"`
	TxCount      int    `json:"txCount"`
}

// --- handlers ---

func (s *replayServer) info(w http.ResponseWriter, r *http.Request) {
	manifest := s.reader.Manifest()
	s.mu.RLock()
	txCount := 0
	if s.currentBlockData != nil {
		txCount = len(s.currentBlockData.Transactions)
	}
	cb := s.currentBlock
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, InfoResp{
		FromBlock:    manifest.FromBlock,
		ToBlock:      manifest.ToBlock,
		CurrentBlock: cb,
		TxCount:      txCount,
	})
}

func (s *replayServer) loadBlock(w http.ResponseWriter, r *http.Request) {
	var req LoadBlockReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}

	blockData, err := s.reader.LoadBlock(req.BlockNum)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}

	blockEnv := replayd.BuildBlockEnv(blockData.Header, map[uint64]common.Hash{})
	baseState, preRoot, err := replayd.BuildBaseStateDB(blockData.Witness, req.BlockNum, s.executor.ChainConfig())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "build state: "+err.Error())
		return
	}

	txHashes := make([]string, 0, len(blockData.RWSets))
	for _, tr := range blockData.RWSets {
		txHashes = append(txHashes, tr.TxHash)
	}

	accts := 0
	if blockData.Witness != nil {
		accts = len(blockData.Witness.Accounts)
	}

	s.mu.Lock()
	s.currentBlock = req.BlockNum
	s.currentBlockData = blockData
	s.baseState = baseState
	s.blockEnv = blockEnv
	s.preStateRoot = preRoot
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, LoadBlockResp{
		Ok:           true,
		TxCount:      len(blockData.Transactions),
		PreStateRoot: preRoot.Hex(),
		BlockNumber:  req.BlockNum,
		WitnessAccts: accts,
		TxHashes:     txHashes,
	})
}

func (s *replayServer) preExecute(w http.ResponseWriter, r *http.Request) {
	var req PreExecuteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	res, err := s.doPreExecute(req.BlockNum, req.TxIdx)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *replayServer) preExecuteAll(w http.ResponseWriter, r *http.Request) {
	var req PreExecuteAllReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}

	s.mu.RLock()
	data := s.currentBlockData
	if data == nil || s.currentBlock != req.BlockNum {
		s.mu.RUnlock()
		writeErr(w, http.StatusBadRequest, "call /load_block first")
		return
	}
	n := len(data.Transactions)
	s.mu.RUnlock()

	start := time.Now()
	conc := s.concurrency
	if conc <= 0 {
		conc = runtime.NumCPU()
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	results := make([]PreExecuteResp, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := s.doPreExecute(req.BlockNum, idx)
			if err != nil {
				results[idx] = PreExecuteResp{Ok: false, TxIdx: idx, Error: err.Error()}
				return
			}
			results[idx] = *res
		}(i)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, PreExecuteAllResp{
		Ok:         true,
		Results:    results,
		DurationMs: time.Since(start).Milliseconds(),
	})
}

// doPreExecute: single tx run with state clone isolation.
func (s *replayServer) doPreExecute(blockNum uint64, txIdx int) (*PreExecuteResp, error) {
	s.mu.RLock()
	if s.currentBlockData == nil || s.currentBlock != blockNum {
		s.mu.RUnlock()
		return nil, fmt.Errorf("block %d not loaded; POST /load_block first", blockNum)
	}
	if txIdx < 0 || txIdx >= len(s.currentBlockData.Transactions) {
		s.mu.RUnlock()
		return nil, fmt.Errorf("txIdx %d out of range", txIdx)
	}
	txRaw := s.currentBlockData.Transactions[txIdx]
	baseState := s.baseState.Copy()
	blockEnv := *s.blockEnv
	s.mu.RUnlock()

	res, err := s.executor.PreExecuteTx(baseState, &blockEnv, txRaw, txIdx)
	if err != nil {
		return nil, fmt.Errorf("preExecuteTx: %w", err)
	}
	return &PreExecuteResp{
		Ok:        true,
		TxIdx:     res.TxIndex,
		TxHash:    res.TxHash,
		Success:   res.Success,
		GasUsed:   res.GasUsed,
		ReadKeys:  res.ReadKeys,
		WriteKeys: res.WriteKeys,
		Error:     res.Error,
	}, nil
}

// doSerialExecute: single tx run on a persistent state (no copy).
// Success commits changes to state; failure reverts.
func (s *replayServer) doSerialExecute(state *state.StateDB, blockEnv *replayd.BlockEnv, txRaw interface{}, txIdx int) (*PreExecuteResp, error) {
	res, err := s.executor.PreExecuteTx(state, blockEnv, txRaw, txIdx)
	if err != nil {
		return nil, fmt.Errorf("serialExecuteTx: %w", err)
	}
	return &PreExecuteResp{
		Ok:        true,
		TxIdx:     res.TxIndex,
		TxHash:    res.TxHash,
		Success:   res.Success,
		GasUsed:   res.GasUsed,
		ReadKeys:  res.ReadKeys,
		WriteKeys: res.WriteKeys,
		Error:     res.Error,
	}, nil
}

func (s *replayServer) serialExecuteAll(w http.ResponseWriter, r *http.Request) {
	var req SerialExecuteAllReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}

	s.mu.RLock()
	data := s.currentBlockData
	if data == nil || s.currentBlock != req.BlockNum {
		s.mu.RUnlock()
		writeErr(w, http.StatusBadRequest, "call /load_block first")
		return
	}
	blockEnv := *s.blockEnv
	s.mu.RUnlock()

	state := s.baseState.Copy()
	n := len(data.Transactions)
	results := make([]PreExecuteResp, n)
	start := time.Now()

	for i := 0; i < n; i++ {
		res, err := s.doSerialExecute(state, &blockEnv, data.Transactions[i], i)
		if err != nil {
			results[i] = PreExecuteResp{Ok: false, TxIdx: i, Error: err.Error()}
		} else {
			results[i] = *res
		}
	}

	writeJSON(w, http.StatusOK, SerialExecuteAllResp{
		Ok:         true,
		Results:    results,
		DurationMs: time.Since(start).Milliseconds(),
	})
}

// StartHTTPServer constructs and starts the replay daemon HTTP service.
// It blocks until the server shuts down.
func StartHTTPServer(listenAddr string, datasetDir string, concurrency int) error {
	reader, err := replayd.NewDatasetReader(datasetDir)
	if err != nil {
		return fmt.Errorf("open dataset: %w", err)
	}
	manifest := reader.Manifest()
	log.Printf("[replayd] Dataset: %d-%d, chain %d", manifest.FromBlock, manifest.ToBlock, manifest.ChainID)

	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	s := &replayServer{
		reader:      reader,
		executor:    replayd.NewTxExecutor(),
		concurrency: concurrency,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/info", s.info)
	mux.HandleFunc("/load_block", s.loadBlock)
	mux.HandleFunc("/pre_execute", s.preExecute)
	mux.HandleFunc("/pre_execute_all", s.preExecuteAll)
	mux.HandleFunc("/serial_execute_all", s.serialExecuteAll)

	httpSrv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 300 * time.Second,
	}
	log.Printf("[replayd] HTTP listening on %s (conc=%d)", listenAddr, concurrency)
	return httpSrv.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]interface{}{"ok": false, "error": msg})
}
