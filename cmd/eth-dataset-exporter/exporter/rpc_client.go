package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type RPCClient struct {
	ethClient  *ethclient.Client
	rpcClient  *rpc.Client
	endpoint   string
	maxRetries int
	rateCh     chan struct{}
}

// NewRPCClient creates a client with a global rate limiter (~10 req/sec,
// under Alchemy PAYG's ~16 traces/sec ceiling for 40 CU/trace).
func NewRPCClient(endpoint string) (*RPCClient, error) {
	rpcClient, err := rpc.Dial(endpoint)
	if err != nil {
		return nil, fmt.Errorf("rpc dial: %w", err)
	}
	ethClient := ethclient.NewClient(rpcClient)
	c := &RPCClient{
		ethClient:  ethClient,
		rpcClient:  rpcClient,
		endpoint:   endpoint,
		maxRetries: 10,
		rateCh:     make(chan struct{}, 1),
	}
	// Refill token every 100ms = 10 req/sec (debug_traceTransaction = 40 CU,
	// Alchemy PAYG ~660 CU/sec → ~16 traces/sec, leave headroom)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case c.rateCh <- struct{}{}:
			default:
			}
		}
	}()
	return c, nil
}

func (c *RPCClient) Close() {
	c.ethClient.Close()
}

func (c *RPCClient) GetBlock(ctx context.Context, blockNum uint64) (*types.Block, error) {
	var block *types.Block
	err := c.retry(ctx, func() error {
		var err error
		block, err = c.ethClient.BlockByNumber(ctx, new(big.Int).SetUint64(blockNum))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get block %d: %w", blockNum, err)
	}
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	return block, nil
}

func (c *RPCClient) GetBlockReceipts(ctx context.Context, blockNum uint64) (types.Receipts, error) {
	var receipts types.Receipts
	err := c.retry(ctx, func() error {
		return c.rpcClient.CallContext(ctx, &receipts, "eth_getBlockReceipts", fmt.Sprintf("0x%x", blockNum))
	})
	if err != nil {
		return nil, fmt.Errorf("get receipts for block %d: %w", blockNum, err)
	}
	return receipts, nil
}

func (c *RPCClient) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, err := c.ethClient.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("get receipt %s: %w", txHash.Hex(), err)
	}
	return receipt, nil
}

type TraceConfig struct {
	DiffMode bool
}

func (c *RPCClient) TraceTx(ctx context.Context, txHash common.Hash, diffMode bool) (*PrestateTracerResult, error) {
	var tracerConfig interface{}
	if diffMode {
		tracerConfig = map[string]interface{}{
			"tracer": "prestateTracer",
			"tracerConfig": map[string]interface{}{
				"diffMode": true,
			},
		}
	} else {
		tracerConfig = map[string]interface{}{
			"tracer": "prestateTracer",
		}
	}

	var rawJSON json.RawMessage
	err := c.retry(ctx, func() error {
		return c.rpcClient.CallContext(ctx, &rawJSON, "debug_traceTransaction", txHash.Hex(), tracerConfig)
	})
	if err != nil {
		return nil, fmt.Errorf("trace tx %s (diffMode=%v): %w", txHash.Hex(), diffMode, err)
	}

	result, err := ParsePrestateTracerResult(rawJSON, diffMode)
	if err != nil {
		return nil, fmt.Errorf("parse trace tx %s: %w", txHash.Hex(), err)
	}
	return result, nil
}

func (c *RPCClient) GetBlockHashAt(ctx context.Context, blockNum uint64) (common.Hash, error) {
	block, err := c.GetBlock(ctx, blockNum)
	if err != nil {
		return common.Hash{}, err
	}
	return block.Hash(), nil
}

func (c *RPCClient) GetBlockHashWindow(ctx context.Context, currentBlock uint64, windowSize int) (map[uint64]common.Hash, error) {
	result := make(map[uint64]common.Hash, windowSize)
	end := currentBlock
	if windowSize > int(currentBlock) {
		windowSize = int(currentBlock)
	}
	start := end - uint64(windowSize)

	for i := 0; i < windowSize; i++ {
		bn := start + uint64(i)
		hash, err := c.GetBlockHashAt(ctx, bn)
		if err != nil {
			return nil, fmt.Errorf("get block hash window at %d: %w", bn, err)
		}
		result[bn] = hash
	}
	return result, nil
}

func (c *RPCClient) retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		// Rate limit: wait for a token before every attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.rateCh:
		}
		if attempt > 0 {
			// Longer backoff for 429s (start at 500ms, cap at 30s)
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryableError(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("all retries failed: %w", lastErr)
}

func isRetryableError(err error) bool {
	errStr := err.Error()
	if err == ethereum.NotFound {
		return false
	}
	if contains(errStr, "429") || contains(errStr, "Too Many Requests") || contains(errStr, "rate limit") {
		return true
	}
	if contains(errStr, "timeout") || contains(errStr, "connection refused") || contains(errStr, "EOF") {
		return true
	}
	return false
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func HexToUint64(s string) (uint64, error) {
	s = s[2:]
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func BigFromHex(s string) *big.Int {
	v := new(big.Int)
	v.SetString(s, 0)
	return v
}
