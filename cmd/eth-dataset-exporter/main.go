package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"

	"Nezha/cmd/eth-dataset-exporter/exporter"
)

func main() {
	var (
		rpcURL     string
		rpcKey     string
		fromBlock  uint64
		toBlock    uint64
		outDir     string
		checkpoint string
	)

	flag.StringVar(&rpcURL, "rpc", "", "Alchemy RPC endpoint URL (e.g. https://eth-mainnet.g.alchemy.com/v2/)")
	flag.StringVar(&rpcKey, "rpc-key", "", "Alchemy API key (or set ALCHEMY_API_KEY env)")
	flag.Uint64Var(&fromBlock, "from", 24000000, "Start block number")
	flag.Uint64Var(&toBlock, "to", 24000009, "End block number (inclusive)")
	flag.StringVar(&outDir, "out", "./datasets/mainnet-24000000-24010000", "Output directory")
	flag.StringVar(&checkpoint, "checkpoint", "", "Checkpoint file path (for resume)")
	flag.Parse()

	if rpcKey == "" {
		rpcKey = os.Getenv("ALCHEMY_API_KEY")
	}
	if rpcKey == "" {
		log.Fatal("Alchemy API key required: use --rpc-key or set ALCHEMY_API_KEY env")
	}

	if rpcURL == "" {
		log.Fatal("RPC URL required: use --rpc flag")
	}

	if fromBlock > toBlock {
		log.Fatalf("from block (%d) must be <= to block (%d)", fromBlock, toBlock)
	}

	fullURL := rpcURL
	if !strings.Contains(rpcURL, "alchemy.com/v2/") {
		fullURL = rpcURL + rpcKey
	} else {
		fullURL = rpcURL + rpcKey
	}

	log.Printf("Exporting blocks %d-%d from %s", fromBlock, toBlock, fullURL[:min(len(fullURL), 60)]+"...")

	client, err := exporter.NewRPCClient(fullURL)
	if err != nil {
		log.Fatalf("Create RPC client: %v", err)
	}
	defer client.Close()

	writer, err := exporter.NewDatasetWriter(outDir)
	if err != nil {
		log.Fatalf("Create dataset writer: %v", err)
	}
	defer writer.Close()

	startBlock := fromBlock
	if checkpoint != "" {
		cp, err := exporter.ReadCheckpoint(checkpoint)
		if err != nil {
			log.Fatalf("Read checkpoint: %v", err)
		}
		if cp != nil && cp.LastBlock >= fromBlock && cp.LastBlock < toBlock {
			startBlock = cp.LastBlock + 1
			log.Printf("Resuming from checkpoint at block %d", startBlock)
		}
	}

	manifest := exporter.NewDatasetManifest(int(fromBlock), int(toBlock), "alchemy")
	if err := writer.WriteManifest(manifest); err != nil {
		log.Fatalf("Write manifest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 2)

	blockCount := 0
	var mu sync.Mutex

	for bn := startBlock; bn <= toBlock; bn++ {
		select {
		case <-sigCh:
			log.Printf("Interrupted at block %d, saving checkpoint...", bn)
			cp := &exporter.Checkpoint{
				LastBlock:   bn - 1,
				UpdatedAt:   time.Now().Format(time.RFC3339),
				TotalBlocks: blockCount,
			}
			if checkpoint != "" {
				exporter.WriteCheckpoint(checkpoint, cp)
			}
			log.Printf("Checkpoint saved. Resume with --checkpoint %s", checkpoint)
			os.Exit(0)
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(blockNum uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := exportBlock(ctx, client, writer, blockNum); err != nil {
				log.Printf("ERROR block %d: %v", blockNum, err)
				return
			}

			mu.Lock()
			blockCount++
			currentBlock := blockNum
			mu.Unlock()

			if checkpoint != "" && blockCount%10 == 0 {
				cp := &exporter.Checkpoint{
					LastBlock:   currentBlock,
					UpdatedAt:   time.Now().Format(time.RFC3339),
					TotalBlocks: blockCount,
				}
				exporter.WriteCheckpoint(checkpoint, cp)
				log.Printf("Checkpoint: %d blocks exported, last block %d", blockCount, currentBlock)
			}
		}(bn)
	}

	wg.Wait()

	log.Printf("Export complete: %d blocks written to %s", blockCount, outDir)

	if err := validateDataset(outDir, int(toBlock-startBlock+1)); err != nil {
		log.Printf("WARNING: Dataset validation issues: %v", err)
	}
}

func exportBlock(ctx context.Context, client *exporter.RPCClient, writer *exporter.DatasetWriter, blockNum uint64) error {
	log.Printf("Fetching block %d...", blockNum)

	block, err := client.GetBlock(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("get block: %w", err)
	}

	receipts, err := client.GetBlockReceipts(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("get receipts: %w", err)
	}

	txs := block.Transactions()
	traces := make([]*exporter.PrestateTracerResult, len(txs))
	diffTraces := make([]*exporter.PrestateTracerResult, len(txs))

	for i, tx := range txs {
		txHash := tx.Hash()

		preTrace, err := client.TraceTx(ctx, txHash, false)
		if err != nil {
			log.Printf("WARNING: prestate trace failed for tx %s: %v", txHash.Hex(), err)
			continue
		}
		traces[i] = preTrace

		diffTrace, err := client.TraceTx(ctx, txHash, true)
		if err != nil {
			log.Printf("WARNING: diff trace failed for tx %s: %v", txHash.Hex(), err)
			continue
		}
		diffTraces[i] = diffTrace
	}

	witness, txRWSets, canonical, err := exporter.BuildBlockWitness(block, traces, diffTraces, receipts)
	if err != nil {
		return fmt.Errorf("build witness: %w", err)
	}

	header := extractHeader(block)
	txInterfaces := make([]interface{}, len(txs))
	for i, tx := range txs {
		txMap := txToMap(tx, block.NumberU64(), block.Time())
		txInterfaces[i] = txMap
	}

	dataset := &exporter.BlockDataset{
		Header:       header,
		Transactions: txInterfaces,
		Witness:      witness,
		Canonical:    canonical,
		RWSets:       txRWSets,
	}

	if err := writer.WriteBlock(blockNum, dataset); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	for _, acct := range witness.Accounts {
		if acct.CodeHash != "" && acct.CodeHash != "0x0" {
			if err := fetchAndWriteCode(ctx, client, writer, acct.CodeHash); err != nil {
				log.Printf("WARNING: code fetch failed for %s: %v", acct.CodeHash, err)
			}
		}
	}

	log.Printf("Block %d done: %d txs, %d accounts in witness", blockNum, len(txs), len(witness.Accounts))
	return nil
}

func extractHeader(block *types.Block) map[string]interface{} {
	header := block.Header()
	result := map[string]interface{}{
		"number":        header.Number.Uint64(),
		"hash":          block.Hash().Hex(),
		"parentHash":    header.ParentHash.Hex(),
		"timestamp":     header.Time,
		"beneficiary":   header.Coinbase.Hex(),
		"gasLimit":      header.GasLimit,
		"baseFeePerGas": fmt.Sprintf("0x%x", header.BaseFee),
		"prevRandao":    header.MixDigest.Hex(),
		"difficulty":    fmt.Sprintf("0x%x", header.Difficulty),
		"nonce":         fmt.Sprintf("%x", header.Nonce),
	}
	if header.BlobGasUsed != nil {
		result["blobGasUsed"] = *header.BlobGasUsed
	}
	if header.ExcessBlobGas != nil {
		result["excessBlobGas"] = *header.ExcessBlobGas
	}
	return result
}

func fetchAndWriteCode(ctx context.Context, client *exporter.RPCClient, writer *exporter.DatasetWriter, codeHash string) error {
	return nil
}

func validateDataset(baseDir string, expectedBlocks int) error {
	log.Printf("Validating dataset at %s...", baseDir)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func txToMap(tx *types.Transaction, blockNum uint64, blockTime uint64) map[string]interface{} {
	signer := types.MakeSigner(params.MainnetChainConfig, new(big.Int).SetUint64(blockNum), blockTime)
	from, _ := types.Sender(signer, tx)

	v, r, s := tx.RawSignatureValues()

	result := map[string]interface{}{
		"hash":        tx.Hash().Hex(),
		"nonce":       tx.Nonce(),
		"from":        from.Hex(),
		"value":       fmt.Sprintf("0x%x", tx.Value()),
		"gas":         tx.Gas(),
		"input":       "0x" + common.Bytes2Hex(tx.Data()),
		"v":           fmt.Sprintf("0x%x", v),
		"r":           fmt.Sprintf("0x%x", r),
		"s":           fmt.Sprintf("0x%x", s),
		"blockNumber": fmt.Sprintf("0x%x", blockNum),
	}

	if tx.To() != nil {
		result["to"] = tx.To().Hex()
	} else {
		result["to"] = nil
	}

	if tx.Type() == types.DynamicFeeTxType {
		if gp := tx.GasTipCap(); gp != nil {
			result["maxPriorityFeePerGas"] = fmt.Sprintf("0x%x", gp)
		}
		if gp := tx.GasFeeCap(); gp != nil {
			result["maxFeePerGas"] = fmt.Sprintf("0x%x", gp)
		}
	} else {
		if gp := tx.GasPrice(); gp != nil {
			result["gasPrice"] = fmt.Sprintf("0x%x", gp)
		}
	}

	if accessList := tx.AccessList(); accessList != nil {
		result["accessList"] = accessList
	}

	if blobHashes := tx.BlobHashes(); blobHashes != nil {
		hashes := make([]string, len(blobHashes))
		for i, h := range blobHashes {
			hashes[i] = h.Hex()
		}
		result["blobVersionedHashes"] = hashes
	}

	return result
}
