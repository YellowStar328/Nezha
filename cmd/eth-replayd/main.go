package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"Nezha/cmd/eth-replayd/replayd"
)

func main() {
	var (
		datasetDir string
		fromBlock  uint64
		toBlock    uint64
		serve      bool
		listenAddr string
		conc       int
	)

	flag.StringVar(&datasetDir, "dataset", "", "Path to exported dataset directory")
	flag.Uint64Var(&fromBlock, "from", 0, "Start block number (default: from manifest)")
	flag.Uint64Var(&toBlock, "to", 0, "End block number (default: from manifest)")
	flag.BoolVar(&serve, "serve", false, "Start HTTP server mode for Nezha scheduler integration")
	flag.StringVar(&listenAddr, "listen", "127.0.0.1:8089", "HTTP listen address (--serve mode)")
	flag.IntVar(&conc, "concurrency", 0, "PreExecute concurrency (default: NumCPU)")
	flag.Parse()

	if datasetDir == "" {
		log.Fatal("Dataset directory required: use --dataset flag")
	}

	if serve {
		if err := StartHTTPServer(listenAddr, datasetDir, conc); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
		return
	}

	runCLIMode(datasetDir, fromBlock, toBlock)
}

func runCLIMode(datasetDir string, fromBlock, toBlock uint64) {
	reader, err := replayd.NewDatasetReader(datasetDir)
	if err != nil {
		log.Fatalf("Failed to open dataset: %v", err)
	}

	manifest := reader.Manifest()
	log.Printf("Dataset: blocks %d-%d, chain %d", manifest.FromBlock, manifest.ToBlock, manifest.ChainID)

	startBlock := uint64(manifest.FromBlock)
	endBlock := uint64(manifest.ToBlock)

	if fromBlock > 0 {
		startBlock = fromBlock
	}
	if toBlock > 0 {
		endBlock = toBlock
	}

	if startBlock > endBlock {
		log.Fatalf("from block (%d) must be <= to block (%d)", startBlock, endBlock)
	}

	log.Printf("Replaying blocks %d-%d", startBlock, endBlock)

	executor := replayd.NewTxExecutor()
	totalTxs := 0
	successTxs := 0
	failedTxs := 0
	startTime := time.Now()

	for bn := startBlock; bn <= endBlock; bn++ {
		blockData, err := reader.LoadBlock(bn)
		if err != nil {
			log.Printf("SKIP block %d: %v", bn, err)
			continue
		}

		blockHashWindow := make(map[uint64]common.Hash)

		result, err := executor.ExecuteBlock(blockData, blockHashWindow)
		if err != nil {
			log.Printf("ERROR block %d: %v", bn, err)
			continue
		}

		blockSuccess := 0
		blockFailed := 0
		for _, txResult := range result.TxResults {
			totalTxs++
			if txResult.Success {
				blockSuccess++
				successTxs++
			} else {
				blockFailed++
				failedTxs++
			}
		}

		log.Printf("Block %d: %d txs (%d success, %d failed), stateRoot=%s, time=%v",
			bn,
			len(result.TxResults),
			blockSuccess,
			blockFailed,
			result.StateRoot.Hex()[:18]+"...",
			time.Since(startTime).Round(time.Millisecond),
		)

		for _, txResult := range result.TxResults {
			if !txResult.Success {
				log.Printf("  FAIL tx %d: %s, error: %s", txResult.TxIndex, txResult.TxHash, txResult.Error)
			}
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("\n=== Replay Summary ===")
	log.Printf("Blocks: %d-%d", startBlock, endBlock)
	log.Printf("Total transactions: %d", totalTxs)
	log.Printf("Successful: %d", successTxs)
	log.Printf("Failed: %d", failedTxs)
	log.Printf("Time: %v", elapsed)
	if totalTxs > 0 {
		log.Printf("Avg time/tx: %v", elapsed/time.Duration(totalTxs))
	}

	if failedTxs > 0 {
		os.Exit(1)
	}
	fmt.Println("OK")
}
