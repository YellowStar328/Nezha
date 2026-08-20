package main

import (
	"Nezha/core"
	"Nezha/ethereum/go-ethereum/accounts/abi"
	"Nezha/ethereum/go-ethereum/common"
	ecore "Nezha/ethereum/go-ethereum/core"
	"Nezha/evm/levm"
	"Nezha/evm/levm/tools"
	"Nezha/graph"
	"Nezha/utils"
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chinuy/zipf"
	"github.com/panjf2000/ants"
	"github.com/syndtr/goleveldb/leveldb"
)

const dbFile1 = "DAG_CG"
const dbFile2 = "DAG_ACG"
const dbFile3 = "DAG_Serial"
const dbFile4 = "DAG_Sim"
const dbFile5 = "DAG_Con"
const dbFile6 = "Eth_Test"
const dbFile7 = "DAG_Depurge"       // 为Depurge算法预留的数据库
const dbFile8 = "DAG_NezhaVariable" // 为 Nezha_variable 算法预留的数据库
const dbFile9 = "DAG_Vegeta"        // 为 Vegeta 算法预留的数据库
const fileName = "Exp_results.txt"

// parseBlockRange parses a block number or range string.
//
// Accepted formats:
//
//	"24000000"       → from=24000000, to=24000000 (single block)
//	"24000000-24000009" → from=24000000, to=24000009 (range)
//
// Returns an error if the string is empty, contains more than one '-',
// or if fromBlock > toBlock.
func parseBlockRange(s string) (fromBlock, toBlock uint64, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, fmt.Errorf("empty block number")
	}
	parts := strings.SplitN(s, "-", 2)
	from, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid from-block %q: %w", parts[0], err)
	}
	if len(parts) == 1 {
		return from, from, nil
	}
	to, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid to-block %q: %w", parts[1], err)
	}
	if from > to {
		return 0, 0, fmt.Errorf("from-block %d > to-block %d", from, to)
	}
	return from, to, nil
}

func main() {
	var addrNum uint64
	var txNum int
	var skew float64
	var blksize int
	var con int
	var testMode bool
	var all bool
	var serial bool
	var Nezha bool
	var NezhaVariable bool
	var CG bool
	var Depurge bool
	var Vegeta bool
	var benchmark bool
	flag.Uint64Var(&addrNum, "a", 10000, "specify address number to use. defaults to 10000.")
	flag.IntVar(&txNum, "t", 200, "specify transaction number to use. defaults to 100.")
	flag.Float64Var(&skew, "s", 0.6, "specify skew to use. defaults to 0.6.")
	flag.IntVar(&blksize, "b", 200, "specify block size to use. defaults to 200.")
	flag.IntVar(&con, "c", 4, "specify block size to use. defaults to 4.")
	flag.BoolVar(&testMode, "test", false, "specify test mode to use. defaults to false.")
	flag.BoolVar(&all, "all", false, "specify all mode to use. defaults to true.")
	flag.BoolVar(&serial, "serial", false, "specify serial mode to use. defaults to false.")
	flag.BoolVar(&Nezha, "Nezha", false, "specify Nezha mode to use. defaults to false.")
	flag.BoolVar(&NezhaVariable, "NezhaVariable", false, "specify NezhaVariable mode to use. defaults to false.")
	flag.BoolVar(&CG, "CG", false, "specify CG mode to use. defaults to false.")
	flag.BoolVar(&Depurge, "Depurge", false, "specify Depurge mode mode to use. defaults to false.")
	flag.BoolVar(&Vegeta, "Vegeta", false, "specify Vegeta mode to use. defaults to false.")
	flag.BoolVar(&benchmark, "benchmark", false, "specify benchmark mode to use. defaults to false.")

	// Replay mode flags (mainnet block replay via eth-replayd).
	var replaydURL string
	var datasetDir string
	var blockNumStr string
	var replayDepurge bool
	var replayVegeta bool
	var txsPerBlock int
	flag.StringVar(&replaydURL, "replayd", "", "eth-replayd HTTP endpoint (enables replay mode)")
	flag.StringVar(&datasetDir, "dataset", "", "Path to mainnet dataset directory (replay mode)")
	flag.StringVar(&blockNumStr, "block-num", "24000000", "Block number or range a-b to replay (replay mode)")
	flag.BoolVar(&replayDepurge, "replay-depurge", false, "Run pure-levm Depurge on mainnet block (no HTTP, uses LLM static analysis + LevmSpecFallback)")
	flag.BoolVar(&replayVegeta, "replay-vegeta", false, "Run pure-levm Vegeta on mainnet block (no HTTP, uses EVM PreExecute + LevmSpecFallback)")
	flag.IntVar(&txsPerBlock, "txs-per-block", 0, "Split the tx stream into blocks of this many txs and run Depurge_schedule / DAG construction + validation per block (simulates one algorithm run per real block). Bounds per-run scheduling complexity; 0 = all txs in one block (current behaviour). Block-splitting control overhead is measured separately and NOT counted in algorithm time (replay-depurge / replay-vegeta)")
	var stateLatency time.Duration
	flag.DurationVar(&stateLatency, "state-latency", 0, "Simulated trie cold-read latency per (addr,slot) first touch (e.g. 500us); 0 disables (replay-depurge / replay-vegeta)")
	var diskCommit bool
	flag.BoolVar(&diskCommit, "disk-commit", false, "Back serial baseline / serial replay with real on-disk leveldb + end-of-block trie commit (IntermediateRoot+Commit+leveldb flush); simulates real node commit cost (replay-depurge / replay-vegeta)")
	var trieDisk bool
	flag.BoolVar(&trieDisk, "disk-trie", false, "Encode the witness as a REAL Merkle Patricia Trie on a shared on-disk leveldb and read state through fresh (empty-cache) StateDBs: real trie traversal + genuine cold node loads from disk, no simulated latency (replay-depurge / replay-vegeta)")
	var trieCacheMB int
	flag.IntVar(&trieCacheMB, "trie-cache", 512, "Shared trie node cache size in MB for -disk-trie mode. All workers share one trie.Database, so a node loaded by any worker is warm for the others (mirrors a full node's shared trie cache). 0 disables the cache (old per-worker cold-cache semantics) (replay-depurge / replay-vegeta)")
	flag.Parse()

	// Parse block-num: either "N" (single block) or "A-B" (range).
	// For a range, the witness is loaded from A (baseline); txs are loaded
	// from all blocks [A, B] and scheduled together as one combined set.
	fromBlock, toBlock, err := parseBlockRange(blockNumStr)
	if err != nil {
		log.Fatalf("invalid -block-num %q: %v", blockNumStr, err)
	}

	// Replay-depurge / replay-vegeta: pure-levm path (no HTTP, no replayd).
	// Both flags can be specified together to run both algorithms in one
	// process, sharing the same output file (results appended in order,
	// mirroring test.go's all-in-one TestDepurge + TestVegeta pattern).
	if replayDepurge || replayVegeta {
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("open output file: %v", err)
		}
		defer file.Close()
		w := bufio.NewWriter(file)
		defer w.Flush()

		w.WriteString(fmt.Sprintf("Replay started at: %s\n", time.Now().Format(time.RFC3339)))
		w.WriteString(fmt.Sprintf("===================================================\n"))
		w.Flush()

		// Depurge runs first (uses LLM cache, read-only); Vegeta runs second
		// (pure EVM PreExecute). Each opens its own LevmSpecFallbackPool +
		// serial baseline levm with defer Close(), so no state leaks between
		// the two runs — only the in-process witness baseline is re-injected.
		if replayDepurge {
			w.WriteString(fmt.Sprintf("\n>>> Replay Depurge <<<\n"))
			w.Flush()
			if err := runReplayDepurgeMode(w, datasetDir, fromBlock, toBlock, stateLatency, diskCommit, trieDisk, trieCacheMB, txsPerBlock); err != nil {
				log.Fatalf("replay depurge mode: %v", err)
			}
		}
		if replayVegeta {
			w.WriteString(fmt.Sprintf("\n>>> Replay Vegeta <<<\n"))
			w.Flush()
			if err := runReplayVegetaMode(w, datasetDir, fromBlock, toBlock, stateLatency, diskCommit, trieDisk, trieCacheMB, txsPerBlock); err != nil {
				log.Fatalf("replay vegeta mode: %v", err)
			}
		}
		return
	}

	// Replay mode: skip synthetic test setup, go straight to mainnet replay.
	if replaydURL != "" {
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("open output file: %v", err)
		}
		defer file.Close()
		w := bufio.NewWriter(file)
		defer w.Flush()

		w.WriteString(fmt.Sprintf("Replay started at: %s\n", time.Now().Format(time.RFC3339)))
		w.WriteString(fmt.Sprintf("===================================================\n"))
		w.Flush()

		if err := runReplayMode(w, datasetDir, replaydURL, fromBlock, serial, CG, Depurge); err != nil {
			log.Fatalf("replay mode: %v", err)
		}
		return
	}

	err = utils.InitContractManager("./config/contracts.yaml")
	if err != nil {
		log.Fatalf("Failed to init contract manager: %v", err)
	}

	// 清理旧的数据库，确保每次测试从零开始
	CleanupDatabases()

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	defer file.Close()
	w := bufio.NewWriter(file)

	// 在文件开头写入当前时间
	w.WriteString(fmt.Sprintf("Test started at: %s\n", time.Now().Format(time.RFC3339)))
	w.WriteString(fmt.Sprintf("===================================================\n"))
	w.Flush()
	var txList []utils.Transaction
	// txList := utils.GenerateTransactions(addrNum, txNum, skew, 12345)
	// txList := utils.GenerateTransactions(addrNum, txNum, skew, 12345)
	if testMode {
		r := rand.New(rand.NewSource(12345))
		z := zipf.NewZipf(r, skew, addrNum)
		addr1 := z.Uint64()
		addr2 := z.Uint64()
		addr3 := z.Uint64()
		addr4 := z.Uint64()
		// 确保 addr2 != addr1
		for addr2 == addr1 {
			addr2 = z.Uint64()
		}
		for addr3 == addr1 || addr3 == addr2 {
			addr3 = z.Uint64()
		}
		for addr4 == addr3 || addr4 == addr1 || addr4 == addr2 {
			addr4 = z.Uint64()
		}

		cm := utils.GetContractManager()
		contractNames := cm.GetAllContractNames()
		defaultContract := "SmallBank"
		if len(contractNames) > 0 {
			defaultContract = contractNames[0]
		}

		txList = []utils.Transaction{
			{
				ContractName: defaultContract,
				Function:     "updateBalance",
				Addr1:        addr1,
				Addr2:        addr2,
			},
			{
				ContractName: defaultContract,
				Function:     "sendPayment",
				Addr1:        addr3,
				Addr2:        addr4,
			},
			{
				ContractName: defaultContract,
				Function:     "sendPayment",
				Addr1:        addr1,
				Addr2:        addr2,
			},
			{
				ContractName: defaultContract,
				Function:     "sendPayment",
				Addr1:        addr1,
				Addr2:        addr2,
			},
		}
	} else {
		txList = utils.GenerateTransactions(addrNum, txNum, skew, 12345)
	}
	if all {
		TestSerialExecution(txList, w)
		TestConflictQueue(txList, w, dbFile1)
		TestConflictGraph(txList, w, dbFile2)
		TestSimulation(txList, w)
		// TODO: 取消下面的注释来运行你的新算法测试
		TestDepurge(txList, w, dbFile7)
		TestNezhaVariable(txList, w, dbFile8)
		TestVegeta(txList, w, dbFile9)
	} else {
		if benchmark {
			TestSerialExecution(txList, w)
		}
		TestSimulation(txList, w)

		if Nezha {
			TestConflictQueue(txList, w, dbFile1)
		}
		if Depurge {
			TestDepurge(txList, w, dbFile7)
		}
		if NezhaVariable {
			TestNezhaVariable(txList, w, dbFile8)
		}
		if Vegeta {
			TestVegeta(txList, w, dbFile9)
		}
		if CG {
			TestConflictGraph(txList, w, dbFile2)
		}

	}
	CleanupDatabases()

}

// CleanupDatabases 删除所有旧的数据库目录，确保每次测试从零开始
func CleanupDatabases() {
	dbFiles := []string{dbFile1, dbFile2, dbFile3, dbFile4, dbFile5, dbFile6, dbFile7, dbFile8, dbFile9}
	for _, dbFile := range dbFiles {
		if err := os.RemoveAll(dbFile); err != nil {
			log.Printf("Warning: could not remove database %s: %v", dbFile, err)
		} else {
			log.Printf("Cleaned up database: %s", dbFile)
		}
	}

	dbFilesPatterns := []string{dbFile7 + "_*", dbFile8 + "_*", dbFile9 + "_*"}
	for _, pattern := range dbFilesPatterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("Warning: could not glob %s: %v", pattern, err)
			continue
		}
		for _, match := range matches {
			if err := os.RemoveAll(match); err != nil {
				log.Printf("Warning: could not remove %s: %v", match, err)
			} else {
				log.Printf("Cleaned up database: %s", match)
			}
		}
	}

	utils.ClearLLMCache()
	log.Printf("Cleaned up LLM cache")
}

// TestSimulation test concurrent transaction simulations
func TestSimulation(txList []utils.Transaction, writer *bufio.Writer) {
	txNum := len(txList)

	type evmInstanceInfo struct {
		lvm           *levm.LEVM
		fromAddr      common.Address
		contractAddrs map[string]common.Address
		abis          map[string]abi.ABI
	}
	var evmInstances []evmInstanceInfo

	for i := 0; i < txNum; i++ {
		fromAddr := tools.NewRandomAddress()
		lvm := levm.New(dbFile4, big.NewInt(0), fromAddr)
		lvm.NewAccount(fromAddr, big.NewInt(1e18))

		contractAddrs, abis := utils.DeployAllContracts(lvm, fromAddr)

		evmInstances = append(evmInstances, evmInstanceInfo{
			lvm:           lvm,
			fromAddr:      fromAddr,
			contractAddrs: contractAddrs,
			abis:          abis,
		})
	}

	//fmt.Println(runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
		n := i.(int)
		inst := evmInstances[n]
		tx := txList[n]

		contractAddr, ok := inst.contractAddrs[tx.ContractName]
		if !ok {
			fmt.Printf("Warning: contract %s not found for tx %d\n", tx.ContractName, n)
			wg.Done()
			return
		}

		abiObject, ok := inst.abis[tx.ContractName]
		if !ok {
			fmt.Printf("Warning: ABI for contract %s not found for tx %d\n", tx.ContractName, n)
			wg.Done()
			return
		}

		utils.SelectFunctions(inst.lvm, inst.fromAddr, contractAddr, abiObject, tx.ContractName, tx.Function, tx.Addr1, tx.Addr2)

		wg.Done()
	})
	defer p.Release()

	start := time.Now()

	wg.Add(1)
	go func() {
		for i := 0; i < txNum; i++ {
			wg.Add(1)
			_ = p.Invoke(i)
		}
		wg.Done()
	}()

	wg.Wait()
	duration := time.Since(start)
	writer.WriteString(fmt.Sprintf("Time of concurrently simulating transactions: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
}

// TestConflictGraph test concurrency control performance of CG
func TestConflictGraph(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {
	var al core.AlGraph
	var inValidTxs []int
	// concurrently simulate transactions to capture read/write sets
	txs, _ := utils.ConCaptureRWSetWithTransactions(txList, dbFile)
	start := time.Now()

	start1 := time.Now()
	// create conflict graph
	gSlice := core.NewBuildConflictGraph(txs)
	al.Init(gSlice)
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of constructing cg: %s\n", duration1))

	start2 := time.Now()
	// cycle detection
	johnsonDAG := graph.NewJohnsonCE(&gSlice)
	abortedNum, abortedTx := johnsonDAG.Run()
	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of finding and removing cycles: %s\n", duration2))

	for i, t := range abortedTx {
		if t == true {
			inValidTxs = append(inValidTxs, i)
		}
	}

	start3 := time.Now()
	// topological sorting
	al.RebuildGraph(inValidTxs)
	commitOrder := al.BasicTopologicalSort()
	duration3 := time.Since(start3)
	writer.WriteString(fmt.Sprintf("Time of topological sorting: %s\n", duration3))

	db := OpenDB(dbFile1)

	start4 := time.Now()
	// commit transactions
	for _, v := range commitOrder {
		for _, vv := range txs[v] {
			if vv.Label == "w" {
				acc := core.CreateAccount(vv.RWSet.Key, vv.RWSet.Value)
				err := utils.StoreState(db, acc)
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}
	duration4 := time.Since(start4)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", duration4))

	duration := time.Since(start)

	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(abortedNum)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on CG: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
}

// TestConflictQueue test concurrency control performance of ACG
func TestConflictQueue(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {
	// concurrently simulate transactions to capture read/write sets
	txs, _ := utils.ConCaptureRWSetWithTransactions(txList, dbFile)

	start := time.Now()

	start1 := time.Now()
	// create conflict graph
	queueGraph := core.CreateGraph(txs)
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of graph construction: %s\n", duration1))

	start2 := time.Now()
	// rank division
	sequence := queueGraph.QueuesSort()
	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of rank divsion: %s\n", duration2))

	start3 := time.Now()
	// sorting
	commitOrder := queueGraph.DeSS(sequence)
	duration3 := time.Since(start3)
	writer.WriteString(fmt.Sprintf("Time of DeSS: %s\n", duration3))

	var keys []int
	for seq := range commitOrder {
		keys = append(keys, int(seq))
	}
	sort.Ints(keys)

	db := OpenDB(dbFile2)

	start4 := time.Now()
	// concurrently commit transactions
	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(2000, func(i interface{}) {
		n := i.([]*core.RWNode)
		for _, rw := range n {
			acc := core.CreateAccount(rw.RWSet.Key, rw.RWSet.Value)
			err := utils.StoreState(db, acc)
			if err != nil {
				log.Panic(err)
			}
		}
		wg.Done()
	})
	defer p.Release()

	for _, n := range keys {
		for _, v := range commitOrder[int32(n)] {
			if len(v) > 0 {
				wg.Add(1)
				_ = p.Invoke(v)
			}
		}
		wg.Wait()
	}
	duration4 := time.Since(start4)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", duration4))

	duration := time.Since(start)
	count := queueGraph.GetAbortedNums()

	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(count)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Nezha: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
}

// TestSerialExecution test serial transaction processing
func TestSerialExecution(txList []utils.Transaction, writer *bufio.Writer) {
	fromAddr := tools.NewRandomAddress()
	lvm := levm.New(dbFile3, big.NewInt(0), fromAddr)

	lvm.NewAccount(fromAddr, big.NewInt(1e18))

	contractAddrs, abis := utils.DeployAllContracts(lvm, fromAddr)

	start := time.Now()

	// 使用预生成的交易序列
	for _, tx := range txList {
		contractAddr, ok := contractAddrs[tx.ContractName]
		if !ok {
			fmt.Printf("Warning: contract %s not found\n", tx.ContractName)
			continue
		}

		abiObject, ok := abis[tx.ContractName]
		if !ok {
			fmt.Printf("Warning: ABI for contract %s not found\n", tx.ContractName)
			continue
		}

		utils.SelectFunctions(lvm, fromAddr, contractAddr, abiObject, tx.ContractName, tx.Function, tx.Addr1, tx.Addr2)
	}

	stateDB := lvm.GetStateDB()
	// obtain the root hash of MPT
	root := stateDB.IntermediateRoot(false)
	stateDB.Commit(false)
	stateDB.Database().TrieDB().Commit(root, true)

	duration := time.Since(start)
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.WriteString(fmt.Sprintf("Time of serial transaction processing: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
}

func TestAppConcurrency(txNum int, blksize int, con int, addrNum uint64, skew float64) {
	avgNum := con * blksize
	loop := math.Ceil(float64(txNum / avgNum))
	count := 0
	db := OpenDB(dbFile5)
	var wg sync.WaitGroup

	runtime.GOMAXPROCS(runtime.NumCPU())

	p, _ := ants.NewPoolWithFunc(100000, func(i interface{}) {
		n := i.([]*core.RWNode)
		for _, rw := range n {
			acc := core.CreateAccount(rw.RWSet.Key, rw.RWSet.Value)
			err := utils.StoreState(db, acc)
			if err != nil {
				log.Panic(err)
			}
		}
		wg.Done()
	})
	defer p.Release()

	start := time.Now()

	for i := 0; i < int(loop); i++ {
		var exeNum int
		var keys []int

		if i == int(loop)-1 {
			exeNum = txNum - i*avgNum
		} else {
			exeNum = avgNum
		}

		txs := utils.ConCaptureRWSet(addrNum, exeNum, skew, dbFile5)
		queueGraph := core.CreateGraph(txs)
		sequence := queueGraph.QueuesSort()
		commitOrder := queueGraph.DeSS(sequence)

		for seq := range commitOrder {
			keys = append(keys, int(seq))
		}
		sort.Ints(keys)

		for _, n := range keys {
			for _, v := range commitOrder[int32(n)] {
				if len(v) > 0 {
					wg.Add(1)
					_ = p.Invoke(v)
				}
			}

			wg.Wait()
		}

		abortedNum := queueGraph.GetAbortedNums()
		count += abortedNum

		// simulate the latency of committing
		time.Sleep(100 * time.Millisecond)
	}

	duration := time.Since(start)
	fmt.Printf("Time of processing transactions: %s\n", duration)
	fmt.Printf("Abort rate is: %.3f\n", float64(count)/float64(txNum))
}

// makeCompositeKey 生成复合键 contractName:storageKey
func makeCompositeKey(contractName, storageKey string) string {
	return contractName + ":" + storageKey
}

// filterContractState 从全局 committedState 中提取指定合约的状态
func filterContractState(committedState map[string][]byte, contractName string) map[string][]byte {
	result := make(map[string][]byte)
	prefix := contractName + ":"
	for compositeKey, value := range committedState {
		if strings.HasPrefix(compositeKey, prefix) {
			storageKey := compositeKey[len(prefix):]
			result[storageKey] = value
		}
	}
	return result
}

// updateCommittedState 更新合约的 committedState（使用复合键）
func updateCommittedState(committedState map[string][]byte, contractName string, writeDelta map[string]*big.Int) {
	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	for key, delta := range writeDelta {
		compositeKey := makeCompositeKey(contractName, key)
		var currentBig *big.Int
		if currentVal, ok := committedState[compositeKey]; ok {
			currentBig = new(big.Int).SetBytes(currentVal)
		} else {
			currentBig = big.NewInt(0)
		}
		newVal := new(big.Int).Add(currentBig, delta)

		if newVal.Sign() < 0 {
			newVal = new(big.Int).Add(newVal, two256)
		}

		committedState[compositeKey] = newVal.Bytes()
	}
}

// decodeSolidityShortString 解码 Solidity 短字符串存储格式
// 短字符串（长度 <= 31 字节）存储在单个 slot 中：
// - 32 字节，最低字节存储 length*2，前 31 字节存储实际内容
func decodeSolidityShortString(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// 获取最后一个字节作为 length*2
	lengthMarker := data[len(data)-1]
	strLen := int(lengthMarker / 2)

	if strLen == 0 {
		return ""
	}

	// 提取字符串内容（前 strLen 个字节）
	if strLen > len(data)-1 {
		strLen = len(data) - 1
	}

	return string(data[:strLen])
}

// resolveAccessKey 根据 LLM 访问描述和 currentState 计算正确的存储键
func resolveAccessKeys(cm *utils.ContractManager, contractName string, access core.LLMAccess, addr1, addr2 uint64, currentState map[string][]byte) []string {
	var keys []string
	seen := make(map[string]bool)

	addKey := func(key string) {
		if key != "" && !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}

	if access.Account == "global" {
		// 直接访问全局变量，返回变量本身的键
		key, err := cm.GetGlobalVarKey(contractName, access.Field)
		if err != nil {
			return nil
		}
		addKey(core.ConvertByte2String(key))

		// 同时添加 keccak256(slot) 作为保守键（用于动态类型变量的存储访问）
		keccakSlotKey, err := cm.GetGlobalVarKeccakSlotKey(contractName, access.Field)
		if err == nil {
			addKey(core.ConvertByte2String(keccakSlotKey))
		}

		return keys
	}

	if access.Account == "addr1" || access.Account == "addr2" {
		// 用函数参数作为键访问 mapping
		var accountID uint64
		if access.Account == "addr1" {
			accountID = addr1
		} else {
			accountID = addr2
		}
		key, err := cm.GetStorageKey(contractName, access.Field, accountID)
		if err != nil {
			return nil
		}
		addKey(core.ConvertByte2String(key))
		return keys
	}

	// account 是全局变量名（如 "pool1", "pool2"）
	// 需要从 currentState 中获取该变量的值
	globalVarKey, err := cm.GetGlobalVarKey(contractName, access.Account)
	if err != nil {
		return nil
	}

	globalVarKeyStr := core.ConvertByte2String(globalVarKey)
	globalVarValue, exists := currentState[globalVarKeyStr]

	if !exists || len(globalVarValue) == 0 {
		// 全局变量没有值，使用空字符串作为键
		key, err := cm.GetStorageKeyWithValue(contractName, access.Field, "")
		if err != nil {
			return nil
		}
		addKey(core.ConvertByte2String(key))
	} else {
		// 解码 Solidity 短字符串获取实际键值
		keyValue := decodeSolidityShortString(globalVarValue)

		// 用全局变量的值作为键来访问 mapping
		key, err := cm.GetStorageKeyWithValue(contractName, access.Field, keyValue)
		if err == nil {
			addKey(core.ConvertByte2String(key))
		}

		// 同时添加空字符串作为键（保守估计：变量可能在未来变为空）
		emptyKey, err := cm.GetStorageKeyWithValue(contractName, access.Field, "")
		if err == nil {
			addKey(core.ConvertByte2String(emptyKey))
		}
	}

	// 同时添加全局变量本身的键（读取变量值）
	addKey(globalVarKeyStr)

	// 同时添加全局变量的 keccak256(slot) 键（用于动态类型变量的存储访问）
	keccakSlotKey, err := cm.GetGlobalVarKeccakSlotKey(contractName, access.Account)
	if err == nil {
		addKey(core.ConvertByte2String(keccakSlotKey))
	}

	return keys
}

// recalculateConservativeKeys 根据 LLM 原始响应和 currentState 动态重新计算保守键
func recalculateConservativeKeys(ctx *core.TransactionContext, currentState map[string][]byte) []string {
	cm := utils.GetContractManager()
	if cm == nil {
		return nil
	}

	var keys []string
	seen := make(map[string]bool)

	addKey := func(key string) {
		if key != "" && !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}

	// 处理读访问
	for _, access := range ctx.LLMReads {
		accessKeys := resolveAccessKeys(cm, ctx.ContractName, access, ctx.Addr1, ctx.Addr2, currentState)
		for _, key := range accessKeys {
			addKey(key)
		}
	}

	// 处理写访问
	for _, access := range ctx.LLMWrites {
		accessKeys := resolveAccessKeys(cm, ctx.ContractName, access, ctx.Addr1, ctx.Addr2, currentState)
		for _, key := range accessKeys {
			addKey(key)
		}
	}

	return keys
}

// TestDepurge test
func TestDepurge(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {
	utils.InitEVMPool(dbFile, runtime.NumCPU()*2)
	start := time.Now()
	start_RW := time.Now()
	cm := utils.GetContractManager()
	allFuncPairs := cm.GetAllFunctionsForPreAnalysis()
	utils.PreAnalyzeContract(allFuncPairs)
	duration_RW := time.Since(start_RW)
	// txs, contexts := utils.ConCaptureRWSetWithTransactions(txList, dbFile, true)
	txs, contexts := utils.LLMCaptureRWSet(txList, dbFile, true)

	// //测试保守读写集
	// if ctx1, ok := contexts["1"]; ok {
	// 	if ctx3, ok := contexts["3"]; ok {
	// 		// ctx1.PreReadSet = make(map[string][]byte) // 或你实际使用的类型
	// 		// ctx1.PreWriteSet = make(map[string][]byte)

	// 		for key, val := range ctx3.PreReadSet {
	// 			ctx1.PreReadSet[key] = val
	// 		}
	// 		for key, val := range ctx3.PreWriteSet {
	// 			ctx1.PreWriteSet[key] = val
	// 		}
	// 	}
	// }
	writer.WriteString(fmt.Sprintf("Time of pre-analysis: %s\n", duration_RW))
	start1 := time.Now()
	scheduler, _ := core.Depurge_schedule(contexts)
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of schedule: %s\n", duration1))
	start_exe := time.Now()
	commitOrder := make(map[int32][][]*core.RWNode)
	validationAborted := 0
	committedState := make(map[string][]byte)

	// 串行重放：仅 key 超出保守集的 abort 事务按 TxID 字典序重放
	var serialReplayList []string
	var serialReplayLock sync.Mutex
	var noContextCount int32
	var reexecErrorCount int32

	type validatedTransaction struct {
		txID         string
		writeDelta   map[string]*big.Int
		realRead     []string
		realWrite    []string
		conservative []string
		prunedKeys   []string
		realKeySet   map[string]bool
	}

	start2 := time.Now()

	totalPrunedKeys := 0
	var committedStateLock sync.RWMutex
	var inProgress int32

	validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
		txID := i.(string)

		ctx, exists := contexts[txID]
		if !exists {
			fmt.Printf("  TX %s: aborted (context not found)\n", txID)
			validationAborted++
			atomic.AddInt32(&noContextCount, 1)
			scheduler.Abort(txID)
			atomic.AddInt32(&inProgress, -1)
			return
		}

		committedStateLock.RLock()
		contractState := filterContractState(committedState, ctx.ContractName)
		committedStateLock.RUnlock()

		realReadKeys, realWriteKeys, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, contractState)
		if err != nil {
			fmt.Printf("  TX %s: aborted (re-execution error: %v)\n", txID, err)
			validationAborted++
			atomic.AddInt32(&reexecErrorCount, 1)
			scheduler.Abort(txID)
			atomic.AddInt32(&inProgress, -1)
			return
		}

		conservativeKeys := scheduler.GetConservativeKeys(txID)

		// 动态重新计算保守键（处理全局变量作为键的情况）
		dynamicConservativeKeys := recalculateConservativeKeys(ctx, contractState)
		if len(dynamicConservativeKeys) > 0 {
			conservativeKeys = dynamicConservativeKeys
		}

		conservativeKeySet := make(map[string]bool)
		for _, k := range conservativeKeys {
			conservativeKeySet[k] = true
		}

		realKeySet := make(map[string]bool)
		for _, k := range realReadKeys {
			realKeySet[k] = true
		}
		for _, k := range realWriteKeys {
			realKeySet[k] = true
		}

		abort := false
		for key := range realKeySet {
			if !conservativeKeySet[key] {
				abort = true
				break
			}
		}

		if abort {
			fmt.Printf("  TX %s: aborted (real keys exceed conservative keys) - function=%s, addr1=%d, addr2=%d\n",
				txID, ctx.Function, ctx.Addr1, ctx.Addr2)
			fmt.Printf("    Conservative Keys (%d):\n", len(conservativeKeys))
			for _, k := range conservativeKeys {
				fmt.Printf("      - %s\n", k)
			}
			fmt.Printf("    Real Read Keys (%d):\n", len(realReadKeys))
			for _, k := range realReadKeys {
				fmt.Printf("      - %s\n", k)
			}
			fmt.Printf("    Real Write Keys (%d):\n", len(realWriteKeys))
			for _, k := range realWriteKeys {
				fmt.Printf("      - %s\n", k)
			}
			var missingKeys []string
			for key := range realKeySet {
				if !conservativeKeySet[key] {
					missingKeys = append(missingKeys, key)
				}
			}
			fmt.Printf("    Missing Keys (real not in conservative) (%d):\n", len(missingKeys))
			for _, k := range missingKeys {
				fmt.Printf("      - %s\n", k)
			}
			fmt.Printf("    LLM Reads: %v\n", ctx.LLMReads)
			fmt.Printf("    LLM Writes: %v\n", ctx.LLMWrites)
			validationAborted++
			scheduler.Abort(txID)
			serialReplayLock.Lock()
			serialReplayList = append(serialReplayList, txID)
			serialReplayLock.Unlock()
			atomic.AddInt32(&inProgress, -1)
			return
		}

		prunedKeys := make([]string, 0)
		for _, key := range conservativeKeys {
			if !realKeySet[key] {
				prunedKeys = append(prunedKeys, key)
			}
		}
		totalPrunedKeys += len(prunedKeys)

		if len(prunedKeys) > 0 {
			allRealKeys := append(realReadKeys, realWriteKeys...)
			scheduler.Prune(txID, allRealKeys)
		}

		scheduler.Execute(txID)

		fmt.Printf("  TX %s: validated and committed - function=%s, addr1=%d, addr2=%d\n",
			txID, ctx.Function, ctx.Addr1, ctx.Addr2)

		if len(prunedKeys) > 0 {
			fmt.Printf("    TX %s: pruned %d keys - conservative:%d, real:%d\n",
				txID, len(prunedKeys), len(conservativeKeys), len(realKeySet))
		}

		committedStateLock.Lock()
		updateCommittedState(committedState, ctx.ContractName, writeDelta)
		committedStateLock.Unlock()

		for _, v := range txs {
			if len(v) > 0 && v[0].TransInfo.ID == txID {
				var wNodes []*core.RWNode
				for _, n := range v {
					if n.Label == "w" {
						wNodes = append(wNodes, n)
					}
				}
				commitOrder[0] = append(commitOrder[0], wNodes)
				break
			}
		}

		atomic.AddInt32(&inProgress, -1)
	})
	defer validatePool.Release()

	for scheduler.GetReadyQueueLen() > 0 || scheduler.GetPruneReadyQueueLen() > 0 || atomic.LoadInt32(&inProgress) > 0 {
		worked := false
		for {
			txID := scheduler.PopReady()
			if txID == "" {
				break
			}
			atomic.AddInt32(&inProgress, 1)
			_ = validatePool.Invoke(txID)
			worked = true
		}

		for {
			txID := scheduler.PopPruneReady()
			if txID == "" {
				break
			}
			atomic.AddInt32(&inProgress, 1)
			_ = validatePool.Invoke(txID)
			worked = true
		}

		if !worked {
			runtime.Gosched()
		}
	}
	duration_exe := time.Since(start_exe)
	writer.WriteString(fmt.Sprintf("Time of execution: %s\n", duration_exe))
	// 串行重放：key 超出保守集的 abort 事务按 TxID 字典序重放
	startSerial := time.Now()
	sort.Strings(serialReplayList)
	serialReplayed := 0
	for _, txID := range serialReplayList {
		ctx, exists := contexts[txID]
		if !exists {
			continue
		}
		committedStateLock.RLock()
		contractState := filterContractState(committedState, ctx.ContractName)
		committedStateLock.RUnlock()

		_, _, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, contractState)
		if err != nil {
			fmt.Printf("  TX %s: serial replay failed (re-execution error: %v)\n", txID, err)
			continue
		}

		committedStateLock.Lock()
		updateCommittedState(committedState, ctx.ContractName, writeDelta)
		committedStateLock.Unlock()
		serialReplayed++
	}
	durationSerial := time.Since(startSerial)
	writer.WriteString(fmt.Sprintf("Time of serial replay: %s\n", durationSerial))
	writer.WriteString(fmt.Sprintf("Serial replayed: %d\n", serialReplayed))

	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of validation and execution: %s\n", duration2))
	writer.WriteString(fmt.Sprintf("Total pruned keys: %d\n", totalPrunedKeys))

	var keys []int
	for seq := range commitOrder {
		keys = append(keys, int(seq))
	}
	sort.Ints(keys)

	db := OpenDB(dbFile)
	startCommit := time.Now()

	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
		n := i.([]*core.RWNode)
		for _, rw := range n {
			keyStr := core.ConvertByte2String(rw.RWSet.Key)
			if finalVal, ok := committedState[keyStr]; ok {
				acc := core.CreateAccount(rw.RWSet.Key, finalVal)
				err := utils.StoreState(db, acc)
				if err != nil {
					log.Panic(err)
				}
			}
		}
		wg.Done()
	})
	defer p.Release()

	for _, n := range keys {
		for _, v := range commitOrder[int32(n)] {
			if len(v) > 0 {
				wg.Add(1)
				_ = p.Invoke(v)
			}
		}
		wg.Wait()
	}

	durationCommit := time.Since(startCommit)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", durationCommit))

	duration := time.Since(start)

	writer.WriteString(fmt.Sprintf("Validation aborted (total): %d\n", validationAborted))
	writer.WriteString(fmt.Sprintf("  - context not found: %d\n", atomic.LoadInt32(&noContextCount)))
	writer.WriteString(fmt.Sprintf("  - re-execution error: %d\n", atomic.LoadInt32(&reexecErrorCount)))
	writer.WriteString(fmt.Sprintf("  - key exceed (serial replayed): %d\n", serialReplayed))
	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(validationAborted)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Depurge: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	utils.CloseEVMPool()
}

// TestNezhaVariable test Nezha_variable algorithm for variable read/write sets and finer-grained scheduling
func TestNezhaVariable(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {
	utils.InitEVMPool(dbFile, runtime.NumCPU())
	// concurrently simulate transactions to capture read/write sets
	txs, contexts := utils.ConCaptureRWSetWithTransactions(txList, dbFile, true)

	start := time.Now()
	// 步骤 1: 构建图
	start1 := time.Now()
	graph := core.CreateVariableGraph(txs)
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of graph construction: %s\n", duration1))

	// 步骤 2: 队列排序
	start2 := time.Now()
	sequence := graph.QueuesSort()
	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of queue sorting: %s\n", duration2))

	// 步骤 3: DeSS 排序
	start3 := time.Now()
	commitOrder := graph.DeSS(sequence)
	duration3 := time.Since(start3)
	writer.WriteString(fmt.Sprintf("Time of DeSS sorting: %s\n", duration3))

	var keys []int
	for seq := range commitOrder {
		keys = append(keys, int(seq))
	}
	sort.Ints(keys)

	// 统计中止数量，包括算法中止和验证中止
	algorithmAborted := graph.GetAbortedNums()
	validationAborted := 0

	start4 := time.Now()
	// 用于保护 validationAborted 的锁
	var abortLock sync.Mutex
	committedState := make(map[string][]byte)

	type validatedTransaction struct {
		txID       string
		writeDelta map[string]*big.Int
	}

	// 按层级顺序处理
	for _, n := range keys {
		level := int32(n)
		transactionsInLevel := commitOrder[level]
		levelState := utils.CloneWriteSet(committedState)

		// 存储当前层级验证通过的交易
		var validTransactions []validatedTransaction
		var validLock sync.Mutex
		var failedTxIDs []string

		// 当前层级的并行验证
		var validateWg sync.WaitGroup
		validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
			wNodes := i.([]*core.RWNode)
			if len(wNodes) == 0 {
				validateWg.Done()
				return
			}

			// 获取交易 ID
			txID := wNodes[0].TransInfo.ID

			// 获取预执行时记录的 context
			ctx, exists := contexts[txID]
			if !exists {
				// 没有 context，中止该交易
				// #region debug-point E:missing-context
				// utils.ReportDebugEvent("E", "test.go:536", "validation aborted because transaction context is missing", map[string]interface{}{
				// 	"level": level,
				// 	"txID":  txID,
				// })
				// #endregion
				abortLock.Lock()
				validationAborted++
				failedTxIDs = append(failedTxIDs, txID)
				abortLock.Unlock()
				validateWg.Done()
				return
			}

			// #region debug-point B:validate-entry
			// utils.ReportDebugEvent("B", "test.go:543", "starting validation for transaction", map[string]interface{}{
			// 	"level":      level,
			// 	"txID":       txID,
			// 	"function":   ctx.Function,
			// 	"readCount":  len(ctx.PreReadSet),
			// 	"writeCount": len(ctx.PreWriteSet),
			// })
			// #endregion

			// 使用新的验证逻辑：重新执行交易并对比写集
			valid, newWriteSet, err := utils.ReExecuteAndValidateTransactionWithState(ctx, dbFile, levelState)
			if err != nil || !valid {
				// reason := "validate-returned-false"
				if err != nil {
					// reason = "validate-returned-error"
				}
				// #region debug-point E:validate-failure-reason
				// utils.ReportDebugEvent("E", "test.go:560", "validation aborted after ReExecuteAndValidateTransaction", map[string]interface{}{
				// 	"level":    level,
				// 	"txID":     txID,
				// 	"function": ctx.Function,
				// 	"valid":    valid,
				// 	"reason":   reason,
				// 	"err": func() string {
				// 		if err != nil {
				// 			return err.Error()
				// 		}
				// 		return ""
				// 	}(),
				// })
				// #endregion
				// 验证失败，中止该交易
				abortLock.Lock()
				validationAborted++
				failedTxIDs = append(failedTxIDs, txID)
				abortLock.Unlock()
				validateWg.Done()
				return
			}

			// 验证通过，保存交易以便提交
			validLock.Lock()
			validTransactions = append(validTransactions, validatedTransaction{
				txID:       txID,
				writeDelta: newWriteSet,
			})
			validLock.Unlock()
			validateWg.Done()
		})

		// 提交验证任务
		for _, v := range transactionsInLevel {
			if len(v) > 0 {
				validateWg.Add(1)
				_ = validatePool.Invoke(v)
			}
		}

		// 等待当前层级所有验证完成
		validateWg.Wait()
		validatePool.Release()

		// #region debug-point D:level-summary
		// utils.ReportDebugEvent("D", "test.go:585", "finished validation for level", map[string]interface{}{
		// 	"level":             level,
		// 	"candidateTxCount":  len(transactionsInLevel),
		// 	"validTxCount":      len(validTransactions),
		// 	"failedTxCount":     len(failedTxIDs),
		// 	"failedTxIDsSample": failedTxIDs,
		// })
		// #endregion

		// 使用验证得到的增量更新逻辑合约存储，供后续层级重执行读取
		two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
		for _, tx := range validTransactions {
			for key, delta := range tx.writeDelta {
				var currentBig *big.Int
				if currentVal, ok := committedState[key]; ok {
					currentBig = new(big.Int).SetBytes(currentVal)
				} else {
					currentBig = big.NewInt(0)
				}
				newVal := new(big.Int).Add(currentBig, delta)

				if newVal.Sign() < 0 {
					newVal = new(big.Int).Add(newVal, two256)
				}

				committedState[key] = newVal.Bytes()
			}
		}
	}

	durationValidation := time.Since(start4)
	writer.WriteString(fmt.Sprintf("Time of validating transactions: %s\n", durationValidation))

	db := OpenDB(dbFile)
	startCommit := time.Now()

	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(2000, func(i interface{}) {
		n := i.([]*core.RWNode)
		for _, rw := range n {
			keyStr := core.ConvertByte2String(rw.RWSet.Key)
			if finalVal, ok := committedState[keyStr]; ok {
				acc := core.CreateAccount(rw.RWSet.Key, finalVal)
				err := utils.StoreState(db, acc)
				if err != nil {
					log.Panic(err)
				}

			}
		}
		wg.Done()
	})
	defer p.Release()

	for _, n := range keys {
		for _, v := range commitOrder[int32(n)] {
			if len(v) > 0 {
				wg.Add(1)
				_ = p.Invoke(v)
			}
		}
		wg.Wait()
	}

	durationCommit := time.Since(startCommit)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", durationCommit))

	duration := time.Since(start)
	totalAborted := algorithmAborted + validationAborted

	writer.WriteString(fmt.Sprintf("Algorithm aborted: %d, Validation aborted: %d, Total aborted: %d\n",
		algorithmAborted, validationAborted, totalAborted))
	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(totalAborted)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Nezha_variable: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	utils.CloseEVMPool()
}

// TestReplayingTx test a single transaction's replaying
func TestReplayingTx(nonce uint64, from, to *common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) (map[string]string, map[string]string, []byte, error) {
	var tx *core.EthTransaction

	// verdict if it is a contract creation tx
	if &to == nil {
		tx = core.NewContractCreation(nonce, from, amount, gasLimit, gasPrice, data)
	} else {
		tx = core.NewEthTransaction(nonce, from, to, amount, gasLimit, gasPrice, data)
	}

	lvm := levm.New(dbFile6, big.NewInt(0), tx.From())
	gasPool := new(ecore.GasPool).AddGas(uint64(1000000000))

	rMap, wMap, output, err := lvm.ReplayTransaction(*tx, gasPool)
	if err != nil {
		return nil, nil, nil, err
	}

	// commit to the database
	stateDB := lvm.GetStateDB()
	root := stateDB.IntermediateRoot(false)
	stateDB.Commit(false)
	stateDB.Database().TrieDB().Commit(root, true)

	if rMap != nil && wMap != nil {
		readSet, writeSet := utils.ProcessRWMap(rMap, wMap)
		return readSet, writeSet, output, nil
	}

	return nil, nil, output, nil
}

func OpenDB(dbFile string) *leveldb.DB {
	db, err := utils.LoadDB(dbFile)
	if err != nil {
		log.Panic(err)
	}

	return db
}

// ==============================
// Vegeta Algorithm
// ==============================

func TestVegeta(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {

	txs, contexts := utils.ConCaptureRWSetWithTransactions(txList, dbFile, true)
	utils.InitEVMPool(dbFile, runtime.NumCPU())
	start := time.Now()
	// Step 1: Build speculative RS/WS and txID→nodes mapping
	txToNodes := make(map[string][]*core.RWNode)
	speculativeRS := make(map[string]map[string]bool)
	speculativeWS := make(map[string]map[string]bool)

	for _, txNodes := range txs {
		if len(txNodes) == 0 {
			continue
		}
		txID := txNodes[0].TransInfo.ID
		txToNodes[txID] = txNodes

		rSet := make(map[string]bool)
		wSet := make(map[string]bool)
		for _, node := range txNodes {
			key := core.ConvertByte2String(node.RWSet.Key)
			if node.Label == "r" {
				rSet[key] = true
			} else if node.Label == "w" {
				wSet[key] = true
			}
		}
		speculativeRS[txID] = rSet
		speculativeWS[txID] = wSet
	}

	// Step 2: Dependency chain construction
	start1 := time.Now()
	keyToTxs := make(map[string][]string)
	for txID := range speculativeRS {
		for key := range speculativeRS[txID] {
			keyToTxs[key] = append(keyToTxs[key], txID)
		}
	}
	for txID := range speculativeWS {
		for key := range speculativeWS[txID] {
			keyToTxs[key] = append(keyToTxs[key], txID)
		}
	}

	var chains [][]string
	for _, txIDs := range keyToTxs {
		if len(txIDs) > 0 {
			seen := make(map[string]bool)
			var chain []string
			for _, id := range txIDs {
				if !seen[id] {
					seen[id] = true
					chain = append(chain, id)
				}
			}
			chains = append(chains, chain)
		}
	}

	sort.Slice(chains, func(i, j int) bool {
		return len(chains[i]) > len(chains[j])
	})

	orderedTxs := make([]string, 0, len(contexts))
	seenTxs := make(map[string]bool)
	for _, chain := range chains {
		for _, txID := range chain {
			if !seenTxs[txID] && contexts[txID] != nil {
				orderedTxs = append(orderedTxs, txID)
				seenTxs[txID] = true
			}
		}
	}
	for txID := range contexts {
		if !seenTxs[txID] {
			orderedTxs = append(orderedTxs, txID)
			seenTxs[txID] = true
		}
	}

	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of speculation (chain ordering): %s\n", duration1))

	// Step 3: Build DAG
	start2 := time.Now()
	dag := make(map[string][]string)
	for i, txID := range orderedTxs {
		var predecessors []string
		for j := 0; j < i; j++ {
			prevTxID := orderedTxs[j]
			hasConflict := false

			for key := range speculativeWS[txID] {
				if speculativeWS[prevTxID][key] {
					hasConflict = true
					break
				}
			}
			if !hasConflict {
				for key := range speculativeRS[txID] {
					if speculativeWS[prevTxID][key] {
						hasConflict = true
						break
					}
				}
			}
			if !hasConflict {
				for key := range speculativeWS[txID] {
					if speculativeRS[prevTxID][key] {
						hasConflict = true
						break
					}
				}
			}
			if hasConflict {
				predecessors = append(predecessors, prevTxID)
			}
		}
		dag[txID] = predecessors
	}
	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of DAG construction: %s\n", duration2))

	// Step 4: Replay loop (DAG-based parallel validation)
	start4 := time.Now()
	committedState := make(map[string][]byte)
	var stateLock sync.Mutex

	executed := make(map[string]bool)
	remaining := make(map[string]bool)
	for _, txID := range orderedTxs {
		remaining[txID] = true
	}

	var serialReplayList []string
	algorithmAborted := 0
	var noContextCount int32
	var reexecErrorCount int32

	for len(remaining) > 0 {
		var batch []string
		for txID := range remaining {
			ready := true
			for _, pred := range dag[txID] {
				if !executed[pred] {
					ready = false
					break
				}
			}
			if ready {
				batch = append(batch, txID)
			}
		}

		if len(batch) == 0 {
			break
		}

		sort.Strings(batch)

		batchResults := make(map[string]bool)
		batchDeltas := make(map[string]map[string]*big.Int)
		var resultsLock sync.Mutex
		var wg sync.WaitGroup

		validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
			txID := i.(string)
			ctx, exists := contexts[txID]
			if !exists {
				atomic.AddInt32(&noContextCount, 1)
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				wg.Done()
				return
			}

			stateLock.Lock()
			contractState := filterContractState(committedState, ctx.ContractName)
			stateLock.Unlock()

			realReadKeys, realWriteKeys, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, contractState)
			if err != nil {
				atomic.AddInt32(&reexecErrorCount, 1)
				resultsLock.Lock()
				batchResults[txID] = false
				resultsLock.Unlock()
				wg.Done()
				return
			}

			preRS := speculativeRS[txID]
			preWS := speculativeWS[txID]

			match := true
			for _, key := range realReadKeys {
				if !preRS[key] {
					match = false
					break
				}
			}
			if match {
				for _, key := range realWriteKeys {
					if !preWS[key] {
						match = false
						break
					}
				}
			}

			if !match {
				resultsLock.Lock()
				batchResults[txID] = false
				serialReplayList = append(serialReplayList, txID)
				resultsLock.Unlock()
				wg.Done()
				return
			}

			stateLock.Lock()
			updateCommittedState(committedState, ctx.ContractName, writeDelta)
			stateLock.Unlock()

			resultsLock.Lock()
			batchResults[txID] = true
			batchDeltas[txID] = writeDelta
			resultsLock.Unlock()
			wg.Done()
		})

		for _, txID := range batch {
			wg.Add(1)
			_ = validatePool.Invoke(txID)
		}
		wg.Wait()
		validatePool.Release()

		for _, txID := range batch {
			delete(remaining, txID)
			executed[txID] = true
		}
	}

	durationValidation := time.Since(start4)
	writer.WriteString(fmt.Sprintf("Time of replay (validation): %s\n", durationValidation))

	// Step 5: Serial replay of inconsistent transactions (按 TxID 字典序)
	start5 := time.Now()
	sort.Strings(serialReplayList)
	serialReplayed := 0
	for _, txID := range serialReplayList {
		ctx, exists := contexts[txID]
		if !exists {
			continue
		}

		contractState := filterContractState(committedState, ctx.ContractName)
		_, _, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, contractState)
		if err != nil {
			continue
		}

		updateCommittedState(committedState, ctx.ContractName, writeDelta)
		serialReplayed++
	}
	durationSerial := time.Since(start5)
	writer.WriteString(fmt.Sprintf("Time of serial replay: %s\n", durationSerial))

	// Commit to DB
	db := OpenDB(dbFile)
	startCommit := time.Now()

	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(2000, func(i interface{}) {
		n := i.([]*core.RWNode)
		for _, rw := range n {
			keyStr := core.ConvertByte2String(rw.RWSet.Key)
			if finalVal, ok := committedState[keyStr]; ok {
				acc := core.CreateAccount(rw.RWSet.Key, finalVal)
				err := utils.StoreState(db, acc)
				if err != nil {
					log.Panic(err)
				}
			}
		}
		wg.Done()
	})
	defer p.Release()

	for _, txID := range orderedTxs {
		if nodes, ok := txToNodes[txID]; ok {
			wg.Add(1)
			_ = p.Invoke(nodes)
		}
	}
	wg.Wait()

	durationCommit := time.Since(startCommit)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", durationCommit))

	duration := time.Since(start)

	writer.WriteString(fmt.Sprintf("Algorithm aborted (total): %d\n", algorithmAborted))
	writer.WriteString(fmt.Sprintf("  - context not found: %d\n", atomic.LoadInt32(&noContextCount)))
	writer.WriteString(fmt.Sprintf("  - re-execution error: %d\n", atomic.LoadInt32(&reexecErrorCount)))
	writer.WriteString(fmt.Sprintf("  - key exceed (serial replayed): %d\n", serialReplayed))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Vegeta: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	utils.CloseEVMPool()
}
