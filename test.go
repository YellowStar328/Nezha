package main

import (
	"Nezha/core"
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
	"sync"
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
const fileName = "Exp_results.txt"

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
	flag.BoolVar(&benchmark, "benchmark", false, "specify benchmark mode to use. defaults to false.")
	flag.Parse()

	err := utils.InitContractManager("./config/contracts.yaml")
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
		if CG {
			TestConflictGraph(txList, w, dbFile2)
		}

	}

}

// CleanupDatabases 删除所有旧的数据库目录，确保每次测试从零开始
func CleanupDatabases() {
	dbFiles := []string{dbFile1, dbFile2, dbFile3, dbFile4, dbFile5, dbFile6, dbFile7, dbFile8}
	for _, dbFile := range dbFiles {
		if err := os.RemoveAll(dbFile); err != nil {
			log.Printf("Warning: could not remove database %s: %v", dbFile, err)
		} else {
			log.Printf("Cleaned up database: %s", dbFile)
		}
	}

	dbFilesPatterns := []string{dbFile7 + "_*", dbFile8 + "_*"}
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
	var evmPools []*levm.LEVM
	var fromAddress []common.Address
	var cAddress []common.Address

	txNum := len(txList)

	abiObject, binData, err := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
		"./SmallBank/small_bank_sol_SmallBank.bin")
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < txNum; i++ {
		fromAddr := tools.NewRandomAddress()
		fromAddress = append(fromAddress, fromAddr)
		// create EVM instances
		lvm := levm.New(dbFile4, big.NewInt(0), fromAddr)
		lvm.NewAccount(fromAddr, big.NewInt(1e18))

		evmPools = append(evmPools, lvm)

		_, addr, _, err := lvm.DeployContract(fromAddr, binData)
		if err != nil {
			fmt.Println(err)
		}

		cAddress = append(cAddress, addr)
	}

	//fmt.Println(runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
		n := i.(int)
		lvm := evmPools[n]
		fromAddr := fromAddress[n]
		addr := cAddress[n]
		tx := txList[n]

		utils.SelectFunctions(lvm, fromAddr, addr, abiObject, tx.ContractName, tx.Function, tx.Addr1, tx.Addr2)

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
	abiObject, binData, err := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
		"./SmallBank/small_bank_sol_SmallBank.bin")
	if err != nil {
		fmt.Println(err)
	}
	lvm := levm.New(dbFile3, big.NewInt(0), fromAddr)

	lvm.NewAccount(fromAddr, big.NewInt(1e18))

	// deploy a contract
	_, addr, _, err := lvm.DeployContract(fromAddr, binData)
	if err != nil {
		fmt.Println(err)
	}

	start := time.Now()

	// 使用预生成的交易序列
	for _, tx := range txList {
		utils.SelectFunctions(lvm, fromAddr, addr, abiObject, tx.ContractName, tx.Function, tx.Addr1, tx.Addr2)
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

// TestDepurge test
func TestDepurge(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {

	cm := utils.GetContractManager()
	allFuncPairs := cm.GetAllFunctionsForPreAnalysis()
	utils.PreAnalyzeContract(allFuncPairs)

	// txs, contexts := utils.ConCaptureRWSetWithTransactions(txList, dbFile, true)
	txs, contexts := utils.LLMCaptureRWSet(txList, dbFile, true)

	//测试保守读写集
	if ctx1, ok := contexts["1"]; ok {
		if ctx3, ok := contexts["3"]; ok {
			// ctx1.PreReadSet = make(map[string][]byte) // 或你实际使用的类型
			// ctx1.PreWriteSet = make(map[string][]byte)

			for key, val := range ctx3.PreReadSet {
				ctx1.PreReadSet[key] = val
			}
			for key, val := range ctx3.PreWriteSet {
				ctx1.PreWriteSet[key] = val
			}
		}
	}
	utils.InitEVMPool(dbFile, runtime.NumCPU())
	start := time.Now()
	start1 := time.Now()
	scheduler, _ := core.Depurge_schedule(contexts)
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of schedule: %s\n", duration1))

	commitOrder := make(map[int32][][]*core.RWNode)
	validationAborted := 0
	committedState := make(map[string][]byte)

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

	levelIndex := int32(0)
	totalPrunedKeys := 0

	for scheduler.GetReadyQueueLen() > 0 {
		currentLevelSize := scheduler.GetReadyQueueLen()
		currentLevel := make([]string, 0, currentLevelSize)

		for i := 0; i < currentLevelSize; i++ {
			txID := scheduler.PopReady()
			if txID == "" {
				break
			}
			currentLevel = append(currentLevel, txID)
		}

		if len(currentLevel) == 0 {
			continue
		}

		fmt.Printf("\nLevel %d:  \n", levelIndex)
		levelState := utils.CloneWriteSet(committedState)

		var validTransactions []validatedTransaction
		var validLock sync.Mutex
		var abortLock sync.Mutex

		var validateWg sync.WaitGroup
		validatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
			txID := i.(string)

			ctx, exists := contexts[txID]
			if !exists {
				fmt.Printf("  TX %s: aborted (context not found)\n", txID)
				abortLock.Lock()
				validationAborted++
				abortLock.Unlock()
				scheduler.Abort(txID)
				validateWg.Done()
				return
			}

			realReadKeys, realWriteKeys, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, levelState)
			if err != nil {
				fmt.Printf("  TX %s: aborted (re-execution error: %v)\n", txID, err)
				abortLock.Lock()
				validationAborted++
				abortLock.Unlock()
				scheduler.Abort(txID)
				validateWg.Done()
				return
			}

			conservativeKeys := scheduler.GetConservativeKeys(txID)
			// fmt.Printf("  TX %s: conservative keys=%v, real read keys=%v, real write keys=%v\n",
			// 	txID, conservativeKeys, realReadKeys, realWriteKeys)
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
				abortLock.Lock()
				validationAborted++
				abortLock.Unlock()
				scheduler.Abort(txID)
				validateWg.Done()
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

			validLock.Lock()
			validTransactions = append(validTransactions, validatedTransaction{
				txID:         txID,
				writeDelta:   writeDelta,
				realRead:     realReadKeys,
				realWrite:    realWriteKeys,
				conservative: conservativeKeys,
				prunedKeys:   prunedKeys,
				realKeySet:   realKeySet,
			})
			validLock.Unlock()

			validateWg.Done()
		})

		for _, txID := range currentLevel {
			validateWg.Add(1)
			_ = validatePool.Invoke(txID)
		}

		validateWg.Wait()
		validatePool.Release()

		if len(validTransactions) > 0 {
			validTxIDs := make([]string, 0, len(validTransactions))
			for _, vt := range validTransactions {
				validTxIDs = append(validTxIDs, vt.txID)
			}
			fmt.Printf("\n %d transactions committed - %v\n", len(validTxIDs), validTxIDs)
		} else {
			fmt.Printf("\n 0 transactions committed\n")
		}

		for _, validTx := range validTransactions {
			if len(validTx.prunedKeys) > 0 {
				fmt.Printf("  TX %s: pruned %d keys (%v) - conservative:%d, real:%d\n",
					validTx.txID,
					len(validTx.prunedKeys),
					validTx.prunedKeys,
					len(validTx.conservative),
					len(validTx.realKeySet))
			}
		}

		for scheduler.GetPruneReadyQueueLen() > 0 {
			pruneReleasedCount := scheduler.GetPruneReadyQueueLen()
			pruneReleasedTxs := make([]string, 0, pruneReleasedCount)
			for i := 0; i < pruneReleasedCount; i++ {
				txID := scheduler.PopPruneReady()
				if txID != "" {
					pruneReleasedTxs = append(pruneReleasedTxs, txID)
				}
			}
			fmt.Printf("  %d transactions released by pruning: %v\n", pruneReleasedCount, pruneReleasedTxs)

			var pruneValidTransactions []validatedTransaction
			var pruneValidLock sync.Mutex
			var pruneAbortLock sync.Mutex

			var pruneValidateWg sync.WaitGroup
			pruneValidatePool, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(i interface{}) {
				txID := i.(string)

				ctx, exists := contexts[txID]
				if !exists {
					fmt.Printf("  TX %s: aborted (context not found)\n", txID)
					pruneAbortLock.Lock()
					validationAborted++
					pruneAbortLock.Unlock()
					scheduler.Abort(txID)
					pruneValidateWg.Done()
					return
				}

				realReadKeys, realWriteKeys, writeDelta, err := utils.ReExecuteAndGetRealRWSet(ctx, dbFile, levelState)
				if err != nil {
					fmt.Printf("  TX %s: aborted (re-execution error: %v)\n", txID, err)
					pruneAbortLock.Lock()
					validationAborted++
					pruneAbortLock.Unlock()
					scheduler.Abort(txID)
					pruneValidateWg.Done()
					return
				}

				conservativeKeys := scheduler.GetConservativeKeys(txID)
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
					pruneAbortLock.Lock()
					validationAborted++
					pruneAbortLock.Unlock()
					scheduler.Abort(txID)
					pruneValidateWg.Done()
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

				pruneValidLock.Lock()
				pruneValidTransactions = append(pruneValidTransactions, validatedTransaction{
					txID:         txID,
					writeDelta:   writeDelta,
					realRead:     realReadKeys,
					realWrite:    realWriteKeys,
					conservative: conservativeKeys,
					prunedKeys:   prunedKeys,
				})
				pruneValidLock.Unlock()

				pruneValidateWg.Done()
			})

			for _, txID := range pruneReleasedTxs {
				pruneValidateWg.Add(1)
				_ = pruneValidatePool.Invoke(txID)
			}

			pruneValidateWg.Wait()
			pruneValidatePool.Release()

			for _, validTx := range pruneValidTransactions {
				if len(validTx.prunedKeys) > 0 {
					fmt.Printf("  TX %s: pruned %d keys (%v) - conservative:%d, real:%d\n",
						validTx.txID,
						len(validTx.prunedKeys),
						validTx.prunedKeys,
						len(validTx.conservative),
						len(validTx.realRead)+len(validTx.realWrite))
				}
			}

			validTransactions = append(validTransactions, pruneValidTransactions...)
		}

		for _, validTx := range validTransactions {
			for _, v := range txs {
				if len(v) > 0 && v[0].TransInfo.ID == validTx.txID {
					var wNodes []*core.RWNode
					for _, n := range v {
						if n.Label == "w" {
							wNodes = append(wNodes, n)
						}
					}
					commitOrder[levelIndex] = append(commitOrder[levelIndex], wNodes)
					break
				}
			}
		}

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

		levelIndex++
	}

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

	writer.WriteString(fmt.Sprintf("Validation aborted: %d\n", validationAborted))
	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(validationAborted)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on Depurge: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()

	utils.CloseEVMPool()
}

// TestNezhaVariable test Nezha_variable algorithm for variable read/write sets and finer-grained scheduling
func TestNezhaVariable(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {

	// concurrently simulate transactions to capture read/write sets
	txs, contexts := utils.ConCaptureRWSetWithTransactions(txList, dbFile, true)

	utils.InitEVMPool(dbFile, runtime.NumCPU())
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
