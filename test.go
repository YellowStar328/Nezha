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
const dbFile7 = "DAG_NewAlgorithm"  // 为新算法预留的数据库
const dbFile8 = "DAG_NezhaVariable" // 为 Nezha_variable 算法预留的数据库
const fileName = "Exp_results.txt"

func main() {
	var addrNum uint64
	var txNum int
	var skew float64
	var blksize int
	var con int
	var testMode bool
	flag.Uint64Var(&addrNum, "a", 10000, "specify address number to use. defaults to 10000.")
	flag.IntVar(&txNum, "t", 200, "specify transaction number to use. defaults to 100.")
	flag.Float64Var(&skew, "s", 0.6, "specify skew to use. defaults to 0.6.")
	flag.IntVar(&blksize, "b", 200, "specify block size to use. defaults to 200.")
	flag.IntVar(&con, "c", 4, "specify block size to use. defaults to 4.")
	flag.BoolVar(&testMode, "test", false, "specify test mode to use. defaults to false.")
	flag.Parse()

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
		// 确保 addr2 != addr1
		for addr2 == addr1 {
			addr2 = z.Uint64()
		}
		txList = []utils.Transaction{
			{
				Function: "updateBalance",
				Addr1:    addr2,
				Addr2:    addr1,
			},
			{
				Function: "sendPayment",
				Addr1:    addr1,
				Addr2:    addr2,
			},
			{
				Function: "sendPayment",
				Addr1:    addr1,
				Addr2:    addr2,
			},
			{
				Function: "sendPayment",
				Addr1:    addr1,
				Addr2:    addr2,
			},
		}
	} else {
		txList = utils.GenerateTransactions(addrNum, txNum, skew, 12345)
	}

	TestSerialExecution(txList, w)
	TestConflictQueue(txList, w, dbFile4)
	TestConflictGraph(txList, w, dbFile4)
	TestSimulation(txList, w)
	// TODO: 取消下面的注释来运行你的新算法测试
	// TestNewAlgorithm(txList, w, dbFile7)
	TestNezhaVariable(txList, w, dbFile8)
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
	p, _ := ants.NewPoolWithFunc(100000, func(i interface{}) {
		n := i.(int)
		lvm := evmPools[n]
		fromAddr := fromAddress[n]
		addr := cAddress[n]
		tx := txList[n]

		utils.SelectFunctions(lvm, fromAddr, addr, abiObject, tx.Function, tx.Addr1, tx.Addr2)

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
		utils.SelectFunctions(lvm, fromAddr, addr, abiObject, tx.Function, tx.Addr1, tx.Addr2)
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

// TestNewAlgorithm test your new concurrency control algorithm
func TestNewAlgorithm(txList []utils.Transaction, writer *bufio.Writer, dbFile string) {
	// concurrently simulate transactions to capture read/write sets
	txs, _ := utils.ConCaptureRWSetWithTransactions(txList, dbFile)

	start := time.Now()

	// TODO: 在这里调用你的新算法
	// 例如：
	// newAlgo := core.NewNewAlgorithmData()
	// commitOrder, abortedNum := newAlgo.ProcessTransactions(txs)

	// 示例计时（根据你的算法调整）
	start1 := time.Now()
	// ... 算法第一部分 ...
	duration1 := time.Since(start1)
	writer.WriteString(fmt.Sprintf("Time of your algorithm step 1: %s\n", duration1))

	start2 := time.Now()
	// ... 算法第二部分 ...
	duration2 := time.Since(start2)
	writer.WriteString(fmt.Sprintf("Time of your algorithm step 2: %s\n", duration2))

	// TODO: 获取提交顺序和中止数量
	// 示例：
	// commitOrder := ...
	// abortedNum := ...

	var keys []int
	// TODO: 准备提交的键
	// for seq := range commitOrder {
	// 	keys = append(keys, int(seq))
	// }
	sort.Ints(keys)

	db := OpenDB(dbFile) // TODO: 为新算法选择合适的数据库文件（如 dbFile6 等）

	start4 := time.Now()
	// commit transactions
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

	// TODO: 提交交易（根据你的 commitOrder 结构调整）
	// for _, n := range keys {
	// 	for _, v := range commitOrder[int32(n)] {
	// 		if len(v) > 0 {
	// 			wg.Add(1)
	// 			_ = p.Invoke(v)
	// 		}
	// 	}
	// 	wg.Wait()
	// }

	duration4 := time.Since(start4)
	writer.WriteString(fmt.Sprintf("Time of committing transactions: %s\n", duration4))

	duration := time.Since(start)
	// TODO: 替换为实际的中止数量
	count := 0 // newAlgo.GetAbortedNums()

	writer.WriteString(fmt.Sprintf("Abort rate is: %.3f\n", float64(count)/float64(len(txs))))
	writer.WriteString(fmt.Sprintf("Time of processing TXs on your new algorithm: %s\n", duration))
	writer.WriteString(fmt.Sprintf("===================================================\n"))
	writer.Flush()
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
		validatePool, _ := ants.NewPoolWithFunc(2000, func(i interface{}) {
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
