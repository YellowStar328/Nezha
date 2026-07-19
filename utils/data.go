package utils

import (
	"Nezha/core"
	"Nezha/ethereum/go-ethereum/accounts/abi"
	"Nezha/ethereum/go-ethereum/common"
	"Nezha/ethereum/go-ethereum/core/vm"
	"Nezha/evm/levm"
	"Nezha/evm/levm/tools"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chinuy/zipf"
	"github.com/panjf2000/ants"
	"github.com/syndtr/goleveldb/leveldb"
)

// Transaction 表示一个预生成的交易
type Transaction struct {
	Function string // 函数名
	Addr1    uint64 // 地址1
	Addr2    uint64 // 地址2
}

type debugEvent struct {
	SessionID    string                 `json:"sessionId"`
	RunID        string                 `json:"runId"`
	HypothesisID string                 `json:"hypothesisId"`
	Location     string                 `json:"location,omitempty"`
	Msg          string                 `json:"msg"`
	Data         map[string]interface{} `json:"data,omitempty"`
	Ts           int64                  `json:"ts"`
}

func ReportDebugEvent(hypothesisID, location, msg string, data map[string]interface{}) {
	url := "http://127.0.0.1:7777/event"
	sessionID := "nezha-validation-abort"

	if envContent, err := os.ReadFile(".dbg/nezha-validation-abort.env"); err == nil {
		for _, line := range strings.Split(string(envContent), "\n") {
			if strings.HasPrefix(line, "DEBUG_SERVER_URL=") {
				url = strings.TrimPrefix(line, "DEBUG_SERVER_URL=")
			}
			if strings.HasPrefix(line, "DEBUG_SESSION_ID=") {
				sessionID = strings.TrimPrefix(line, "DEBUG_SESSION_ID=")
			}
		}
	}

	body, err := json.Marshal(debugEvent{
		SessionID:    sessionID,
		RunID:        "pre-fix",
		HypothesisID: hypothesisID,
		Location:     location,
		Msg:          "[DEBUG] " + msg,
		Data:         data,
		Ts:           time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// GenerateTransactions 预生成确定的交易序列
func GenerateTransactions(addrNum uint64, txNum int, skew float64, seed int64) []Transaction {
	var txs []Transaction

	// 使用固定的随机种子确保结果可重复
	r := rand.New(rand.NewSource(seed))
	z := zipf.NewZipf(r, skew, addrNum)

	selectFunc := []string{"almagate", "updateBalance", "updateSaving", "sendPayment", "writeCheck", "getBalance"}

	for i := 0; i < txNum; i++ {
		// 5% 只读交易，95% 写交易（与原 Nezha 一致）
		random := r.Float32()
		var function string
		if random <= 0.05 {
			function = selectFunc[5] // getBalance
		} else {
			random2 := r.Intn(5)
			function = selectFunc[random2]
		}

		// 生成地址
		addr1 := z.Uint64()
		addr2 := z.Uint64()
		// 确保 addr2 != addr1
		for addr2 == addr1 {
			addr2 = z.Uint64()
		}

		txs = append(txs, Transaction{
			Function: function,
			Addr1:    addr1,
			Addr2:    addr2,
		})
	}

	return txs
}

func txCollector(addrNum uint64, txNum int, skew float64) [][]*core.RWNode {
	var txs [][]*core.RWNode

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := zipf.NewZipf(r, skew, addrNum)
	//z := rand.NewZipf(r, skew, 1, addrNum)

	for i := 0; i < txNum; i++ {
		rAddr1 := z.Uint64()
		rAddr2 := z.Uint64()
		wAddr1 := z.Uint64()
		wAddr2 := z.Uint64()

		tx := core.CreateRWNode(strconv.FormatInt(int64(i), 10), uint32(i), [][]byte{[]byte(strconv.FormatUint(rAddr1, 10)),
			[]byte(strconv.FormatUint(rAddr2, 10))}, [][]byte{[]byte("1"), []byte("2")},
			[][]byte{[]byte(strconv.FormatUint(wAddr1, 10)), []byte(strconv.FormatUint(wAddr2, 10))},
			[][]byte{[]byte("1"), []byte("2")})
		txs = append(txs, tx)
	}

	return txs
}

// CaptureRWSet capture read/write set in a single thread
func CaptureRWSet(addrNum uint64, txNum int, skew float64, dbFile string) [][]*core.RWNode {
	var txs [][]*core.RWNode

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := zipf.NewZipf(r, skew, addrNum)

	selectFunc := []string{"almagate", "updateBalance", "updateSaving", "sendPayment", "writeCheck", "getBalance"}

	abiObject, binData, err := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
		"./SmallBank/small_bank_sol_SmallBank.bin")
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < txNum; i++ {
		fromAddr := tools.NewRandomAddress()

		lvm := levm.New(dbFile, big.NewInt(0), fromAddr)
		lvm.NewAccount(fromAddr, big.NewInt(1e18))

		_, addr, _, err := lvm.DeployContract(fromAddr, binData)
		if err != nil {
			fmt.Println(err)
		}

		rand.Seed(time.Now().UnixNano())
		// random := rand.Intn(5)
		random := rand.Float32()

		// read-write 50-50
		var function string
		if random <= 0.05 {
			function = selectFunc[5]
		} else {
			random2 := rand.Intn(5)
			function = selectFunc[random2]
		}

		addr1 := z.Uint64()
		addr2 := z.Uint64()
		for {
			if addr2 != addr1 {
				break
			}
			addr2 = z.Uint64()
		}

		rMap, wMap := SelectFunctions2(lvm, fromAddr, addr, abiObject, function, addr1, addr2)

		// generate r/w set
		var rAddr [][]byte
		var rValue [][]byte
		var wAddr [][]byte
		var wValue [][]byte

		for key := range rMap {
			s := key.Bytes()
			v := rMap[key].Bytes()
			rAddr = append(rAddr, s)
			rValue = append(rValue, v)

			//s1 := ConvertByte2String(s)
			//v1 := ConvertByte2String(v)
			//fmt.Printf("T_%d, Read/value: %s%s\n", i, s1, v1)
		}

		for key := range wMap {
			s := key.Bytes()
			v := wMap[key].Bytes()
			wAddr = append(wAddr, s)
			wValue = append(wValue, v)

			//s1 := ConvertByte2String(s)
			//v1 := ConvertByte2String(v)
			//fmt.Printf("T_%d, Write/value: %s%s\n", i, s1, v1)
		}

		rwNodes := core.CreateRWNode(strconv.FormatInt(int64(i), 10), uint32(i), rAddr, rValue, wAddr, wValue)
		txs = append(txs, rwNodes)
	}

	return txs
}

// ConCaptureRWSet capture read/write set in multiple threads
func ConCaptureRWSet(addrNum uint64, txNum int, skew float64, dbFile string) [][]*core.RWNode {
	// 使用固定种子生成交易
	txList := GenerateTransactions(addrNum, txNum, skew, 12345)
	rwNodes, _ := ConCaptureRWSetWithTransactions(txList, dbFile)
	return rwNodes
}

// ConCaptureRWSetWithTransactions capture read/write set using pre-generated transactions
// captureContext: 可选参数，是否记录交易上下文用于后续验证
func ConCaptureRWSetWithTransactions(
	txList []Transaction,
	dbFile string,
	captureContext ...bool,
) (
	rwNodes [][]*core.RWNode,
	contexts map[string]*core.TransactionContext,
) {
	var evmPools []*levm.LEVM
	var fromAddress []common.Address
	var cAddress []common.Address
	var txs [][]*core.RWNode

	txNum := len(txList)

	// 根据参数决定是否初始化 contexts
	shouldCapture := len(captureContext) > 0 && captureContext[0]
	if shouldCapture {
		contexts = make(map[string]*core.TransactionContext)
	}

	abiObject, binData, err := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
		"./SmallBank/small_bank_sol_SmallBank.bin")
	if err != nil {
		fmt.Println(err)
	}
	//随机地址充值1e8 wei
	for i := 0; i < txNum; i++ {
		fromAddr := tools.NewRandomAddress()
		fromAddress = append(fromAddress, fromAddr)
		// create EVM instances
		lvm := levm.New(dbFile, big.NewInt(0), fromAddr)
		lvm.NewAccount(fromAddr, big.NewInt(1e18))

		evmPools = append(evmPools, lvm)

		_, addr, _, err := lvm.DeployContract(fromAddr, binData)
		if err != nil {
			fmt.Println(err)
		}
		cAddress = append(cAddress, addr)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup
	var lock sync.Mutex
	//var rw = make(chan []*core.RWNode, txNum)

	p, _ := ants.NewPoolWithFunc(100000, func(i interface{}) {
		n := i.(int)
		lvm := evmPools[n]
		fromAddr := fromAddress[n]
		addr := cAddress[n]
		tx := txList[n]

		rMap, wMap := SelectFunctions2(lvm, fromAddr, addr, abiObject, tx.Function, tx.Addr1, tx.Addr2)

		// generate r/w set
		var rAddr [][]byte
		var rValue [][]byte
		var wAddr [][]byte
		var wValue [][]byte

		for key := range rMap {
			s := key.Bytes()
			v := rMap[key].Bytes()
			rAddr = append(rAddr, s)
			rValue = append(rValue, v)
		}

		for key := range wMap {
			s := key.Bytes()
			v := wMap[key].Bytes()
			wAddr = append(wAddr, s)
			wValue = append(wValue, v)
		}

		rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), rAddr, rValue, wAddr, wValue)

		lock.Lock()
		txs = append(txs, rwNodes)

		// 仅当需要时才记录 context
		if shouldCapture {
			ctx := core.RWNodesToContext(
				strconv.FormatInt(int64(n), 10),
				tx.Function,
				tx.Addr1,
				tx.Addr2,
				rwNodes,
				fromAddr,
				addr,
			)
			contexts[ctx.TxID] = ctx
			// #region debug-point C:capture-context
			ReportDebugEvent("C", "utils/data.go:305", "captured pre-execution context", map[string]interface{}{
				"txID":         ctx.TxID,
				"function":     ctx.Function,
				"addr1":        ctx.Addr1,
				"addr2":        ctx.Addr2,
				"readCount":    len(ctx.PreReadSet),
				"writeCount":   len(ctx.PreWriteSet),
				"contractAddr": ctx.ContractAddr.Hex(),
			})
			// #endregion
		}
		lock.Unlock()

		wg.Done()
	})
	defer p.Release()

	for i := 0; i < txNum; i++ {
		wg.Add(1)
		_ = p.Invoke(i)
	}

	wg.Wait()

	for _, lvm := range evmPools {
		if lvm != nil {
			_ = lvm.Close()
		}
	}

	// 按照交易顺序重新排序（因为并发执行可能导致顺序混乱）
	sortedTxs := make([][]*core.RWNode, txNum)
	for _, rwNode := range txs {
		if len(rwNode) > 0 {
			txID, _ := strconv.Atoi(rwNode[0].TransInfo.ID)
			sortedTxs[txID] = rwNode
		}
	}

	return sortedTxs, contexts
}

func SelectFunctions(lvm *levm.LEVM, fromAddr common.Address, cAddr common.Address, abiObject abi.ABI, funcName string,
	addr1 uint64, addr2 uint64) {
	switch funcName {
	case "almagate":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "almagate",
			strconv.FormatUint(addr1, 10), strconv.FormatUint(addr2, 10))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	case "getBalance":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "getBalance",
			strconv.FormatUint(addr1, 10))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	case "updateBalance":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "updateBalance",
			strconv.FormatUint(addr2, 10), big.NewInt(100))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	case "updateSaving":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "updateSaving",
			strconv.FormatUint(addr1, 10), big.NewInt(100))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	case "sendPayment":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "sendPayment",
			strconv.FormatUint(addr1, 10), strconv.FormatUint(addr2, 10), big.NewInt(50))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	case "writeCheck":
		_, err := lvm.CallContractABI(fromAddr, cAddr, big.NewInt(0), abiObject, "writeCheck",
			strconv.FormatUint(addr2, 10), big.NewInt(20))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		//fmt.Println("get output:", getOutput)
	default:
		fmt.Println("Invalid inputs")
	}
}

func SelectFunctions2(lvm *levm.LEVM, fromAddr common.Address, cAddr common.Address, abiObject abi.ABI, funcName string,
	addr1 uint64, addr2 uint64) (vm.Storage, vm.Storage) {
	switch funcName {
	case "almagate":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "almagate",
			strconv.FormatUint(addr1, 10), strconv.FormatUint(addr2, 10))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	case "getBalance":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "getBalance",
			strconv.FormatUint(addr1, 10))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	case "updateBalance":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "updateBalance",
			strconv.FormatUint(addr2, 10), big.NewInt(100))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	case "updateSaving":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "updateSaving",
			strconv.FormatUint(addr1, 10), big.NewInt(100))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	case "sendPayment":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "sendPayment",
			strconv.FormatUint(addr1, 10), strconv.FormatUint(addr2, 10), big.NewInt(50))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	case "writeCheck":
		rMap, wMap, _, err := lvm.CallContractABI2(fromAddr, cAddr, big.NewInt(0), abiObject, "writeCheck",
			strconv.FormatUint(addr2, 10), big.NewInt(20))
		if err != nil {
			fmt.Println("get error : ", err)
		}
		return rMap, wMap
	default:
		fmt.Println("Invalid inputs")
		return nil, nil
	}
}

func ProcessRWMap(rMap, wMap vm.Storage) (map[string]string, map[string]string) {
	var readSet = make(map[string]string)
	var writeSet = make(map[string]string)

	for key := range rMap {
		s := key.Bytes()
		v := rMap[key].Bytes()
		readSet[string(s)] = string(v)
	}

	for key := range wMap {
		s := key.Bytes()
		v := wMap[key].Bytes()
		writeSet[string(s)] = string(v)
	}

	return readSet, writeSet
}

// ValidateAndExecuteTransactionWithDB validates that the pre-executed read set
// still matches the current committed database state using a shared DB handle.
func ValidateAndExecuteTransactionWithDB(
	ctx *core.TransactionContext,
	db *leveldb.DB,
) (bool, error) {
	for key, preValue := range ctx.PreReadSet {
		addr, err := hex.DecodeString(key)
		if err != nil {
			// #region debug-point C:decode-failure
			ReportDebugEvent("C", "utils/data.go:472", "failed to decode pre-read-set key", map[string]interface{}{
				"txID": ctx.TxID,
				"key":  key,
				"err":  err.Error(),
			})
			// #endregion
			return false, err
		}

		currentValue, err := FetchStateValue(db, addr)
		if err != nil {
			if err == leveldb.ErrNotFound && isZeroValue(preValue) {
				continue
			}
			if err == leveldb.ErrNotFound {
				// #region debug-point A:not-found-mismatch
				ReportDebugEvent("A", "utils/data.go:484", "validation failed because current value is missing", map[string]interface{}{
					"txID":           ctx.TxID,
					"function":       ctx.Function,
					"key":            key,
					"preValueHex":    hex.EncodeToString(preValue),
					"currentMissing": true,
				})
				// #endregion
				return false, nil
			}
			// #region debug-point A:fetch-error
			ReportDebugEvent("A", "utils/data.go:493", "validation failed while reading current database value", map[string]interface{}{
				"txID":     ctx.TxID,
				"function": ctx.Function,
				"key":      key,
				"err":      err.Error(),
			})
			// #endregion
			return false, err
		}

		if !bytes.Equal(preValue, currentValue) {
			// #region debug-point A:value-mismatch
			ReportDebugEvent("A", "utils/data.go:503", "validation failed because current value differs from pre-read-set", map[string]interface{}{
				"txID":            ctx.TxID,
				"function":        ctx.Function,
				"addr1":           ctx.Addr1,
				"addr2":           ctx.Addr2,
				"key":             key,
				"preValueHex":     hex.EncodeToString(preValue),
				"currentValueHex": hex.EncodeToString(currentValue),
			})
			// #endregion
			return false, nil
		}
	}

	return true, nil
}

// ValidateAndExecuteTransaction validates that the pre-executed read set still
// matches the current committed database state.
func ValidateAndExecuteTransaction(
	ctx *core.TransactionContext,
	dbFile string,
) (bool, error) {
	db, err := LoadDB(dbFile)
	if err != nil {
		return false, err
	}
	defer db.Close()

	return ValidateAndExecuteTransactionWithDB(ctx, db)
}

func isZeroValue(value []byte) bool {
	if len(value) == 0 {
		return true
	}
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

// WriteSetEqual 比较两个写集是否完全一致
func WriteSetEqual(set1, set2 map[string][]byte) bool {
	if len(set1) != len(set2) {
		return false
	}
	for key, val1 := range set1 {
		val2, exists := set2[key]
		if !exists {
			return false
		}
		if !bytes.Equal(val1, val2) {
			return false
		}
	}
	return true
}

// WriteDeltaEqual 比较两个增量写集是否完全一致
func WriteDeltaEqual(set1, set2 map[string]*big.Int) bool {
	if len(set1) != len(set2) {
		return false
	}
	for key, delta1 := range set1 {
		delta2, exists := set2[key]
		if !exists {
			return false
		}
		if delta1.Cmp(delta2) != 0 {
			return false
		}
	}
	return true
}

// CloneWriteSet 深拷贝写集，避免不同层级之间共享底层切片。
func CloneWriteSet(set map[string][]byte) map[string][]byte {
	cloned := make(map[string][]byte, len(set))
	for key, value := range set {
		cloned[key] = append([]byte(nil), value...)
	}
	return cloned
}

func applyLogicalStateToContract(
	lvm *levm.LEVM,
	contractAddr common.Address,
	logicalState map[string][]byte,
) error {
	stateDB := lvm.GetStateDB()
	for keyHex, value := range logicalState {
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			return err
		}
		stateDB.SetState(contractAddr, common.BytesToHash(keyBytes), common.BytesToHash(value))
	}
	return nil
}

// ReExecuteAndValidateTransactionWithState 基于当前逻辑状态重新执行交易，验证增量是否一致。
func ReExecuteAndValidateTransactionWithState(
	ctx *core.TransactionContext,
	dbFile string,
	logicalState map[string][]byte,
) (bool, map[string]*big.Int, error) {
	// 加载合约 ABI 和二进制代码
	abiObject, binData, err := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
		"./SmallBank/small_bank_sol_SmallBank.bin")
	if err != nil {
		return false, nil, err
	}

	// 为每个交易创建独立的 EVM 实例，并用层间已提交的逻辑状态初始化合约存储
	lvm := levm.New(dbFile, big.NewInt(0), ctx.FromAddr)
	defer lvm.Close()

	// 为发送者账户充值（确保有足够的 Gas）
	lvm.NewAccount(ctx.FromAddr, big.NewInt(1e18))

	// 重新部署合约，得到本次重执行实际使用的合约地址
	_, contractAddr, _, err := lvm.DeployContract(ctx.FromAddr, binData)
	if err != nil {
		return false, nil, err
	}

	if err := applyLogicalStateToContract(lvm, contractAddr, logicalState); err != nil {
		return false, nil, err
	}

	// 重新创建 EVM 以刷新 tracer，避免部署和状态注入污染本次交易的读写集
	lvm.NewEVM(big.NewInt(0), ctx.FromAddr)

	// 重新执行交易并捕获新的写集
	newRMap, newWMap := SelectFunctions2(lvm, ctx.FromAddr, contractAddr, abiObject, ctx.Function, ctx.Addr1, ctx.Addr2)

	// 计算增量：写值 - 读值（处理 uint256 下溢）
	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	two255 := new(big.Int).Rsh(two256, 1)

	newWriteDelta := make(map[string]*big.Int)
	for key := range newWMap {
		keyStr := core.ConvertByte2String(key.Bytes())
		writeBig := new(big.Int).SetBytes(newWMap[key].Bytes())

		var readBig *big.Int
		if readVal, ok := newRMap[key]; ok {
			readBig = new(big.Int).SetBytes(readVal.Bytes())
		} else {
			if currentVal, ok := logicalState[keyStr]; ok {
				readBig = new(big.Int).SetBytes(currentVal)
			} else {
				readBig = big.NewInt(0)
			}
		}

		delta := new(big.Int).Sub(writeBig, readBig)

		if delta.Sign() < 0 {
			delta = new(big.Int).Add(delta, two256)
			if delta.Cmp(two255) >= 0 {
				delta = new(big.Int).Sub(delta, two256)
			}
		}

		newWriteDelta[keyStr] = delta
	}

	// 对比增量
	return WriteDeltaEqual(ctx.PreWriteDelta, newWriteDelta), newWriteDelta, nil
}
