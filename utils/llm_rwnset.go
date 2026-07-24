package utils

import (
	"Nezha/core"
	"Nezha/evm/levm"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"Nezha/evm/levm/tools"

	"github.com/panjf2000/ants"
)

type LLMRequest struct {
	Function string `json:"function"`
	Addr1    uint64 `json:"addr1"`
	Addr2    uint64 `json:"addr2"`
}

type LLMFieldAccess struct {
	Account string `json:"account"`
	Field   string `json:"field"`
}

type LLMResponse struct {
	Reads  []LLMFieldAccess `json:"reads"`
	Writes []LLMFieldAccess `json:"writes"`
}

type LLMConfig struct {
	APIEndpoint string
	APIKey      string
	MaxRetries  int
	Timeout     time.Duration
	Concurrency int
}

var llmConfig = LLMConfig{
	APIEndpoint: "https://api.deepseek.com/chat/completions",
	APIKey:      "sk-e788e33be40844c5a56c74bcda30cd95",
	MaxRetries:  3,
	Timeout:     30 * time.Second,
	Concurrency: 5,
}

func SetLLMConfig(config LLMConfig) {
	llmConfig = config
}

var llmCache sync.Map

func ClearLLMCache() {
	llmCache = sync.Map{}
}

var ErrNotPreAnalyzed = fmt.Errorf("function not pre-analyzed")

func buildLLMPrompt(contractName, functionName string) string {
	cm := GetContractManager()
	if cm == nil {
		return ""
	}

	sourceCode, err := cm.GetSourceCode(contractName)
	if err != nil {
		fmt.Printf("Warning: failed to get source code for %s: %v\n", contractName, err)
		return ""
	}

	funcDef, ok := cm.GetFunction(contractName, functionName)
	if !ok {
		fmt.Printf("Warning: function %s not found in contract %s\n", functionName, contractName)
		return ""
	}

	argMappingStr := fmt.Sprintf("- %s:", functionName)
	first := true
	for arg, addr := range funcDef.ArgMapping {
		if !first {
			argMappingStr += ", "
		}
		argMappingStr += fmt.Sprintf(" %s=%s", arg, addr)
		first = false
	}

	contractConfig, _ := cm.GetContractConfig(contractName)
	fieldOptions := ""
	for _, mapping := range contractConfig.StorageLayout {
		fieldOptions += fmt.Sprintf("- \"%s\" - 对应 %s\n", strings.TrimSuffix(mapping.MappingName, "Store"), mapping.MappingName)
	}

	prompt := fmt.Sprintf(`你是一个智能合约分析专家。请分析以下 %s 合约中函数 "%s" 的保守读写集。

合约代码：
%s

参数映射规则：
%s

请返回保守的读写集（包含所有可能访问的存储位置，即使在某些条件下可能不被访问）。

返回格式要求（JSON格式，只返回保守读写集JSON）：
{
  "reads": [
    {"account": "addr1", "field": "checking"},
    {"account": "addr2", "field": "saving"}
  ],
  "writes": [
    {"account": "addr1", "field": "checking"}
  ]
}

字段选项：
%s

注意：
1. account 只能是 "addr1" 或 "addr2"
2. 保守分析意味着包含所有可能被访问的存储位置
3. 不要遗漏任何可能的分支路径
4. 只返回JSON，不要包含其他文字`, contractName, functionName, sourceCode, argMappingStr, fieldOptions)

	return prompt
}

func callLLM(prompt string) (*LLMResponse, error) {
	reqBody := map[string]interface{}{
		"model": "deepseek-v4-flash",
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"temperature": 0.0,
		"max_tokens":  500,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: llmConfig.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
			MaxIdleConnsPerHost: 5,
		},
	}

	req, err := http.NewRequest("POST", llmConfig.APIEndpoint, strings.NewReader(string(reqBytes)))
	if err != nil {
		return nil, err
	}

	if llmConfig.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+llmConfig.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")

	var resp *http.Response
	var respBody []byte

	for retry := 0; retry < llmConfig.MaxRetries; retry++ {
		resp, err = client.Do(req)
		if err != nil {
			time.Sleep(time.Duration(retry+1) * 2 * time.Second)
			continue
		}

		respBody, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			time.Sleep(time.Duration(retry+1) * 2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			time.Sleep(time.Duration(retry+1) * 2 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("LLM request failed with status %d: %s", resp.StatusCode, string(respBody))
		}

		break
	}

	if err != nil {
		return nil, err
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("LLM returned no choices")
	}

	content := strings.TrimSpace(result.Choices[0].Message.Content)
	content = strings.TrimPrefix(content, "```json")
	content = strings.TrimSuffix(content, "```")
	content = strings.TrimSpace(content)

	var llmResp LLMResponse
	if err := json.Unmarshal([]byte(content), &llmResp); err != nil {
		return nil, fmt.Errorf("failed to parse LLM response: %v, content: %s", err, content)
	}

	fmt.Printf("LLM parsed response: reads=%+v, writes=%+v\n", llmResp.Reads, llmResp.Writes)

	return &llmResp, nil
}

func PreAnalyzeContract(pairs []ContractFunctionPair) error {
	for _, pair := range pairs {
		cacheKey := fmt.Sprintf("%s:%s", pair.ContractName, pair.FunctionName)

		if _, ok := llmCache.Load(cacheKey); ok {
			fmt.Printf("Function %s:%s already analyzed, skipping\n", pair.ContractName, pair.FunctionName)
			continue
		}

		fmt.Printf("Pre-analyzing function: %s:%s\n", pair.ContractName, pair.FunctionName)
		prompt := buildLLMPrompt(pair.ContractName, pair.FunctionName)
		if prompt == "" {
			fmt.Printf("Warning: failed to build prompt for %s:%s\n", pair.ContractName, pair.FunctionName)
			continue
		}

		resp, err := callLLM(prompt)
		if err != nil {
			fmt.Printf("Pre-analysis failed for %s:%s: %v\n", pair.ContractName, pair.FunctionName, err)
			return err
		}

		llmCache.Store(cacheKey, resp)
		fmt.Printf("Pre-analysis completed for %s:%s: reads=%d, writes=%d\n", pair.ContractName, pair.FunctionName, len(resp.Reads), len(resp.Writes))
	}

	fmt.Println("Pre-analysis of all functions completed")
	return nil
}

func analyzeTransactionLLM(tx Transaction) (*LLMResponse, error) {
	cacheKey := fmt.Sprintf("%s:%s", tx.ContractName, tx.Function)

	if cached, ok := llmCache.Load(cacheKey); ok {
		return cached.(*LLMResponse), nil
	}

	return nil, ErrNotPreAnalyzed
}

func llmResponseToRWSet(contractName string, resp *LLMResponse, addr1, addr2 uint64) ([][]byte, [][]byte, [][]byte, [][]byte) {
	var rAddr, rValue, wAddr, wValue [][]byte

	cm := GetContractManager()
	if cm == nil {
		return rAddr, rValue, wAddr, wValue
	}

	for _, access := range resp.Reads {
		var accountID uint64
		if access.Account == "addr1" {
			accountID = addr1
		} else {
			accountID = addr2
		}

		mappingName := access.Field + "Store"
		key, err := cm.GetStorageKey(contractName, mappingName, accountID)
		if err != nil {
			fmt.Printf("Warning: failed to get storage key for %s:%s: %v\n", contractName, mappingName, err)
			continue
		}

		rAddr = append(rAddr, key)
		rValue = append(rValue, big.NewInt(0).Bytes())
	}

	for _, access := range resp.Writes {
		var accountID uint64
		if access.Account == "addr1" {
			accountID = addr1
		} else {
			accountID = addr2
		}

		mappingName := access.Field + "Store"
		key, err := cm.GetStorageKey(contractName, mappingName, accountID)
		if err != nil {
			fmt.Printf("Warning: failed to get storage key for %s:%s: %v\n", contractName, mappingName, err)
			continue
		}

		wAddr = append(wAddr, key)
		wValue = append(wValue, big.NewInt(0).Bytes())
	}

	return rAddr, rValue, wAddr, wValue
}

func LLMCaptureRWSet(txList []Transaction, dbFile string, captureContext ...bool) ([][]*core.RWNode, map[string]*core.TransactionContext) {
	var txs [][]*core.RWNode
	txNum := len(txList)

	shouldCapture := len(captureContext) > 0 && captureContext[0]
	var contexts map[string]*core.TransactionContext
	if shouldCapture {
		contexts = make(map[string]*core.TransactionContext)
	}

	var wg sync.WaitGroup
	var lock sync.Mutex

	p, _ := ants.NewPoolWithFunc(llmConfig.Concurrency, func(i interface{}) {
		n := i.(int)
		tx := txList[n]

		llmResp, err := analyzeTransactionLLM(tx)
		if err != nil {
			fmt.Printf("LLM analysis failed for tx %d, falling back to EVM execution: %v\n", n, err)

			fromAddr := tools.NewRandomAddress()
			lvm := levm.New(dbFile, big.NewInt(0), fromAddr)
			lvm.NewAccount(fromAddr, big.NewInt(1e18))
			defer lvm.Close()

			cm := GetContractManager()
			if cm == nil {
				fmt.Println("ContractManager not initialized")
				wg.Done()
				return
			}

			contractConfig, ok := cm.GetContractConfig(tx.ContractName)
			if !ok {
				fmt.Printf("Contract %s not found\n", tx.ContractName)
				wg.Done()
				return
			}

			abiObject, binData, loadErr := tools.LoadContract(contractConfig.ABIPath, contractConfig.BinPath)
			if loadErr != nil {
				fmt.Println(loadErr)
				wg.Done()
				return
			}

			_, addr, _, deployErr := lvm.DeployContract(fromAddr, binData)
			if deployErr != nil {
				fmt.Println(deployErr)
				wg.Done()
				return
			}

			rMap, wMap := SelectFunctions2(lvm, fromAddr, addr, abiObject, tx.ContractName, tx.Function, tx.Addr1, tx.Addr2)

			var rAddr, rValue, wAddr, wValue [][]byte
			for key := range rMap {
				rAddr = append(rAddr, key.Bytes())
				rValue = append(rValue, rMap[key].Bytes())
			}
			for key := range wMap {
				wAddr = append(wAddr, key.Bytes())
				wValue = append(wValue, wMap[key].Bytes())
			}

			rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), rAddr, rValue, wAddr, wValue)

			lock.Lock()
			txs = append(txs, rwNodes)
			if shouldCapture {
				ctx := core.RWNodesToContext(
					strconv.FormatInt(int64(n), 10),
					tx.ContractName,
					tx.Function,
					tx.Addr1,
					tx.Addr2,
					rwNodes,
					fromAddr,
					addr,
				)
				contexts[ctx.TxID] = ctx
			}
			lock.Unlock()
			wg.Done()
			return
		}

		rAddr, rValue, wAddr, wValue := llmResponseToRWSet(tx.ContractName, llmResp, tx.Addr1, tx.Addr2)

		rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), rAddr, rValue, wAddr, wValue)

		lock.Lock()
		txs = append(txs, rwNodes)

		if shouldCapture {
			ctx := &core.TransactionContext{
				TxID:         strconv.FormatInt(int64(n), 10),
				ContractName: tx.ContractName,
				Function:     tx.Function,
				Addr1:        tx.Addr1,
				Addr2:        tx.Addr2,
				PreReadSet:   make(map[string][]byte),
				PreWriteSet:  make(map[string][]byte),
				FromAddr:     tools.NewRandomAddress(),
			}

			for i := range rAddr {
				keyStr := core.ConvertByte2String(rAddr[i])
				ctx.PreReadSet[keyStr] = rValue[i]
			}
			for i := range wAddr {
				keyStr := core.ConvertByte2String(wAddr[i])
				ctx.PreWriteSet[keyStr] = wValue[i]
			}

			contexts[ctx.TxID] = ctx
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

	sortedTxs := make([][]*core.RWNode, txNum)
	for _, rwNode := range txs {
		if len(rwNode) > 0 {
			txID, _ := strconv.Atoi(rwNode[0].TransInfo.ID)
			sortedTxs[txID] = rwNode
		}
	}

	return sortedTxs, contexts
}
