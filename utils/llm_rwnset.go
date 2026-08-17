package utils

import (
	"Nezha/core"
	"Nezha/evm/levm"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
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

type CrossContractCall struct {
	Contract string `json:"contract"`
	Function string `json:"function"`
}

type LLMResponse struct {
	Reads      []LLMFieldAccess    `json:"reads"`
	Writes     []LLMFieldAccess    `json:"writes"`
	CrossCalls []CrossContractCall `json:"crossCalls,omitempty"`
}

type LLMConfig struct {
	APIEndpoint string
	APIKey      string
	MaxRetries  int
	Timeout     time.Duration
	Concurrency int
}

var llmConfig = LLMConfig{
	APIEndpoint: "https://developer.amd.com.cn/radeon/api/v1/chat/completions",
	APIKey:      "rc-e8fe1d82122e04c4fd1d6de8dd79ab2a9d9f6a441565df73",
	MaxRetries:  4,
	Timeout:     180 * time.Second,
	Concurrency: 1, // AMD Radeon free tier: 20 RPM, $1/day. Stay well below the RPM cap.
}

func SetLLMConfig(config LLMConfig) {
	llmConfig = config
}

var llmCache sync.Map
var llmCacheDir = "./cache/llm"

func init() {
	if err := os.MkdirAll(llmCacheDir, 0755); err != nil {
		fmt.Printf("Warning: failed to create LLM cache directory %s: %v\n", llmCacheDir, err)
	}
}

func ClearLLMCache() {
	llmCache = sync.Map{}
}

func getCacheFilePath(contractName, functionName string) string {
	return fmt.Sprintf("%s/%s_%s.json", llmCacheDir, contractName, functionName)
}

func saveLLMCacheToFile(contractName, functionName string, resp *LLMResponse) error {
	filePath := getCacheFilePath(contractName, functionName)
	data, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filePath, data, 0644)
}

func loadLLMCacheFromFile(contractName, functionName string) (*LLMResponse, error) {
	filePath := getCacheFilePath(contractName, functionName)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var resp LLMResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

var ErrNotPreAnalyzed = fmt.Errorf("function not pre-analyzed")

// ErrUnresolvableAccount signals that the LLM cache contains an access whose
// account is a global-variable-name-as-key (e.g. {"account":"owner",...}),
// which requires the runtime value of that variable to compute the concrete
// slot key. Static analysis cannot resolve it; the caller should fall back to
// EVM PreExecute for this tx.
var ErrUnresolvableAccount = fmt.Errorf("unresolvable account (global-var-as-key) requires EVM fallback")

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
	globalVars := ""
	for _, mapping := range contractConfig.StorageLayout {
		if mapping.KeyType == "simple" {
			fieldOptions += fmt.Sprintf("- \"%s\" - 全局状态变量\n", mapping.MappingName)
			globalVars += fmt.Sprintf("- \"%s\"\n", mapping.MappingName)
		} else {
			fieldOptions += fmt.Sprintf("- \"%s\" - 对应 %s\n", strings.TrimSuffix(mapping.MappingName, "Store"), mapping.MappingName)
		}
	}

	availableContracts := ""
	allContracts := cm.GetAllContracts()
	for _, c := range allContracts {
		funcNames := ""
		for i, f := range c.Functions {
			if i > 0 {
				funcNames += ", "
			}
			funcNames += f.Name
		}
		availableContracts += fmt.Sprintf("- %s: %s\n", c.Name, funcNames)
	}

	prompt := fmt.Sprintf(`分析以下 %s 合约中函数 "%s" 的保守读写集。直接返回JSON格式，不要任何分析文字。

合约代码：
%s

参数映射规则：
%s

字段选项：
%s

全局变量列表（key_type为simple的字段）：
%s

可用合约与函数列表（用于识别跨合约调用）：
%s

重要规则：
1. 直接访问全局变量（如读取pool1的值）：{"account": "global", "field": "pool1"}
2. 用全局变量的值作为键访问mapping（如poolLiquidity[pool1]）：{"account": "pool1", "field": "poolLiquidity"}
3. 用函数参数作为键访问mapping（如poolLiquidity[addr1]）：{"account": "addr1", "field": "poolLiquidity"}
4. account可以是：addr1、addr2（函数参数）、global（直接访问全局变量）、或某个全局变量名（用该变量的值作为键）
5. 保守规则：对于if/else等条件语句，必须包含两个分支中所有可能的存储访问。例如，如果else分支中使用了pool2，则必须包含对pool2的读取和写入（如果有）
6. 对于mapping访问，即使条件分支可能不执行，也要保守地包含所有可能的mapping访问
7. 全局变量读写一致性：任何出现在writes中的全局变量（account为"global"的字段），必须同时出现在reads中。因为写入状态变量通常需要先读取其当前值（如x = x + 1），所以必须同时记录该变量的读取
8. 跨合约调用：如果合约代码中调用了上述"可用合约与函数列表"中的其他合约函数（如调用 USDT 合约的 transfer 函数），必须在 crossCalls 数组中列出。每个 crossCall 包含 contract（合约名）和 function（函数名）

返回示例：
{"reads":[{"account":"addr1","field":"userBalances"},{"account":"global","field":"pool1"},{"account":"global","field":"pool2"},{"account":"global","field":"totalSupply"},{"account":"pool1","field":"poolLiquidity"},{"account":"pool2","field":"poolLiquidity"}],"writes":[{"account":"addr1","field":"userBalances"},{"account":"global","field":"totalSupply"},{"account":"pool1","field":"poolLiquidity"},{"account":"pool2","field":"poolLiquidity"}],"crossCalls":[{"contract":"USDT","function":"transfer"}]}`, contractName, functionName, sourceCode, argMappingStr, fieldOptions, globalVars, availableContracts)

	return prompt
}

// backoffFor returns a retry delay for the given 0-based retry attempt.
// Rate-limit (429) responses use a longer schedule so we stay under the
// provider's per-minute request budget (e.g. AMD Radeon free: 20 RPM).
func backoffFor(retry int, isRateLimit bool) time.Duration {
	base := 3 * time.Second
	if isRateLimit {
		base = 10 * time.Second
	}
	d := base
	for i := 0; i < retry; i++ {
		d *= 2
		if d > 2*time.Minute {
			return 2 * time.Minute
		}
	}
	return d
}

func callLLM(prompt string) (*LLMResponse, error) {
	reqBody := map[string]interface{}{
		"model": "DeepSeek-V4-Flash",
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"temperature": 0.0,
		"max_tokens":  5000,
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
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM request failed: %w", err)
		}

		respBody, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM response read failed: %w", err)
		}

		// Dashscope compatible-mode v1 sometimes returns transient 404 / 5xx
		// under load; 429 needs backoff. Treat all of them as retryable.
		// 408 (timeout) and 425 (too early) are also transient.
		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode >= 500 ||
			resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusRequestTimeout ||
			resp.StatusCode == 425 {
			time.Sleep(backoffFor(retry, resp.StatusCode == http.StatusTooManyRequests))
			continue
		}

		// Move response parsing inside the retry loop. Sometimes the LLM returns
		// malformed JSON or leaves content empty while writing the actual JSON
		// into reasoning_content; retrying usually fixes it.
		if resp.StatusCode != http.StatusOK {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM request failed with status %d: %s", resp.StatusCode, truncateBytes(respBody, 400))
		}

		var result struct {
			Choices []struct {
				Message struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if jerr := json.Unmarshal(respBody, &result); jerr != nil {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM response envelope: %w, raw: %s", jerr, truncateBytes(respBody, 500))
		}
		if len(result.Choices) == 0 {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM returned no choices")
		}

		content := strings.TrimSpace(result.Choices[0].Message.Content)
		// deepseek-v4-flash often returns reasoning as JSON in reasoning_content
		// while leaving content as "", so fall back before erroring.
		if content == "" {
			content = strings.TrimSpace(result.Choices[0].Message.ReasoningContent)
		}
		if content == "" {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("LLM returned empty content and empty reasoning_content")
		}

		// Strip markdown fences ```json ... ``` with any trailing text.
		content = stripMarkdownJSONFence(content)

		var llmResp LLMResponse
		if jerr := json.Unmarshal([]byte(content), &llmResp); jerr != nil {
			if retry+1 < llmConfig.MaxRetries {
				time.Sleep(backoffFor(retry, false))
				continue
			}
			return nil, fmt.Errorf("failed to parse LLM response JSON: %w, content: %s", jerr, truncateStr(content, 800))
		}

		return &llmResp, nil
	}

	return nil, fmt.Errorf("LLM request failed after %d retries", llmConfig.MaxRetries)
}

// truncateStr returns s truncated to at most n runes, appending "..." if truncated.
func truncateStr(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// truncateBytes truncates raw bytes for safe logging (avoid invalid UTF in error).
func truncateBytes(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// stripMarkdownJSONFence removes ```json ... ``` markdown wrappers, including
// any leading text before the fence (e.g. deepseek reasoning preamble) and
// any trailing text after the closing fence.
func stripMarkdownJSONFence(content string) string {
	orig := content
	content = strings.TrimSpace(content)

	// Case 1: explicit ```json ... ``` block
	if idx := strings.Index(content, "```json"); idx >= 0 {
		rest := content[idx+len("```json"):]
		if end := strings.Index(rest, "```"); end >= 0 {
			content = strings.TrimSpace(rest[:end])
		} else {
			content = strings.TrimSpace(rest)
		}
		return content
	}

	// Case 2: fence without "json" tag: ``` ... ```
	if idx := strings.Index(content, "```"); idx >= 0 {
		rest := content[idx+3:]
		// Sometimes the first line is "json" on its own. Skip that line.
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(strings.ToLower(rest), "json\n") ||
			strings.HasPrefix(strings.ToLower(rest), "json\r\n") {
			rest = rest[4:] // after "json"
			for len(rest) > 0 && (rest[0] == '\n' || rest[0] == '\r') {
				rest = rest[1:]
			}
		}
		if end := strings.Index(rest, "```"); end >= 0 {
			content = strings.TrimSpace(rest[:end])
		} else {
			content = strings.TrimSpace(rest)
		}
		return content
	}

	// Case 3: no fence, but content has leading text before a JSON object
	// LLM reasoning models sometimes say "好的我返回如下JSON..." then print
	// the JSON. Find first '{' and last '}' and return that slice.
	start := strings.Index(content, "{")
	end := strings.LastIndex(content, "}")
	if start >= 0 && end > start {
		return content[start : end+1]
	}

	return orig
}

func PreAnalyzeContract(pairs []ContractFunctionPair) error {
	for _, pair := range pairs {
		cacheKey := fmt.Sprintf("%s:%s", pair.ContractName, pair.FunctionName)

		if _, ok := llmCache.Load(cacheKey); ok {
			fmt.Printf("Function %s:%s already analyzed, skipping\n", pair.ContractName, pair.FunctionName)
			continue
		}

		if cachedResp, err := loadLLMCacheFromFile(pair.ContractName, pair.FunctionName); err == nil {
			llmCache.Store(cacheKey, cachedResp)
			fmt.Printf("Function %s:%s loaded from file cache\n", pair.ContractName, pair.FunctionName)
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
		if err := saveLLMCacheToFile(pair.ContractName, pair.FunctionName, resp); err != nil {
			fmt.Printf("Warning: failed to save cache to file for %s:%s: %v\n", pair.ContractName, pair.FunctionName, err)
		}
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

	if cachedResp, err := loadLLMCacheFromFile(tx.ContractName, tx.Function); err == nil {
		llmCache.Store(cacheKey, cachedResp)
		return cachedResp, nil
	}

	return nil, ErrNotPreAnalyzed
}

func llmResponseToRWSet(contractName string, resp *LLMResponse, addr1, addr2 uint64) ([][]byte, [][]byte, [][]byte, [][]byte) {
	var rAddr, rValue, wAddr, wValue [][]byte

	cm := GetContractManager()
	if cm == nil {
		return rAddr, rValue, wAddr, wValue
	}

	globalVarNames := cm.GetGlobalVarNames(contractName)
	globalVarSet := make(map[string]bool)
	for _, name := range globalVarNames {
		globalVarSet[name] = true
	}

	processAccess := func(access LLMFieldAccess) (key []byte, err error) {
		var mappingName string

		if access.Account == "global" {
			mappingName = access.Field
			return cm.GetStorageKey(contractName, mappingName, 0)
		}

		if access.Account == "addr1" || access.Account == "addr2" {
			var accountID uint64
			if access.Account == "addr1" {
				accountID = addr1
			} else {
				accountID = addr2
			}
			mappingName = access.Field
			return cm.GetStorageKey(contractName, mappingName, accountID)
		}

		// account 是全局变量名（如 "pool1", "pool2"）
		if globalVarSet[access.Account] {
			mappingName = access.Field
			// 预分析阶段没有 currentState，使用 accountID=0 作为占位符
			// 验证阶段会动态重新计算
			return cm.GetStorageKey(contractName, mappingName, 0)
		}

		// 默认情况
		mappingName = access.Field
		return cm.GetStorageKey(contractName, mappingName, 0)
	}

	for _, access := range resp.Reads {
		key, err := processAccess(access)
		if err != nil {
			fmt.Printf("Warning: failed to get storage key for %s:%s: %v\n", contractName, access.Field, err)
			continue
		}
		rAddr = append(rAddr, key)
		rValue = append(rValue, big.NewInt(0).Bytes())
	}

	for _, access := range resp.Writes {
		key, err := processAccess(access)
		if err != nil {
			fmt.Printf("Warning: failed to get storage key for %s:%s: %v\n", contractName, access.Field, err)
			continue
		}
		wAddr = append(wAddr, key)
		wValue = append(wValue, big.NewInt(0).Bytes())
	}

	return rAddr, rValue, wAddr, wValue
}

var ErrCrossContractNotCached = fmt.Errorf("cross-contract call not found in LLM cache")

// mergeCrossContractRWSet checks cross-contract calls in the LLM response and merges
// cached RW sets from those contracts. Returns extra raw keys to append to the main RW set.
// If any cross-contract call is not cached, returns ErrCrossContractNotCached with details.
func mergeCrossContractRWSet(
	resp *LLMResponse,
	addr1, addr2 uint64,
) (extraRAddr, extraRValue, extraWAddr, extraWValue [][]byte, err error) {
	if len(resp.CrossCalls) == 0 {
		return nil, nil, nil, nil, nil
	}

	for _, cc := range resp.CrossCalls {
		cacheKey := fmt.Sprintf("%s:%s", cc.Contract, cc.Function)

		var subResp *LLMResponse
		if cached, ok := llmCache.Load(cacheKey); ok {
			subResp = cached.(*LLMResponse)
		} else if loaded, loadErr := loadLLMCacheFromFile(cc.Contract, cc.Function); loadErr == nil {
			llmCache.Store(cacheKey, loaded)
			subResp = loaded
		} else {
			return nil, nil, nil, nil, fmt.Errorf(
				"%w: %s.%s", ErrCrossContractNotCached, cc.Contract, cc.Function)
		}

		subR, subRV, subW, subWV := llmResponseToRWSet(cc.Contract, subResp, addr1, addr2)
		extraRAddr = append(extraRAddr, subR...)
		extraRValue = append(extraRValue, subRV...)
		extraWAddr = append(extraWAddr, subW...)
		extraWValue = append(extraWValue, subWV...)
		fmt.Printf("  [LLM CROSS] Merged %s.%s: %d reads, %d writes\n",
			cc.Contract, cc.Function, len(subR), len(subW))
	}

	return extraRAddr, extraRValue, extraWAddr, extraWValue, nil
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

			contractAddrs, abis := DeployAllContracts(lvm, fromAddr)
			addr, ok := contractAddrs[tx.ContractName]
			if !ok {
				fmt.Printf("Contract %s not deployed\n", tx.ContractName)
				wg.Done()
				return
			}
			abiObject := abis[tx.ContractName]

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

			rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), tx.ContractName, rAddr, rValue, wAddr, wValue)

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

		extraR, extraRV, extraW, extraWV, crossErr := mergeCrossContractRWSet(llmResp, tx.Addr1, tx.Addr2)
		if crossErr != nil {
			fmt.Printf("Cross-contract RWSet not cached (%v), falling back to EVM for tx %d\n", crossErr, n)

			fromAddr := tools.NewRandomAddress()
			lvm := levm.New(dbFile, big.NewInt(0), fromAddr)
			lvm.NewAccount(fromAddr, big.NewInt(1e18))
			defer lvm.Close()

			contractAddrs, abis := DeployAllContracts(lvm, fromAddr)
			addr, ok := contractAddrs[tx.ContractName]
			if !ok {
				fmt.Printf("Contract %s not deployed\n", tx.ContractName)
				wg.Done()
				return
			}
			abiObject := abis[tx.ContractName]

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

			rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), tx.ContractName, rAddr, rValue, wAddr, wValue)

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

		rAddr = append(rAddr, extraR...)
		rValue = append(rValue, extraRV...)
		wAddr = append(wAddr, extraW...)
		wValue = append(wValue, extraWV...)

		rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), tx.ContractName, rAddr, rValue, wAddr, wValue)

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

			for _, r := range llmResp.Reads {
				ctx.LLMReads = append(ctx.LLMReads, core.LLMAccess{
					Account: r.Account,
					Field:   r.Field,
				})
			}
			for _, w := range llmResp.Writes {
				ctx.LLMWrites = append(ctx.LLMWrites, core.LLMAccess{
					Account: w.Account,
					Field:   w.Field,
				})
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
