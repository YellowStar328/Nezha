package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// MainnetFuncPair identifies a function to analyze by contract address + selector
type MainnetFuncPair struct {
	Address  string // lowercase 0x-prefixed contract address
	Selector string // 0x-prefixed 4-byte selector e.g. "0xa9059cbb"
}

// MainnetCrossCall describes a cross-contract call by target address + selector
type MainnetCrossCall struct {
	Contract string `json:"contract"` // target contract address (lowercase 0x-prefixed)
	Function string `json:"function"` // target function selector (0x-prefixed)
}

// MainnetLLMResponse reuses LLMResponse but crossCalls use address+selector
type MainnetLLMResponse struct {
	Reads      []LLMFieldAccess   `json:"reads"`
	Writes     []LLMFieldAccess   `json:"writes"`
	CrossCalls []MainnetCrossCall `json:"crossCalls,omitempty"`
}

// mainnetStorageLayout mirrors the solc storage-layout JSON format, parsed
// locally to avoid exporting parseSolcStorageLayout from contract_manager.go.
// The "value" field on each type entry lets us identify nested mappings
// (e.g. t_mapping(t_address,t_mapping(t_address,t_uint256)) whose value is
// itself a mapping type id).
type mainnetStorageLayout struct {
	Storage []struct {
		Slot  string `json:"slot"`
		Label string `json:"label"`
		Type  string `json:"type"`
	} `json:"storage"`
	Types map[string]struct {
		Encoding string `json:"encoding"`
		Key      string `json:"key"`
		Value    string `json:"value,omitempty"`
	} `json:"types"`
}

// mainnetABIEntry represents a single entry in an abi.json array
type mainnetABIEntry struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Inputs []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"inputs"`
}

// getMainnetCacheDir returns the base cache directory for mainnet RW-set analysis
func getMainnetCacheDir() string {
	return "./cache/mainnet_rw"
}

// getMainnetCacheFilePath returns the path to the cached LLM response for a
// given contract address + function selector: cache/mainnet_rw/<address>/<selector>.json
func getMainnetCacheFilePath(address, selector string) string {
	return filepath.Join(getMainnetCacheDir(), address, selector+".json")
}

// readContractFile reads a file from a contract's cache directory
func readContractFile(address, filename string) ([]byte, error) {
	path := filepath.Join(getMainnetCacheDir(), address, filename)
	return os.ReadFile(path)
}

// mainnetMetaInfo mirrors the subset of meta.json fields needed for alias
// resolution. The full metaInfo (see cmd/ethscan-fetcher) has more fields.
type mainnetMetaInfo struct {
	Address      string `json:"address"`
	ContractName string `json:"contract_name"`
	Verified     bool   `json:"verified"`
}

// readContractAlias returns the contract_name (alias) from a contract's
// meta.json, or "" if meta.json is missing/unreadable or has no contract_name.
func readContractAlias(address string) string {
	data, err := readContractFile(address, "meta.json")
	if err != nil {
		return ""
	}
	var meta mainnetMetaInfo
	if err := json.Unmarshal(data, &meta); err != nil {
		return ""
	}
	return meta.ContractName
}

// saveMainnetLLMCache marshals a MainnetLLMResponse to JSON and writes it to the
// cache file, creating the directory if needed.
func saveMainnetLLMCache(address, selector string, resp *MainnetLLMResponse) error {
	filePath := getMainnetCacheFilePath(address, selector)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// loadMainnetLLMCache reads and unmarshals a MainnetLLMResponse from the cache file
func loadMainnetLLMCache(address, selector string) (*MainnetLLMResponse, error) {
	filePath := getMainnetCacheFilePath(address, selector)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var resp MainnetLLMResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// readFuncsMap reads funcs.json for a contract and returns the selector→name
// mapping. Three formats are supported (tried in this order):
//   - Array of objects: [{"selector":"0x...","name":"...","inputs":[...]},...]
//     — written by cmd/ethscan-fetcher computeFuncSelectors
//     Empty array [] returns an empty map (e.g. minimal proxy with no funcs).
//   - map[string]string — legacy flat map
//   - map[string]{"name":...} — legacy object map
//
// ReadFuncsMap reads funcs.json for a contract and returns the selector→name
// mapping. Exported for diagnostics (abort logging in runReplayDepurgeMode).
func ReadFuncsMap(address string) (map[string]string, error) {
	return readFuncsMap(address)
}

func readFuncsMap(address string) (map[string]string, error) {
	data, err := readContractFile(address, "funcs.json")
	if err != nil {
		return nil, err
	}
	// Newest format (ethscan-fetcher): array of func entries
	var arr []struct {
		Selector string `json:"selector"`
		Name     string `json:"name"`
	}
	if err := json.Unmarshal(data, &arr); err == nil {
		result := make(map[string]string, len(arr))
		for _, e := range arr {
			if e.Selector == "" {
				continue
			}
			result[e.Selector] = e.Name
		}
		return result, nil // empty arr → empty map (buildMainnetPrompt will warn)
	}
	var strMap map[string]string
	if err := json.Unmarshal(data, &strMap); err == nil {
		return strMap, nil
	}
	var objMap map[string]struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &objMap); err == nil {
		result := make(map[string]string, len(objMap))
		for k, v := range objMap {
			result[k] = v.Name
		}
		return result, nil
	}
	return nil, fmt.Errorf("unsupported funcs.json format for %s", address)
}

// readABIEntries reads abi.json for a contract (expected as a JSON array)
func readABIEntries(address string) ([]mainnetABIEntry, error) {
	data, err := readContractFile(address, "abi.json")
	if err != nil {
		return nil, err
	}
	var entries []mainnetABIEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// findABIEntry returns the ABI entry matching funcName (function type only)
func findABIEntry(entries []mainnetABIEntry, name string) (mainnetABIEntry, bool) {
	for _, e := range entries {
		if e.Name == name && (e.Type == "function" || e.Type == "") {
			return e, true
		}
	}
	return mainnetABIEntry{}, false
}

// formatSignature builds "name(type1,type2,...)" from an ABI entry
func formatSignature(e mainnetABIEntry) string {
	var types []string
	for _, in := range e.Inputs {
		types = append(types, in.Type)
	}
	return fmt.Sprintf("%s(%s)", e.Name, strings.Join(types, ","))
}

// buildParamMapping maps the first two address-type parameters of funcName to
// addr1/addr2 based on ABI inputs.
func buildParamMapping(funcName string, entries []mainnetABIEntry) string {
	mappingStr := fmt.Sprintf("- %s:", funcName)
	e, ok := findABIEntry(entries, funcName)
	if !ok {
		return mappingStr
	}
	count := 0
	first := true
	for _, in := range e.Inputs {
		if in.Type != "address" {
			continue
		}
		paramName := in.Name
		if paramName == "" {
			paramName = fmt.Sprintf("arg%d", count)
		}
		if !first {
			mappingStr += ","
		}
		mappingStr += fmt.Sprintf(" %s=addr%d", paramName, count+1)
		first = false
		count++
		if count >= 2 {
			break
		}
	}
	return mappingStr
}

// buildStorageInfo parses storage.json and returns field options + global vars
// list. Returns empty strings if storage.json is unavailable.
func buildStorageInfo(address string) (fieldOptions string, globalVars string) {
	data, err := readContractFile(address, "storage.json")
	if err != nil {
		return "", ""
	}
	var layout mainnetStorageLayout
	if err := json.Unmarshal(data, &layout); err != nil {
		return "", ""
	}
	var fOptions, gVars strings.Builder
	for _, item := range layout.Storage {
		isMapping := false
		if typeInfo, ok := layout.Types[item.Type]; ok {
			if typeInfo.Encoding == "mapping" {
				isMapping = true
			}
		}
		if isMapping {
			fOptions.WriteString(fmt.Sprintf("- \"%s\" - mapping (slot %s)\n", item.Label, item.Slot))
		} else {
			fOptions.WriteString(fmt.Sprintf("- \"%s\" - simple (slot %s)\n", item.Label, item.Slot))
			gVars.WriteString(fmt.Sprintf("- \"%s\"\n", item.Label))
		}
	}
	// Cap per-section output so the aggregate prompt payload stays well under
	// reverse-proxy body-size limits (Azure App Gateway ~100 KB). The first N
	// fields still give the LLM enough pattern recognition for conservative
	// sets; for the largest contracts the storage.json alone can exceed 30 KB
	// when rendered line-by-line.
	return truncateStr(fOptions.String(), 10000), truncateStr(gVars.String(), 5000)
}

// buildAvailableContractsList scans all contract directories under the mainnet
// cache and lists each contract's functions (with signatures when ABI is
// available) for cross-contract call recognition.
func buildAvailableContractsList() string {
	entries, err := os.ReadDir(getMainnetCacheDir())
	if err != nil {
		return ""
	}
	var sb strings.Builder
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		addr := entry.Name()
		funcs, err := readFuncsMap(addr)
		if err != nil {
			continue
		}
		abiEntries, _ := readABIEntries(addr)
		var sigs []string
		for _, name := range funcs {
			sig := name
			if e, ok := findABIEntry(abiEntries, name); ok {
				sig = formatSignature(e)
			}
			sigs = append(sigs, sig)
		}
		sort.Strings(sigs)
		alias := readContractAlias(addr)
		if alias != "" {
			sb.WriteString(fmt.Sprintf("- %s (alias=%s): %s\n", addr, alias, strings.Join(sigs, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf("- %s: %s\n", addr, strings.Join(sigs, ", ")))
		}
	}
	return truncateStr(sb.String(), 3000) // keep prompt payload well under reverse-proxy body limits
}

// buildMainnetPrompt constructs the LLM prompt for a mainnet contract function,
// reading source.sol, abi.json, storage.json and funcs.json from the contract's
// cache directory and listing all other available contracts for cross-call
// recognition.
func buildMainnetPrompt(address, selector string) string {
	sourceBytes, err := readContractFile(address, "source.sol")
	if err != nil {
		fmt.Printf("Warning: failed to read source.sol for %s: %v\n", address, err)
		return ""
	}
	sourceCode := string(sourceBytes)
	// Cap source code in the prompt to keep the HTTP request body well under
	// most reverse-proxy size limits (Azure App Gateway default ~128 KB).
	// The model's context window itself is much larger (1M+ tokens), so the
	// real bottleneck is ingress, not reasoning. If an analysed function
	// lives in a truncated portion of source.sol, the LLM can still fall
	// back to storage/ABI information in the prompt to produce a
	// conservative set; correctness is not degraded.
	sourceCode = truncateStr(sourceCode, 40000)

	funcs, err := readFuncsMap(address)
	if err != nil {
		fmt.Printf("Warning: failed to read funcs.json for %s: %v\n", address, err)
		return ""
	}
	funcName, ok := funcs[selector]
	if !ok {
		fmt.Printf("Warning: selector %s not found in funcs.json for %s\n", selector, address)
		return ""
	}

	abiEntries, _ := readABIEntries(address)
	argMappingStr := buildParamMapping(funcName, abiEntries)

	fieldOptions, globalVars := buildStorageInfo(address)
	availableContracts := buildAvailableContractsList()

	if fieldOptions == "" {
		fieldOptions = "(无 storage.json，请根据源码自行识别字段)\n"
	}
	if globalVars == "" {
		globalVars = "(无 storage.json，请根据源码自行识别全局变量)\n"
	}
	if availableContracts == "" {
		availableContracts = "(无其他可用合约)\n"
	}

	prompt := fmt.Sprintf(`分析以下以太坊主网合约 (地址: %s) 中函数 "%s" (selector: %s) 的保守读写集。直接返回JSON格式，不要任何分析文字。

合约代码：
%s

参数映射规则：
%s

字段选项 (from storage layout):
%s

全局变量列表：
%s

可用合约与函数列表 (用于识别跨合约调用)：
%s

重要规则：
1. 直接访问全局变量: {"account": "global", "field": "owner"}
2. 用函数参数作为键访问mapping: {"account": "addr1", "field": "balances"}
3. account可以是：addr1、addr2（函数参数）、msg.sender（交易发送者地址，即调用该函数的EOA）、global（直接访问全局变量）、或某个全局变量名（用该变量的值作为键）
4. msg.sender 标识交易发送者地址。当函数代码中访问 balances[msg.sender]、allowed[_from][msg.sender] 等以 msg.sender 为键的 mapping 时，必须用 {"account": "msg.sender", "field": "balances"} 表示。不要用 addr1/addr2 代替 msg.sender —— addr1/addr2 仅对应函数的地址参数，msg.sender 是独立于函数参数的发送者地址
5. 保守规则：对于if/else等条件语句，必须包含两个分支中所有可能的存储访问
6. 全局变量读写一致性：任何出现在writes中的全局变量，必须同时出现在reads中
7. 跨合约调用：如果合约代码中调用了上述"可用合约与函数列表"中的其他合约函数，必须在 crossCalls 数组中列出。crossCall 的 contract 字段可用合约地址（0x开头）或合约别名（如 USDT，见可用合约列表中的 alias 标注），function 字段用 selector (0x开头)

返回示例（transfer函数，_to=addr1, 发送者=msg.sender）：
{"reads":[{"account":"msg.sender","field":"balances"},{"account":"addr1","field":"balances"},{"account":"global","field":"paused"}],"writes":[{"account":"msg.sender","field":"balances"},{"account":"addr1","field":"balances"}],"crossCalls":[]}`,
		address, funcName, selector, sourceCode, argMappingStr, fieldOptions, globalVars, availableContracts)

	// Final safety net: cap the assembled prompt at 70 000 runes so the
	// JSON-encoded request body stays well under the Azure App Gateway
	// reverse-proxy hard limit (~100 KB) that the AMD endpoint sits behind.
	// A prompt this large still comfortably covers the function + all of
	// the non-source context (storage layout, ABI, cross-contract list).
	// Conservative-safety: any function affected was already covered by
	// the storage/ABI fallback.
	return truncateStr(prompt, 70000)
}

// llmResponseToMainnet converts an LLMResponse to a MainnetLLMResponse.
// Because the prompt instructs the LLM to return contract addresses and
// selectors directly in crossCalls, the CrossCalls fields are copied as-is.
func llmResponseToMainnet(resp *LLMResponse) *MainnetLLMResponse {
	out := &MainnetLLMResponse{
		Reads:  resp.Reads,
		Writes: resp.Writes,
	}
	for _, cc := range resp.CrossCalls {
		out.CrossCalls = append(out.CrossCalls, MainnetCrossCall{
			Contract: cc.Contract,
			Function: cc.Function,
		})
	}
	return out
}

// PreAnalyzeMainnetContract analyzes each (address, selector) pair via LLM and
// caches the result. Per the project concurrency constraint, up to
// runtime.NumCPU() pairs are analyzed concurrently. Already-cached pairs are
// skipped. In-flight pairs are allowed to finish on error; the first error
// (if any) is returned after all goroutines complete.
func PreAnalyzeMainnetContract(pairs []MainnetFuncPair) error {
	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, llmConfig.Concurrency) // concurrency cap from llmConfig to respect provider rate limits
		mu       sync.Mutex
		firstErr error
	)

	for _, pair := range pairs {
		if _, err := os.Stat(getMainnetCacheFilePath(pair.Address, pair.Selector)); err == nil {
			fmt.Printf("Function %s:%s already analyzed, skipping\n", pair.Address, pair.Selector)
			continue
		}

		wg.Add(1)
		pair := pair
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			fmt.Printf("Pre-analyzing mainnet function: %s:%s\n", pair.Address, pair.Selector)

			prompt := buildMainnetPrompt(pair.Address, pair.Selector)
			if prompt == "" {
				fmt.Printf("Warning: failed to build prompt for %s:%s, skipping\n", pair.Address, pair.Selector)
				return
			}

			resp, err := callLLM(prompt)
			if err != nil {
				fmt.Printf("Pre-analysis failed for %s:%s: %v\n", pair.Address, pair.Selector, err)
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			mainnetResp := llmResponseToMainnet(resp)
			if err := saveMainnetLLMCache(pair.Address, pair.Selector, mainnetResp); err != nil {
				fmt.Printf("Warning: failed to save cache for %s:%s: %v\n", pair.Address, pair.Selector, err)
			}
			fmt.Printf("Pre-analysis completed for %s:%s: reads=%d, writes=%d, crossCalls=%d\n",
				pair.Address, pair.Selector, len(resp.Reads), len(resp.Writes), len(resp.CrossCalls))
		}()
	}

	wg.Wait()
	fmt.Println("Pre-analysis of all mainnet functions completed")
	if firstErr != nil {
		return firstErr
	}
	return nil
}

// analyzeMainnetTransaction returns the cached RW-set analysis for a mainnet
// contract function, or ErrNotPreAnalyzed if it has not been pre-analyzed.
func analyzeMainnetTransaction(address, selector string) (*MainnetLLMResponse, error) {
	cached, err := loadMainnetLLMCache(address, selector)
	if err != nil {
		return nil, ErrNotPreAnalyzed
	}
	return cached, nil
}
