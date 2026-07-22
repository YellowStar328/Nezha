package utils

import (
	"Nezha/core"
	"Nezha/ethereum/go-ethereum/common"
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
	"golang.org/x/crypto/sha3"
)

const (
	SmallBankSavingSlot   = 0
	SmallBankCheckingSlot = 1
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
	APIEndpoint: "http://localhost:8080/v1/chat/completions",
	APIKey:      "",
	MaxRetries:  3,
	Timeout:     30 * time.Second,
	Concurrency: 5,
}

func SetLLMConfig(config LLMConfig) {
	llmConfig = config
}

var llmCache sync.Map

func getStorageKey(accountID uint64, field string) []byte {
	accountStr := strconv.FormatUint(accountID, 10)

	var mappingSlot uint64
	if field == "saving" || field == "savings" || field == "balance" && strings.Contains(field, "saving") {
		mappingSlot = SmallBankSavingSlot
	} else {
		mappingSlot = SmallBankCheckingSlot
	}

	paddedAccount := common.RightPadBytes([]byte(accountStr), 32)
	paddedSlot := common.LeftPadBytes(big.NewInt(int64(mappingSlot)).Bytes(), 32)

	data := append(paddedAccount, paddedSlot...)
	hash := sha3.NewLegacyKeccak256()
	hash.Write(data)
	return hash.Sum(nil)
}

func buildLLMPrompt(function string, addr1, addr2 uint64) string {
	contractCode := `pragma solidity >=0.4.0 <0.7.0;
contract SmallBank {
    mapping(string => uint256) savingStore;  // slot 0
    mapping(string => uint256) checkingStore; // slot 1

    function almagate(string memory arg0, string memory arg1) public {
        uint256 bal1 = savingStore[arg0];
        uint256 bal2 = checkingStore[arg1];
        checkingStore[arg0] = 0;
        savingStore[arg1] = bal1 + bal2;
    }

    function getBalance(string memory arg0) public view returns (uint256 balance) {
        uint256 bal1 = savingStore[arg0];
        uint256 bal2 = checkingStore[arg0];
        balance = bal1 + bal2;
        return balance;
    }

    function updateBalance(string memory arg0, uint256 arg1) public {
        uint256 bal1 = checkingStore[arg0];
        checkingStore[arg0] = bal1 + arg1;
    }

    function updateSaving(string memory arg0, uint256 arg1) public {
        uint256 bal1 = savingStore[arg0];
        savingStore[arg0] = bal1 + arg1;
    }

    function sendPayment(string memory arg0, string memory arg1, uint256 arg2) public {
        uint256 bal1 = checkingStore[arg0];
        uint256 bal2 = checkingStore[arg1];
        uint256 amount = arg2;
        if (!(bal2 == 0 || bal2 == 25 || bal2 == 100)) {
            bal1 -= amount;
            amount = 0;
        }
        bal1 -= amount;
        bal2 += amount;
        checkingStore[arg0] = bal1;
        checkingStore[arg1] = bal2;
    }

    function writeCheck(string memory arg0, uint256 arg1) public {
        uint256 bal1 = checkingStore[arg0];
        uint256 bal2 = savingStore[arg0];
        uint256 amount = arg1;
        if (amount < bal1 + bal2) {
            checkingStore[arg0] = bal1 + amount - 1;
        } else {
            checkingStore[arg0] = bal1 + amount;
        }
    }
}`

	prompt := fmt.Sprintf(`你是一个智能合约分析专家。请分析以下 SmallBank 合约中函数 "%s" 的保守读写集。

合约代码：
%s

当前调用：%s("%d", "%d")

请返回保守的读写集（包含所有可能访问的存储位置，即使在某些条件下可能不被访问）。

返回格式要求（JSON格式，只返回JSON）：
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
- "checking" - 对应 checkingStore
- "saving" - 对应 savingStore

注意：
1. 保守分析意味着包含所有可能被访问的存储位置
2. 不要遗漏任何可能的分支路径
3. 只返回JSON，不要包含其他文字`, function, contractCode, function, addr1, addr2)

	return prompt
}

func callLLM(prompt string) (*LLMResponse, error) {
	reqBody := map[string]interface{}{
		"model": "gpt-4o-mini",
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

	return &llmResp, nil
}

func analyzeTransactionLLM(tx Transaction) (*LLMResponse, error) {
	cacheKey := fmt.Sprintf("%s_%d_%d", tx.Function, tx.Addr1, tx.Addr2)

	if cached, ok := llmCache.Load(cacheKey); ok {
		return cached.(*LLMResponse), nil
	}

	prompt := buildLLMPrompt(tx.Function, tx.Addr1, tx.Addr2)
	resp, err := callLLM(prompt)
	if err != nil {
		return nil, err
	}

	llmCache.Store(cacheKey, resp)
	return resp, nil
}

func llmResponseToRWSet(resp *LLMResponse, addr1, addr2 uint64) ([][]byte, [][]byte, [][]byte, [][]byte) {
	var rAddr, rValue, wAddr, wValue [][]byte

	for _, access := range resp.Reads {
		var accountID uint64
		if access.Account == "addr1" {
			accountID = addr1
		} else {
			accountID = addr2
		}
		key := getStorageKey(accountID, access.Field)
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
		key := getStorageKey(accountID, access.Field)
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

			abiObject, binData, loadErr := tools.LoadContract("./SmallBank/small_bank_sol_SmallBank.abi",
				"./SmallBank/small_bank_sol_SmallBank.bin")
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

			rMap, wMap := SelectFunctions2(lvm, fromAddr, addr, abiObject, tx.Function, tx.Addr1, tx.Addr2)

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

		rAddr, rValue, wAddr, wValue := llmResponseToRWSet(llmResp, tx.Addr1, tx.Addr2)

		rwNodes := core.CreateRWNode(strconv.FormatInt(int64(n), 10), uint32(n), rAddr, rValue, wAddr, wValue)

		lock.Lock()
		txs = append(txs, rwNodes)

		if shouldCapture {
			fromAddr := tools.NewRandomAddress()
			ctx := &core.TransactionContext{
				TxID:        strconv.FormatInt(int64(n), 10),
				Function:    tx.Function,
				Addr1:       tx.Addr1,
				Addr2:       tx.Addr2,
				PreReadSet:  make(map[string][]byte),
				PreWriteSet: make(map[string][]byte),
				FromAddr:    fromAddr,
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
