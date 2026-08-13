package exporter

import (
	"fmt"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockWitness struct {
	Accounts map[string]*WitnessAccount `json:"accounts"`
}

type WitnessAccount struct {
	Balance  string            `json:"balance"`
	Nonce    string            `json:"nonce"`
	CodeHash string            `json:"codeHash"`
	Code     string            `json:"code,omitempty"`
	Storage  map[string]string `json:"storage"`
}

type TxRWSets struct {
	TxHash    string   `json:"txHash"`
	TxIndex   int      `json:"txIndex"`
	ReadKeys  []string `json:"readKeys"`
	WriteKeys []string `json:"writeKeys"`
}

type CanonicalReceipt struct {
	TxHash    string `json:"txHash"`
	TxIndex   int    `json:"txIndex"`
	Status    uint64 `json:"status"`
	GasUsed   uint64 `json:"gasUsed"`
	LogsCount int    `json:"logsCount"`
}

type CanonicalBlock struct {
	Receipts []CanonicalReceipt `json:"receipts"`
}

func BuildBlockWitness(
	block *types.Block,
	traces []*PrestateTracerResult,
	diffTraces []*PrestateTracerResult,
	receipts []*types.Receipt,
) (*BlockWitness, []TxRWSets, *CanonicalBlock, error) {
	witness := &BlockWitness{
		Accounts: make(map[string]*WitnessAccount),
	}

	txRWSets := make([]TxRWSets, 0, len(traces))

	canonical := &CanonicalBlock{
		Receipts: make([]CanonicalReceipt, 0, len(receipts)),
	}

	for i, receipt := range receipts {
		if receipt == nil {
			continue
		}
		canonical.Receipts = append(canonical.Receipts, CanonicalReceipt{
			TxHash:    receipt.TxHash.Hex(),
			TxIndex:   i,
			Status:    receipt.Status,
			GasUsed:   receipt.GasUsed,
			LogsCount: len(receipt.Logs),
		})
	}

	txs := block.Transactions()

	for txIndex, trace := range traces {
		var txHash string
		if txIndex < len(txs) && txs[txIndex] != nil {
			txHash = txs[txIndex].Hash().Hex()
		}

		if trace == nil {
			txRWSets = append(txRWSets, TxRWSets{TxHash: txHash, TxIndex: txIndex})
			continue
		}

		readKeys := make(map[string]bool)

		// preTrace (diffMode=false): populate witness + readKeys
		processPrestate(witness, trace.Accounts, readKeys)

		// diffTrace (diffMode=true): compute writeKeys from pre/post diff
		var writeKeys map[string]bool
		if txIndex < len(diffTraces) && diffTraces[txIndex] != nil {
			writeKeys = computeWriteKeys(diffTraces[txIndex].Accounts, diffTraces[txIndex].Added)
		} else {
			writeKeys = make(map[string]bool)
		}

		readKeyList := sortedKeys(readKeys)
		writeKeyList := sortedKeys(writeKeys)

		txRWSets = append(txRWSets, TxRWSets{
			TxHash:    txHash,
			TxIndex:   txIndex,
			ReadKeys:  readKeyList,
			WriteKeys: writeKeyList,
		})
	}

	return witness, txRWSets, canonical, nil
}

// computeWriteKeys compares pre-state and post-state to determine which keys were written.
// pre = diffMode response's "pre" field; post = "post" field.
// A key is a write if: (a) account is new (in post but not pre),
// (b) a field (balance/nonce/codeHash/storage slot) differs between pre and post,
// or (c) account was removed (in pre but not post, e.g. SELFDESTRUCT).
func computeWriteKeys(pre, post map[string]*PrestateAccount) map[string]bool {
	writeKeys := make(map[string]bool)

	for addr, postAcct := range post {
		if postAcct == nil {
			continue
		}
		preAcct, existed := pre[addr]

		if !existed || preAcct == nil {
			// new account created by this tx
			writeKeys[AcctKey(addr, "balance")] = true
			writeKeys[AcctKey(addr, "nonce")] = true
			if postAcct.Code != "" && postAcct.Code != "0x" {
				writeKeys[AcctKey(addr, "code")] = true
			}
			for slot := range postAcct.Storage {
				writeKeys[SlotKey(addr, slot)] = true
			}
			continue
		}

		// existing account: compare each field
		if preAcct.Balance != postAcct.Balance {
			writeKeys[AcctKey(addr, "balance")] = true
		}
		if preAcct.Nonce != postAcct.Nonce {
			writeKeys[AcctKey(addr, "nonce")] = true
		}
		if preAcct.CodeHash != postAcct.CodeHash {
			writeKeys[AcctKey(addr, "code")] = true
		}
		for slot, postVal := range postAcct.Storage {
			preVal, ok := preAcct.Storage[slot]
			if !ok || preVal != postVal {
				writeKeys[SlotKey(addr, slot)] = true
			}
		}
	}

	// removed accounts (in pre but not in post): SELFDESTRUCT case
	for addr := range pre {
		if _, ok := post[addr]; !ok {
			writeKeys[AcctKey(addr, "balance")] = true
			writeKeys[AcctKey(addr, "nonce")] = true
			writeKeys[AcctKey(addr, "code")] = true
		}
	}

	return writeKeys
}

func processDiffAccounts(
	witness *BlockWitness,
	accounts map[string]*PrestateAccount,
	readKeys, writeKeys map[string]bool,
) {
	for addr, acct := range accounts {
		if acct == nil {
			continue
		}
		if _, exists := witness.Accounts[addr]; !exists {
			witness.Accounts[addr] = &WitnessAccount{
				Balance:  string(acct.Balance),
				Nonce:    string(acct.Nonce),
				CodeHash: acct.CodeHash,
				Code:     acct.Code,
				Storage:  acct.Storage,
			}
		} else {
			existing := witness.Accounts[addr]
			if existing.Storage == nil {
				existing.Storage = make(map[string]string)
			}
			for slot, val := range acct.Storage {
				if _, ok := existing.Storage[slot]; !ok {
					existing.Storage[slot] = val
				}
			}
		}

		for slot := range acct.Storage {
			key := SlotKey(addr, slot)
			readKeys[key] = true
			writeKeys[key] = true
		}

		if acct.Balance != "" && acct.Balance != "0x0" {
			key := AcctKey(addr, "balance")
			readKeys[key] = true
			writeKeys[key] = true
		}
		if acct.Nonce != "" {
			key := AcctKey(addr, "nonce")
			readKeys[key] = true
			writeKeys[key] = true
		}
		if acct.CodeHash != "" && acct.CodeHash != "0x0" {
			key := AcctKey(addr, "code")
			readKeys[key] = true
		}
	}
}

func processDiffAdded(
	witness *BlockWitness,
	added map[string]*PrestateAccount,
	writeKeys map[string]bool,
) {
	for addr, acct := range added {
		if acct == nil {
			continue
		}
		if _, exists := witness.Accounts[addr]; !exists {
			witness.Accounts[addr] = &WitnessAccount{
				Balance:  string(acct.Balance),
				Nonce:    string(acct.Nonce),
				CodeHash: acct.CodeHash,
				Code:     acct.Code,
				Storage:  acct.Storage,
			}
		} else {
			existing := witness.Accounts[addr]
			if existing.Storage == nil {
				existing.Storage = make(map[string]string)
			}
			for slot, val := range acct.Storage {
				if _, ok := existing.Storage[slot]; !ok {
					existing.Storage[slot] = val
				}
			}
		}

		for slot := range acct.Storage {
			key := SlotKey(addr, slot)
			writeKeys[key] = true
		}
		if acct.Balance != "" {
			key := AcctKey(addr, "balance")
			writeKeys[key] = true
		}
		if acct.Nonce != "" {
			key := AcctKey(addr, "nonce")
			writeKeys[key] = true
		}
	}
}

func processDiffRemoved(
	witness *BlockWitness,
	removed map[string]*PrestateAccount,
	writeKeys map[string]bool,
) {
	for addr := range removed {
		writeKeys[AcctKey(addr, "balance")] = true
		writeKeys[AcctKey(addr, "nonce")] = true
		writeKeys[AcctKey(addr, "code")] = true
	}
}

func processPrestate(
	witness *BlockWitness,
	accounts map[string]*PrestateAccount,
	readKeys map[string]bool,
) {
	for addr, acct := range accounts {
		if acct == nil {
			continue
		}
		if _, exists := witness.Accounts[addr]; !exists {
			witness.Accounts[addr] = &WitnessAccount{
				Balance:  string(acct.Balance),
				Nonce:    string(acct.Nonce),
				CodeHash: acct.CodeHash,
				Code:     acct.Code,
				Storage:  acct.Storage,
			}
		} else {
			existing := witness.Accounts[addr]
			if existing.Storage == nil {
				existing.Storage = make(map[string]string)
			}
			for slot, val := range acct.Storage {
				if _, ok := existing.Storage[slot]; !ok {
					existing.Storage[slot] = val
				}
			}
		}

		for slot := range acct.Storage {
			key := SlotKey(addr, slot)
			readKeys[key] = true
		}
		if acct.Balance != "" && acct.Balance != "0x0" {
			key := AcctKey(addr, "balance")
			readKeys[key] = true
		}
		if acct.Nonce != "" {
			key := AcctKey(addr, "nonce")
			readKeys[key] = true
		}
		if acct.CodeHash != "" && acct.CodeHash != "0x0" {
			key := AcctKey(addr, "code")
			readKeys[key] = true
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type CodeStore struct {
	mu    sync.Mutex
	codes map[string]bool
}

func NewCodeStore() *CodeStore {
	return &CodeStore{
		codes: make(map[string]bool),
	}
}

func (cs *CodeStore) Add(codeHash string) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, exists := cs.codes[codeHash]; exists {
		return false
	}
	cs.codes[codeHash] = true
	return true
}

func (cs *CodeStore) Count() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.codes)
}

type BlockDataset struct {
	Header       map[string]interface{} `json:"header"`
	Transactions []interface{}          `json:"transactions"`
	Witness      *BlockWitness          `json:"witness"`
	Canonical    *CanonicalBlock        `json:"canonical"`
	RWSets       []TxRWSets             `json:"rwsets"`
}

type DatasetManifest struct {
	FormatVersion int    `json:"formatVersion"`
	ChainID       int    `json:"chainId"`
	FromBlock     int    `json:"fromBlock"`
	ToBlock       int    `json:"toBlock"`
	ExportedAt    string `json:"exportedAt"`
	StateAnchor   string `json:"stateAnchor"`
	ExecutionMode string `json:"executionMode"`
	SourceClient  string `json:"sourceClient"`
	HashWindow    int    `json:"hashWindow"`
}

func NewDatasetManifest(fromBlock, toBlock int, sourceClient string) *DatasetManifest {
	return &DatasetManifest{
		FormatVersion: 1,
		ChainID:       1,
		FromBlock:     fromBlock,
		ToBlock:       toBlock,
		ExportedAt:    "2026-08-11T00:00:00Z",
		StateAnchor:   "pre-first-user-tx",
		ExecutionMode: "block-local-user-tx",
		SourceClient:  sourceClient,
		HashWindow:    256,
	}
}

func ValidateWitness(witness *BlockWitness) error {
	if witness == nil {
		return fmt.Errorf("witness is nil")
	}
	if len(witness.Accounts) == 0 {
		return fmt.Errorf("witness has no accounts")
	}
	return nil
}

func ValidateRWSets(rwsets []TxRWSets) error {
	for i, rw := range rwsets {
		if len(rw.ReadKeys) == 0 && len(rw.WriteKeys) == 0 {
			continue
		}
		for _, k := range rw.ReadKeys {
			_, _, _, err := ParseKey(k)
			if err != nil {
				return fmt.Errorf("tx %d read key %s: %w", i, k, err)
			}
		}
		for _, k := range rw.WriteKeys {
			_, _, _, err := ParseKey(k)
			if err != nil {
				return fmt.Errorf("tx %d write key %s: %w", i, k, err)
			}
		}
	}
	return nil
}
