package utils

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/crypto/sha3"
)

// MainnetTxArgs holds the decoded address arguments from a tx's calldata.
// Addr1 = first address-type parameter, Addr2 = second (if exists).
// MsgSender = tx.from (the EOA that signed the transaction).
type MainnetTxArgs struct {
	Addr1     string // 0x-prefixed lowercase address (or "" if none)
	Addr2     string // 0x-prefixed lowercase address (or "" if none)
	MsgSender string // 0x-prefixed lowercase address (tx.from)
}

// storageItem is a parsed storage layout entry.
type storageItem struct {
	Label     string // e.g. "balances", "owner"
	Slot      uint64 // slot number
	KeyType   string // "simple", "mapping", "string"
	IsNested  bool   // true iff the mapping value is itself a mapping (double-hash)
	RawType   string // e.g. "t_address", "t_uint256", "t_mapping(...)"
	IsAddress bool   // true iff RawType == "t_address" (used for global-var-as-key detection)
}

// MainnetContractManager loads and queries analyzed mainnet contracts.
type MainnetContractManager struct {
	baseDir string
	// cache of loaded storage layouts: address -> []storageItem
	layouts map[string][]storageItem
	// cache of loaded ABI entries: address -> []mainnetABIEntry
	abis map[string][]mainnetABIEntry
	// addrToAlias maps address(lowercase) -> alias (contract_name)
	addrToAlias map[string]string
	// aliasToAddr maps alias -> address(lowercase)
	aliasToAddr map[string]string
	mu          sync.RWMutex
}

// NewMainnetContractManager returns a manager with baseDir = ./cache/mainnet_rw.
func NewMainnetContractManager() *MainnetContractManager {
	return &MainnetContractManager{
		baseDir:     "./cache/mainnet_rw",
		layouts:     make(map[string][]storageItem),
		abis:        make(map[string][]mainnetABIEntry),
		addrToAlias: make(map[string]string),
		aliasToAddr: make(map[string]string),
	}
}

// LoadAll scans all subdirectories of baseDir. For each directory (address),
// it loads storage.json (if exists) into m.layouts[addr] and abi.json into
// m.abis[addr].
func (m *MainnetContractManager) LoadAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		// Missing cache directory is not fatal: treat as empty cache so that
		// every tx falls back to EVM PreExecute in LLM mode.
		if os.IsNotExist(err) {
			fmt.Printf("Warning: mainnet cache dir %s does not exist; all txs will fall back to EVM\n", m.baseDir)
			return nil
		}
		return fmt.Errorf("failed to read mainnet cache dir %s: %w", m.baseDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		addr := entry.Name()

		// Load storage.json if it exists
		storagePath := filepath.Join(m.baseDir, addr, "storage.json")
		if data, err := os.ReadFile(storagePath); err == nil {
			items, perr := parseMainnetStorageLayout(data)
			if perr != nil {
				fmt.Printf("Warning: failed to parse storage.json for %s: %v\n", addr, perr)
			} else {
				m.layouts[addr] = items
			}
		}

		// Load abi.json if it exists
		abiPath := filepath.Join(m.baseDir, addr, "abi.json")
		if data, err := os.ReadFile(abiPath); err == nil {
			var abiEntries []mainnetABIEntry
			if jerr := json.Unmarshal(data, &abiEntries); jerr != nil {
				fmt.Printf("Warning: failed to parse abi.json for %s: %v\n", addr, jerr)
			} else {
				m.abis[addr] = abiEntries
			}
		}

		// Load meta.json if it exists; populate alias maps when contract_name
		// is non-empty so crossCalls can reference this contract by alias.
		metaPath := filepath.Join(m.baseDir, addr, "meta.json")
		if data, err := os.ReadFile(metaPath); err == nil {
			var meta struct {
				Address      string `json:"address"`
				ContractName string `json:"contract_name"`
				Verified     bool   `json:"verified"`
			}
			if jerr := json.Unmarshal(data, &meta); jerr == nil && meta.ContractName != "" {
				m.addrToAlias[addr] = meta.ContractName
				m.aliasToAddr[meta.ContractName] = addr
				fmt.Printf("loaded alias %s -> %s\n", meta.ContractName, addr)
			}
		}
	}

	return nil
}

// parseMainnetStorageLayout parses a solc storage layout JSON into []storageItem.
// Storage.json format mirrors parseSolcStorageLayout in contract_manager.go:
// {"storage":[{"slot":"0","label":"owner","type":"t_address"}],
//
//	"types":{"t_address":{"encoding":"inplace"}}}
func parseMainnetStorageLayout(data []byte) ([]storageItem, error) {
	var layout mainnetStorageLayout
	if err := json.Unmarshal(data, &layout); err != nil {
		return nil, err
	}

	var result []storageItem
	for _, item := range layout.Storage {
		slot, err := strconv.ParseUint(item.Slot, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid slot %q for %s: %w", item.Slot, item.Label, err)
		}

		keyType := "simple"
		isNested := false
		if typeInfo, ok := layout.Types[item.Type]; ok {
			if typeInfo.Encoding == "mapping" {
				if strings.Contains(typeInfo.Key, "string") {
					keyType = "string"
				} else {
					keyType = "mapping"
				}
				// Detect nested mappings: the value of the outer mapping is
				// itself a mapping type (recursive "encoding":"mapping").
				if typeInfo.Value != "" {
					if valueInfo, vok := layout.Types[typeInfo.Value]; vok && valueInfo.Encoding == "mapping" {
						isNested = true
					}
				}
			}
		}

		result = append(result, storageItem{
			Label:     item.Label,
			Slot:      slot,
			KeyType:   keyType,
			IsNested:  isNested,
			RawType:   item.Type,
			IsAddress: item.Type == "t_address",
		})
	}

	return result, nil
}

// GetConservativeRWSet is the main entry point. It loads the cached LLM
// analysis for (address, selector), converts the abstract reads/writes to
// concrete acct:/slot: keys, and recursively merges cross-contract calls.
func (m *MainnetContractManager) GetConservativeRWSet(address, selector string, args MainnetTxArgs) (readKeys, writeKeys []string, err error) {
	resp, err := loadMainnetLLMCache(address, selector)
	if err != nil {
		return nil, nil, ErrNotPreAnalyzed
	}

	// Detect accesses whose account is a global-variable-name-as-key
	// (e.g. {"account":"owner","field":"balances"}). These represent
	// second-level indirect accesses like balances[owner] whose concrete
	// slot key requires the runtime value of the global variable. Static
	// analysis cannot resolve them, so we signal the caller to fall back
	// to EVM PreExecute for this tx.
	if hasUnresolvableAccount(resp.Reads) || hasUnresolvableAccount(resp.Writes) {
		return nil, nil, ErrUnresolvableAccount
	}

	readKeys = m.abstractToKeys(address, resp.Reads, args)
	writeKeys = m.abstractToKeys(address, resp.Writes, args)

	if len(resp.CrossCalls) > 0 {
		extraReads, extraWrites, crossErr := m.mergeCrossCalls(address, resp.CrossCalls, args, 0)
		if crossErr != nil {
			return nil, nil, crossErr
		}
		readKeys = append(readKeys, extraReads...)
		writeKeys = append(writeKeys, extraWrites...)
	}

	readKeys = dedupKeys(readKeys)
	writeKeys = dedupKeys(writeKeys)

	return readKeys, writeKeys, nil
}

// abstractToKeys converts each abstract {account, field} access to concrete
// slot: key strings for the given contract address. Nested mappings may emit
// multiple concrete keys for a single abstract access (see abstractAccessToKeys).
func (m *MainnetContractManager) abstractToKeys(contractAddr string, accesses []LLMFieldAccess, args MainnetTxArgs) []string {
	var keys []string
	for _, acc := range accesses {
		more := m.abstractAccessToKeys(contractAddr, acc, args)
		keys = append(keys, more...)
	}
	return keys
}

// resolveAccountAddr translates a LLM account tag (addr1 / addr2 /
// msg.sender / a global-variable name / "global") to a concrete 0x address.
// Returns ("", false) when resolution is impossible statically.
func resolveAccountAddr(account string, args MainnetTxArgs) (string, bool) {
	switch account {
	case "addr1":
		return args.Addr1, args.Addr1 != ""
	case "addr2":
		return args.Addr2, args.Addr2 != ""
	case "msg.sender":
		return args.MsgSender, args.MsgSender != ""
	case "global":
		// simple variable access — caller must handle separately
		return "", false
	default:
		// account = a global-var-name-as-key (e.g. "owner", "pool1")
		// Static conversion not supported.
		return "", false
	}
}

// hasUnresolvableAccount returns true if any access uses an account that is
// neither a known concrete tag (addr1/addr2/msg.sender) nor "global". Such
// accounts are global-variable-name-as-key references (e.g. "owner") that
// require runtime state to resolve and cannot be handled statically.
func hasUnresolvableAccount(accesses []LLMFieldAccess) bool {
	for _, acc := range accesses {
		switch acc.Account {
		case "addr1", "addr2", "msg.sender", "global":
			continue
		default:
			return true
		}
	}
	return false
}

// collectAvailableAddrs returns every concrete address we can resolve for the
// transaction (addr1, addr2, msg.sender) in a fixed order for deterministic
// conservative expansion of nested mapping accesses.
func collectAvailableAddrs(args MainnetTxArgs) []string {
	out := make([]string, 0, 3)
	for _, a := range []string{args.Addr1, args.Addr2, args.MsgSender} {
		if a != "" {
			out = append(out, a)
		}
	}
	return out
}

// abstractAccessToKeys converts a single abstract access to one or more
// concrete slot: keys. It returns an empty slice when the access cannot be
// converted statically and should be skipped.
//
// For NESTED mappings (IsNested==true), a single {account,field} access
// represents ONE key of the 2-level structure. Because we don't know which
// level this account occupies, we conservatively emit the cartesian-product
// double-hash keys: for each resolved address `x` in {addr1,addr2,msg.sender}
// we output nestedMappingSlotKey(account_addr, x) AND
// nestedMappingSlotKey(x, account_addr) plus the single-level hashes of both.
// This is conservative over-approximation that guarantees the real key is
// present. Any extra keys cannot cause aborts (they only widen RW sets).
func (m *MainnetContractManager) abstractAccessToKeys(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs) []string {
	item, found := m.findStorageItem(contractAddr, acc.Field)
	if !found {
		fmt.Printf("Warning: field %q not found in storage layout for %s\n", acc.Field, contractAddr)
		return nil
	}

	switch {
	case acc.Account == "global":
		// Case 1: direct global variable access.
		if item.KeyType != "simple" {
			fmt.Printf("Warning: global access to non-simple field %s (keyType=%s) in %s\n", acc.Field, item.KeyType, contractAddr)
			return nil
		}
		return []string{simpleSlotKey(contractAddr, item.Slot)}

	case item.KeyType == "simple":
		// Single-level global access disguised as an account access: safe fallback.
		return []string{simpleSlotKey(contractAddr, item.Slot)}

	case item.KeyType == "mapping" && !item.IsNested:
		// Case 2 (non-nested mapping): build single-level key using the account.
		addr, ok := resolveAccountAddr(acc.Account, args)
		if !ok {
			// Case 4 fallback: account is a global-var-name-as-key. Cannot
			// resolve statically; skip.
			return nil
		}
		return []string{mappingSlotKey(contractAddr, addr, item.Slot)}

	case item.KeyType == "mapping" && item.IsNested:
		// Case 3 (NESTED mapping, e.g. allowed[_from][msg.sender]):
		// {acc.Account, field} represents ONE level of the 2-level key. We
		// don't know statically whether it's the outer or inner key, so we
		// emit both orientations plus all cross-products with the other
		// known addresses in the tx — this is guaranteed to cover the true
		// key. For example:
		//   allowed access with account=addr1 should emit:
		//     nestedKey(addr1, addr2), nestedKey(addr1, msg.sender),
		//     nestedKey(addr2, addr1), nestedKey(msg.sender, addr1)
		//   Similarly with account=msg.sender.
		accAddr, ok := resolveAccountAddr(acc.Account, args)
		if !ok {
			// Cannot resolve the account statically; if it's a
			// global-var-as-key we also don't have the runtime value. Skip.
			return nil
		}
		others := collectAvailableAddrs(args)
		var out []string
		// Also include accAddr paired with itself just in case both keys
		// resolve to the same address; deduplication happens later anyway.
		seen := make(map[string]bool)
		for _, other := range others {
			// Orientation A: accAddr is the OUTER key, other is the INNER key
			k := nestedMappingSlotKey(contractAddr, accAddr, other, item.Slot)
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
			// Orientation B: other is the OUTER key, accAddr is the INNER key
			k2 := nestedMappingSlotKey(contractAddr, other, accAddr, item.Slot)
			if !seen[k2] {
				seen[k2] = true
				out = append(out, k2)
			}
		}
		return out

	default:
		// KeyType == "string" or other unsupported — no static key.
		fmt.Printf("Warning: unsupported key type %q for field %s accessed with %s\n", item.KeyType, acc.Field, acc.Account)
		return nil
	}
}

// findStorageItem looks up a storage item by label in the cached layout.
func (m *MainnetContractManager) findStorageItem(contractAddr, label string) (*storageItem, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items, ok := m.layouts[contractAddr]
	if !ok {
		return nil, false
	}
	for i := range items {
		if items[i].Label == label {
			return &items[i], true
		}
	}
	return nil, false
}

// mergeCrossCalls recursively merges conservative RW sets from cross-contract
// calls. The target contract's abstract accesses are converted using the
// TARGET contract's address (call.Contract), not the caller's.
func (m *MainnetContractManager) mergeCrossCalls(callerAddr string, calls []MainnetCrossCall, args MainnetTxArgs, depth int) (extraReads, extraWrites []string, err error) {
	if depth >= 5 {
		return nil, nil, fmt.Errorf("mergeCrossCalls max depth exceeded at caller %s", callerAddr)
	}

	for _, call := range calls {
		targetAddr, ok := m.ResolveContractIdentifier(call.Contract)
		if !ok {
			return nil, nil, fmt.Errorf("%w: alias %q not registered", ErrCrossContractNotCached, call.Contract)
		}

		subResp, loadErr := loadMainnetLLMCache(targetAddr, call.Function)
		if loadErr != nil {
			return nil, nil, fmt.Errorf("%w: %s:%s", ErrCrossContractNotCached, targetAddr, call.Function)
		}

		// Convert using the TARGET contract's address.
		subReads := m.abstractToKeys(targetAddr, subResp.Reads, args)
		subWrites := m.abstractToKeys(targetAddr, subResp.Writes, args)
		extraReads = append(extraReads, subReads...)
		extraWrites = append(extraWrites, subWrites...)

		// Recurse into nested cross calls.
		if len(subResp.CrossCalls) > 0 {
			nestedReads, nestedWrites, nestedErr := m.mergeCrossCalls(targetAddr, subResp.CrossCalls, args, depth+1)
			if nestedErr != nil {
				return nil, nil, nestedErr
			}
			extraReads = append(extraReads, nestedReads...)
			extraWrites = append(extraWrites, nestedWrites...)
		}
	}

	return extraReads, extraWrites, nil
}

// IsAnalyzed checks if the cached LLM analysis exists for (address, selector).
func (m *MainnetContractManager) IsAnalyzed(address, selector string) bool {
	path := filepath.Join(m.baseDir, address, selector+".json")
	_, err := os.Stat(path)
	return err == nil
}

// ResolveContractIdentifier resolves a crossCall contract identifier to a
// canonical lowercase 0x address. If id is a valid 0x address (len==42,
// "0x" prefix, all hex), returns (lowercase(id), true). Otherwise looks up
// the alias→address map. Returns ("", false) if not resolvable.
func (m *MainnetContractManager) ResolveContractIdentifier(id string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if isHexAddress(id) {
		return strings.ToLower(id), true
	}
	if addr, ok := m.aliasToAddr[id]; ok {
		return addr, true
	}
	return "", false
}

// isHexAddress reports whether s is a 0x-prefixed 42-character hex address.
func isHexAddress(s string) bool {
	if len(s) != 42 {
		return false
	}
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return false
	}
	for i := 2; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// GetStorageLayout returns the cached storage layout for a contract (or nil if
// not loaded).
func (m *MainnetContractManager) GetStorageLayout(address string) []storageItem {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.layouts[address]
}

// GetABIInputs returns the ABI entry for the function whose 4-byte selector
// matches the given selector. The selector must be 0x-prefixed lowercase 8-hex
// chars (e.g. "0xa9059cbb"). The returned slice contains at most one entry;
// callers iterate entry.Inputs to determine which params are addresses.
// Returns nil if the ABI is not loaded or no function matches.
func (m *MainnetContractManager) GetABIInputs(address, selector string) []mainnetABIEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries, ok := m.abis[address]
	if !ok {
		return nil
	}
	target := strings.ToLower(selector)
	if !strings.HasPrefix(target, "0x") {
		target = "0x" + target
	}
	for _, e := range entries {
		if e.Type != "function" && e.Type != "" {
			continue
		}
		sig := formatSignature(e)
		h := sha3.NewLegacyKeccak256()
		h.Write([]byte(sig))
		sum := h.Sum(nil)
		sel := "0x" + hex.EncodeToString(sum[:4])
		if sel == target {
			return []mainnetABIEntry{e}
		}
	}
	return nil
}

// --- key computation helpers ---

// dedupKeys removes duplicate keys while preserving first-seen order.
func dedupKeys(keys []string) []string {
	seen := make(map[string]bool, len(keys))
	result := make([]string, 0, len(keys))
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			result = append(result, k)
		}
	}
	return result
}

// pad32 pads a value to 32 bytes (left-pad for big-endian integers/addresses).
func pad32(data []byte) []byte {
	if len(data) >= 32 {
		return data[:32]
	}
	padded := make([]byte, 32)
	copy(padded[32-len(data):], data)
	return padded
}

// hexSlot converts a 32-byte slot hash to "0x..." lowercase hex.
func hexSlot(hash []byte) string {
	return "0x" + hex.EncodeToString(hash)
}

// addrToBytes converts a 0x-prefixed address string to 20 bytes.
func addrToBytes(addr string) []byte {
	if strings.HasPrefix(addr, "0x") || strings.HasPrefix(addr, "0X") {
		addr = addr[2:]
	}
	b, _ := hex.DecodeString(addr)
	return b
}

// slotToBytes converts a uint64 slot number to a 32-byte big-endian padded value.
func slotToBytes(slot uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, slot)
	return pad32(b)
}

// simpleSlotKey builds "slot:<contractAddr>:<hex(padded_slot)>" for a simple
// (non-mapping) state variable.
func simpleSlotKey(contractAddr string, slot uint64) string {
	return fmt.Sprintf("slot:%s:%s", strings.ToLower(contractAddr), hexSlot(slotToBytes(slot)))
}

// mappingSlotKey builds "slot:<contractAddr>:<hex(keccak256(paddedAddr . paddedSlot))>"
// for a single-key mapping access keyed by an address.
func mappingSlotKey(contractAddr, addr string, slot uint64) string {
	keyBytes := pad32(addrToBytes(addr))
	slotBytes := slotToBytes(slot)
	h := sha3.NewLegacyKeccak256()
	h.Write(keyBytes)
	h.Write(slotBytes)
	slotHash := h.Sum(nil)
	return fmt.Sprintf("slot:%s:%s", strings.ToLower(contractAddr), hexSlot(slotHash))
}

// nestedMappingSlotKey builds the 2-level keccak256 key for a nested
// mapping mapping(address => mapping(address => V)).
//
// Solidity formula:
//
//	inner_slot = keccak256(outer_addr_padded || base_slot_padded)
//	final_slot = keccak256(inner_addr_padded || inner_slot)
func nestedMappingSlotKey(contractAddr, outerAddr, innerAddr string, slot uint64) string {
	// Step 1: keccak256(paddedOuterAddr || paddedBaseSlot)
	h1 := sha3.NewLegacyKeccak256()
	h1.Write(pad32(addrToBytes(outerAddr)))
	h1.Write(slotToBytes(slot))
	innerSlot := h1.Sum(nil)

	// Step 2: keccak256(paddedInnerAddr || innerSlot)  [no second slot append]
	h2 := sha3.NewLegacyKeccak256()
	h2.Write(pad32(addrToBytes(innerAddr)))
	h2.Write(innerSlot)
	finalHash := h2.Sum(nil)

	return fmt.Sprintf("slot:%s:%s", strings.ToLower(contractAddr), hexSlot(finalHash))
}
