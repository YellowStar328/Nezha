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

// ErrKeyUnresolvable signals that the LLM cache contains an access whose
// concrete storage key cannot be derived statically — neither by the generic
// slot machinery nor by a registered calldata-derived key deriver. The caller
// must fall back to EVM PreExecute for the whole tx; silently dropping the key
// would under-approximate the conservative set and cause spurious key-exceed
// aborts during validation.
var ErrKeyUnresolvable = fmt.Errorf("LLM key unresolvable: mapping/struct key cannot be derived statically")

// MainnetTxArgs holds the decoded address arguments from a tx's calldata.
// Addr1 = first address-type parameter, Addr2 = second (if exists).
// MsgSender = tx.from (the EOA that signed the transaction).
// Selector = the 4-byte function selector (0x-prefixed lowercase).
// RawCalldata = the raw calldata bytes (including the 4-byte selector),
// used by calldata-derived key derivers (see mainnet_keyderive.go).
type MainnetTxArgs struct {
	Addr1       string // 0x-prefixed lowercase address (or "" if none)
	Addr2       string // 0x-prefixed lowercase address (or "" if none)
	MsgSender   string // 0x-prefixed lowercase address (tx.from)
	Selector    string // 0x-prefixed lowercase 8-hex selector
	RawCalldata []byte // raw calldata bytes including the 4-byte selector
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
	// rwSets is an in-memory cache of parsed LLM RW-set analyses:
	// key = strings.ToLower(address) + ":" + strings.ToLower(selector).
	// Populated eagerly in LoadAll so hot-path LLM hits never touch the disk.
	rwSets map[string]*MainnetLLMResponse
	// itemCache memoizes findStorageItem lookups: "addr:label" -> *storageItem.
	// Built lazily under mu on first access; layouts are immutable after
	// LoadAll, so cached pointers stay valid.
	itemCache map[string]*storageItem
	mu        sync.RWMutex
}

// NewMainnetContractManager returns a manager with baseDir = ./cache/mainnet_rw.
func NewMainnetContractManager() *MainnetContractManager {
	return &MainnetContractManager{
		baseDir:     "./cache/mainnet_rw",
		layouts:     make(map[string][]storageItem),
		abis:        make(map[string][]mainnetABIEntry),
		addrToAlias: make(map[string]string),
		aliasToAddr: make(map[string]string),
		rwSets:      make(map[string]*MainnetLLMResponse),
		itemCache:   make(map[string]*storageItem),
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

		// Eagerly load all per-function RW-set analyses into memory so that
		// hot-path LLM hits are pure map lookups instead of disk reads.
		m.loadRWSetCache(addr)

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

// loadRWSetCache reads every per-function analysis JSON under the contract's
// cache directory (all *.json except the special storage/abi/meta/funcs files)
// and stores the parsed result in m.rwSets. Cache misses on disk are skipped
// silently; callers fall back to EVM PreExecute as before.
func (m *MainnetContractManager) loadRWSetCache(addr string) {
	dirPath := filepath.Join(m.baseDir, addr)
	names, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}
	for _, name := range names {
		if name.IsDir() {
			continue
		}
		fname := name.Name()
		switch fname {
		case "storage.json", "abi.json", "meta.json", "funcs.json":
			continue
		}
		if !strings.HasSuffix(fname, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dirPath, fname))
		if err != nil {
			continue
		}
		var resp MainnetLLMResponse
		if jerr := json.Unmarshal(data, &resp); jerr != nil {
			continue
		}
		selector := strings.TrimSuffix(fname, ".json")
		m.rwSets[strings.ToLower(addr)+":"+strings.ToLower(selector)] = &resp
	}
}

// IsRWSetCached reports whether an LLM analysis for (address, selector) is
// present in the in-memory cache (i.e. a cache file existed at LoadAll time).
// The hot path uses this to skip the full GetConservativeRWSet machinery —
// including calldata decoding and a failed disk open — for known misses.
// Note: analyses written to disk after LoadAll are not visible here and fall
// back to EVM PreExecute, which is conservative and correct.
func (m *MainnetContractManager) IsRWSetCached(address, selector string) bool {
	key := strings.ToLower(address) + ":" + strings.ToLower(selector)
	m.mu.RLock()
	_, ok := m.rwSets[key]
	m.mu.RUnlock()
	return ok
}

// loadLLMResponse returns the parsed LLM analysis for (address, selector),
// preferring the in-memory cache populated by LoadAll. On a cache miss it falls
// back to reading the file from disk, so analyses written after LoadAll (e.g.
// by the pre-analysis tool) are still picked up.
func (m *MainnetContractManager) loadLLMResponse(address, selector string) (*MainnetLLMResponse, error) {
	key := strings.ToLower(address) + ":" + strings.ToLower(selector)
	m.mu.RLock()
	resp, ok := m.rwSets[key]
	m.mu.RUnlock()
	if ok {
		return resp, nil
	}
	return loadMainnetLLMCache(address, selector)
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
	resp, err := m.loadLLMResponse(address, selector)
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

	var unresolved bool
	readKeys, unresolved = m.abstractToKeys(address, resp.Reads, args, false)
	if unresolved {
		return nil, nil, ErrKeyUnresolvable
	}
	writeKeys, unresolved = m.abstractToKeys(address, resp.Writes, args, true)
	if unresolved {
		return nil, nil, ErrKeyUnresolvable
	}

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
// multiple concrete keys for a single abstract access (see
// abstractAccessToKeys). The returned unresolved flag is set when ANY access
// cannot be converted statically — the caller must treat the whole tx as
// unresolvable and fall back to EVM PreExecute instead of dropping keys.
func (m *MainnetContractManager) abstractToKeys(contractAddr string, accesses []LLMFieldAccess, args MainnetTxArgs, write bool) ([]string, bool) {
	var keys []string
	for _, acc := range accesses {
		more, unresolved := m.abstractAccessToKeys(contractAddr, acc, args, write)
		if unresolved {
			return nil, true
		}
		keys = append(keys, more...)
	}
	return keys, false
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
// concrete slot: keys. The returned unresolved flag is set when the key cannot
// be derived statically — neither by the generic slot machinery nor by a
// registered calldata-derived key deriver. The caller must then mark the whole
// tx as unresolvable (EVM PreExecute fallback). We NEVER silently drop a key,
// because that would under-approximate the conservative set and cause spurious
// key-exceed aborts during validation.
//
// For NESTED mappings (IsNested==true), a single {account,field} access
// represents ONE key of the 2-level structure. When no deriver is registered,
// we don't know statically which level this account occupies, so we
// conservatively emit the cartesian-product double-hash keys: for each
// resolved address `x` in {addr1,addr2,msg.sender} we output
// nestedMappingSlotKey(account_addr, x) AND nestedMappingSlotKey(x, account_addr).
// This is conservative over-approximation that guarantees the real key is
// present. Any extra keys cannot cause aborts (they only widen RW sets).
func (m *MainnetContractManager) abstractAccessToKeys(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs, write bool) ([]string, bool) {
	item, found := m.findStorageItem(contractAddr, acc.Field)
	if !found {
		// The LLM referenced a field that does not exist in the storage layout
		// (or the contract has no layout at all). We can neither derive a key
		// nor prove the access is harmless — under parallel DAG execution the
		// phantom field MAY be touched by real execution (observed on mainnet
		// block 24000000 for a 0xba83b5ed transfer where _balances WAS
		// accessed). Mark it unresolved so the tx falls back to EVM PreExecute
		// instead of dropping the key and risking a key-exceed abort.
		fmt.Printf("Warning: field %q not found in storage layout for %s is unresolvable\n", acc.Field, contractAddr)
		return nil, true
	}

	switch {
	case acc.Account == "global":
		// Case 1: direct global variable access.
		if item.KeyType != "simple" {
			// A mapping/struct accessed as "global" has no static key. First
			// try a registered calldata-derived key deriver (e.g.
			// receiveMessage's usedNonces). If none can handle it, mark the
			// tx unresolved instead of silently dropping the access.
			if keys, handled, unresolved := m.tryKeyDeriver(contractAddr, acc, args, write); handled {
				if unresolved {
					fmt.Printf("Warning: global access to non-simple field %s (keyType=%s) in %s is unresolvable\n", acc.Field, item.KeyType, contractAddr)
					return nil, true
				}
				return keys, false
			}
			fmt.Printf("Warning: global access to non-simple field %s (keyType=%s) in %s is unresolvable\n", acc.Field, item.KeyType, contractAddr)
			return nil, true
		}
		return []string{simpleSlotKey(contractAddr, item.Slot)}, false

	case item.KeyType == "simple":
		// Single-level global access disguised as an account access: safe fallback.
		return []string{simpleSlotKey(contractAddr, item.Slot)}, false

	case item.KeyType == "mapping" && !item.IsNested:
		// Case 2 (non-nested mapping): build single-level key using the account.
		addr, ok := resolveAccountAddr(acc.Account, args)
		if !ok {
			// Case 4 fallback: account is a global-var-name-as-key. Cannot
			// resolve statically → unresolved.
			return nil, true
		}
		return []string{mappingSlotKey(contractAddr, addr, item.Slot)}, false

	case item.KeyType == "mapping" && item.IsNested:
		// Case 3 (NESTED mapping, e.g. allowed[_from][msg.sender]).
		// A registered deriver may resolve the exact key from calldata
		// (e.g. CoinTool.t: map[msg.sender][_salt] where the inner key is a
		// dynamic bytes). If one is registered for this contract+selector it
		// decides the outcome — success yields the exact keys, failure means
		// the tx is unresolvable (never fall through to the cartesian guess,
		// which would emit wrong keys and under-approximate).
		if keys, handled, unresolved := m.tryKeyDeriver(contractAddr, acc, args, write); handled {
			if unresolved {
				return nil, true
			}
			return keys, false
		}
		// No deriver: {acc.Account, field} represents ONE level of the 2-level
		// key. We don't know statically whether it's the outer or inner key,
		// so we emit both orientations plus all cross-products with the other
		// known addresses in the tx — this is guaranteed to cover the true
		// key. For example:
		//   allowed access with account=addr1 should emit:
		//     nestedKey(addr1, addr2), nestedKey(addr1, msg.sender),
		//     nestedKey(addr2, addr1), nestedKey(msg.sender, addr1)
		//   Similarly with account=msg.sender.
		accAddr, ok := resolveAccountAddr(acc.Account, args)
		if !ok {
			// Cannot resolve the account statically; if it's a
			// global-var-as-key we also don't have the runtime value.
			return nil, true
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
		return out, false

	default:
		// KeyType == "string" or other unsupported — no static key.
		fmt.Printf("Warning: unsupported key type %q for field %s accessed with %s\n", item.KeyType, acc.Field, acc.Account)
		return nil, true
	}
}

// tryKeyDeriver consults the calldata-derived key deriver registry (keyed by
// "contractAddr:selector"). It returns:
//
//   - (nil, false, _)  : no deriver registered → caller proceeds with the
//     generic static machinery.
//   - (nil, true, true): a deriver is registered but cannot derive the key →
//     the access is unresolvable (EVM PreExecute fallback).
//   - (keys, true, false): the deriver derived the exact conservative keys.
func (m *MainnetContractManager) tryKeyDeriver(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs, write bool) ([]string, bool, bool) {
	deriver, ok := derivedKeyDerivers[strings.ToLower(contractAddr)+":"+strings.ToLower(args.Selector)]
	if !ok {
		return nil, false, false
	}
	dk, ok := deriver(contractAddr, acc, args)
	if !ok {
		return nil, true, true
	}
	if write {
		return dk.WriteKeys, true, false
	}
	return dk.ReadKeys, true, false
}

// findStorageItem looks up a storage item by label in the cached layout.
func (m *MainnetContractManager) findStorageItem(contractAddr, label string) (*storageItem, bool) {
	cacheKey := contractAddr + ":" + label
	m.mu.RLock()
	if it, ok := m.itemCache[cacheKey]; ok {
		m.mu.RUnlock()
		return it, true
	}
	items, ok := m.layouts[contractAddr]
	if !ok {
		m.mu.RUnlock()
		return nil, false
	}
	var found *storageItem
	for i := range items {
		if items[i].Label == label {
			found = &items[i]
			break
		}
	}
	m.mu.RUnlock()
	if found == nil {
		return nil, false
	}
	// Store under a short write-lock. Layouts are immutable after LoadAll, so
	// the pointer remains valid; a benign race (two goroutines storing the
	// same entry) is harmless.
	m.mu.Lock()
	m.itemCache[cacheKey] = found
	m.mu.Unlock()
	return found, true
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

		subResp, loadErr := m.loadLLMResponse(targetAddr, call.Function)
		if loadErr != nil {
			return nil, nil, fmt.Errorf("%w: %s:%s", ErrCrossContractNotCached, targetAddr, call.Function)
		}

		// Convert using the TARGET contract's address. An unresolvable key in
		// the sub-contract analysis is as dangerous as one in the caller's own
		// analysis — propagate it so the whole tx falls back to EVM.
		subReads, rUnres := m.abstractToKeys(targetAddr, subResp.Reads, args, false)
		if rUnres {
			return nil, nil, ErrKeyUnresolvable
		}
		subWrites, wUnres := m.abstractToKeys(targetAddr, subResp.Writes, args, true)
		if wUnres {
			return nil, nil, ErrKeyUnresolvable
		}
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

// keccak256Bytes returns the LegacyKeccak256 hash of data.
func keccak256Bytes(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

// rightPad32 pads data with trailing zeros up to a multiple of 32 bytes.
// This is Solidity's encoding for dynamic bytes keys inside mappings
// (keccak256 of the right-padded bytes).
func rightPad32(data []byte) []byte {
	n := ((len(data) + 31) / 32) * 32
	padded := make([]byte, n)
	copy(padded, data)
	return padded
}

// mappingSlotKeyBytes32 builds the value slot for a single-key mapping keyed
// by bytes32 (e.g. usedNonces: mapping(bytes32 => uint256)):
//
//	slot_key = keccak256(key32 || pad32(slot))
func mappingSlotKeyBytes32(contractAddr string, key32 []byte, slot uint64) string {
	keyBytes := pad32(key32)
	slotBytes := slotToBytes(slot)
	h := sha3.NewLegacyKeccak256()
	h.Write(keyBytes)
	h.Write(slotBytes)
	slotHash := h.Sum(nil)
	return fmt.Sprintf("slot:%s:%s", strings.ToLower(contractAddr), hexSlot(slotHash))
}

// nestedBytesMappingSlotKey builds the value slot for
// mapping(address => mapping(bytes => V)) where the inner key is a dynamic
// bytes value from calldata (e.g. CoinTool.t: map[msg.sender][_salt]).
//
// Verified against real mainnet storage access (block 24000000, CoinTool_App
// tx 176/187): Solidity uses the UNPADDED bytes data as h(k) for bytes/string
// mapping keys ("h(k) is just the unpadded data"), concatenated directly with
// the inner mapping base slot:
//
//	inner_slot = keccak256(pad32(outerAddr) || pad32(baseSlot))
//	value_slot = keccak256(innerBytes || inner_slot)
func nestedBytesMappingSlotKey(contractAddr, outerAddr string, innerBytes []byte, slot uint64) string {
	// Step 1: keccak256(paddedOuterAddr || paddedBaseSlot)
	h1 := sha3.NewLegacyKeccak256()
	h1.Write(pad32(addrToBytes(outerAddr)))
	h1.Write(slotToBytes(slot))
	innerSlot := h1.Sum(nil)

	// Step 2: keccak256(unpaddedBytes || innerSlot)
	h2 := sha3.NewLegacyKeccak256()
	h2.Write(innerBytes)
	h2.Write(innerSlot)
	finalHash := h2.Sum(nil)

	return fmt.Sprintf("slot:%s:%s", strings.ToLower(contractAddr), hexSlot(finalHash))
}
