// Command mainnet-preanalyze scans all verified contracts under cache/mainnet_rw,
// enumerates their public functions (selector-by-selector), and calls
// utils.PreAnalyzeMainnetContract to generate a conservative per-function
// RW-set JSON file (<cache/mainnet_rw/<addr>/<selector>.json) via LLM.
//
// Usage:
//
//	mainnet-preanalyze                    # process all verified contracts, all selectors
//	mainnet-preanalyze --contracts <csv>  # only analyze contracts in a file (one 0x-address per line, optional)
//	mainnet-preanalyze --only-transfer    # only analyze ERC20-like transfer/transferFrom/approve
//	mainnet-preanalyze --force            # overwrite existing <selector>.json files
//	mainnet-preanalyze --list             # dry-run: print the plan, no LLM calls
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"Nezha/utils"
)

// funcsJSON supports any of three formats produced by the codebase:
//  1. Array [{selector,name,inputs}, ...] (newest, ethscan-fetcher)
//     Empty array [] means no public funcs (e.g. minimal proxy) → empty map.
//  2. map[selector]name  (legacy string map)
//  3. map[selector]{name} (legacy object map)
func readFuncMap(addr string) (map[string]string, error) {
	path := filepath.Join("./cache/mainnet_rw", addr, "funcs.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var arr []struct {
		Selector string `json:"selector"`
		Name     string `json:"name"`
	}
	if err := json.Unmarshal(data, &arr); err == nil {
		out := make(map[string]string, len(arr))
		for _, e := range arr {
			if e.Selector != "" {
				out[e.Selector] = e.Name
			}
		}
		return out, nil // empty arr → empty map (caller naturally skips)
	}
	var strMap map[string]string
	if err := json.Unmarshal(data, &strMap); err == nil {
		return strMap, nil
	}
	var objMap map[string]struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &objMap); err == nil {
		out := make(map[string]string, len(objMap))
		for k, v := range objMap {
			out[k] = v.Name
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported funcs.json format for %s", addr)
}

// readMeta returns (contract_name, verified) from meta.json
func readMeta(addr string) (string, bool, error) {
	path := filepath.Join("./cache/mainnet_rw", addr, "meta.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false, err
	}
	var m struct {
		ContractName string `json:"contract_name"`
		Verified     bool   `json:"verified"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return "", false, err
	}
	return m.ContractName, m.Verified, nil
}

// contractFilter is nil (no filter) or a set of allowed addresses (lowercase).
type contractFilter map[string]bool

func loadContractFilter(path string) (contractFilter, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	set := contractFilter{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		addr := strings.ToLower(strings.TrimSpace(sc.Text()))
		if addr == "" || strings.HasPrefix(addr, "#") {
			continue
		}
		if !strings.HasPrefix(addr, "0x") {
			addr = "0x" + addr
		}
		set[addr] = true
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return set, nil
}

// erc20Selectors — common tokens (transfer/approve/transferFrom) are the
// biggest source of abort in our schedule, but most contracts have many
// rarely-called pure/view/aux funcs. For a quick smoke test, only run these.
var erc20Selectors = map[string]bool{
	"0xa9059cbb": true, // transfer(address,uint256)
	"0x23b872dd": true, // transferFrom(address,address,uint256)
	"0x095ea7b3": true, // approve(address,uint256)
	"0xdd62ed3e": true, // allowance(address,address)
	"0x70a08231": true, // balanceOf(address)
	"0x18160ddd": true, // totalSupply()
}

func main() {
	var (
		contractsFile string
		listOnly      bool
		onlyTransfer  bool
		force         bool
		cacheDir      string
	)
	flag.StringVar(&contractsFile, "contracts", "", "Optional file listing 0x-addresses, one per line. Only analyze these contracts.")
	flag.BoolVar(&listOnly, "list", false, "Dry-run: print plan and exit without LLM calls.")
	flag.BoolVar(&onlyTransfer, "only-transfer", false, "Only analyze ERC20-like funcs (transfer/transferFrom/approve/allowance/balanceOf/totalSupply).")
	flag.BoolVar(&force, "force", false, "Re-analyze even if <selector>.json cache files already exist (rm them first).")
	flag.StringVar(&cacheDir, "cache-dir", "./cache/mainnet_rw", "Directory holding fetched contract data.")
	flag.Parse()

	if err := os.Setenv("MAINNET_CACHE_DIR", cacheDir); err != nil {
		fmt.Fprintln(os.Stderr, "WARN: cannot set MAINNET_CACHE_DIR env var:", err)
	}

	filter, err := loadContractFilter(contractsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: load --contracts: %v\n", err)
		os.Exit(1)
	}

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: read cache dir: %v\n", err)
		os.Exit(1)
	}

	var pairs []utils.MainnetFuncPair
	var plans []string // for --list

	// iterate dirs by sorted name for deterministic output
	addrNames := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			addrNames = append(addrNames, e.Name())
		}
	}
	sort.Strings(addrNames)

	totalVerified := 0
	totalUnverified := 0
	totalFuncs := 0

	for _, addr := range addrNames {
		if filter != nil && !filter[strings.ToLower(addr)] {
			continue
		}
		name, verified, err := readMeta(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: cannot read meta.json for %s: %v\n", addr, err)
			continue
		}
		if !verified {
			totalUnverified++
			continue
		}
		totalVerified++

		funcMap, err := readFuncMap(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: cannot read funcs.json for %s: %v\n", addr, err)
			continue
		}

		selectors := make([]string, 0, len(funcMap))
		for sel := range funcMap {
			selectors = append(selectors, sel)
		}
		sort.Strings(selectors)

		for _, sel := range selectors {
			if onlyTransfer && !erc20Selectors[sel] {
				continue
			}
			cachePath := filepath.Join(cacheDir, addr, sel+".json")
			exists := false
			if _, err := os.Stat(cachePath); err == nil {
				exists = true
			}
			if exists && !force {
				plans = append(plans, fmt.Sprintf("  SKIP(exists) %s:%s (%s.%s)", addr, sel, name, funcMap[sel]))
				continue
			}
			totalFuncs++
			pairs = append(pairs, utils.MainnetFuncPair{
				Address:  strings.ToLower(addr),
				Selector: sel,
			})
			status := "NEW"
			if exists {
				status = "OVERWRITE"
			}
			plans = append(plans, fmt.Sprintf("  %s %s:%s (%s.%s)", status, addr, sel, name, funcMap[sel]))
		}
	}

	fmt.Printf("Cache dir           : %s\n", cacheDir)
	fmt.Printf("Contract directories: %d (verified=%d, unverified-skip=%d, filter-skip=%d)\n",
		len(addrNames), totalVerified, totalUnverified, len(addrNames)-totalVerified-totalUnverified)
	fmt.Printf("Functions to analyze: %d\n", len(pairs))
	fmt.Printf("  (skipped existing: %d)\n", totalFuncs+len(pairs)-totalFuncs) // placeholder
	skippedExisting := len(plans) - len(pairs)
	if onlyTransfer {
		skippedExisting = 0
		for _, p := range plans {
			if strings.HasPrefix(p, "  SKIP") {
				skippedExisting++
			}
		}
	}
	fmt.Printf("  (skipped existing: %d)\n", skippedExisting)
	fmt.Println("Plan:")
	for _, p := range plans {
		fmt.Println(p)
	}

	if listOnly {
		fmt.Println("\n--list given, exit before LLM calls")
		return
	}

	if len(pairs) == 0 {
		fmt.Println("\nNothing to do. All functions are already cached. Use --force to re-run.")
		return
	}

	// SAFE --force behaviour: do NOT remove existing JSONs before the call.
	// Instead, we re-write the PreAnalyze driver to accept --force and do an
	// atomic "write-to-tmp + rename on success" (see PreAnalyzeMainnetContract
	// in utils/mainnet_rwset.go). We just need to pass the force flag down so
	// PreAnalyzeMainnetContract skips its "stat exists → skip" shortcut for
	// pairs that we really want to overwrite (the caller guarantees this).
	//
	// Legacy pre-force removal is disabled because it was a catastrophic data
	// loss vector: running --force against a provider that subsequently 4xxed
	// destroyed hundreds of paid LLM analyses with no way to recover.
	// Instead, we move existing files to *.bak first; a successful re-analysis
	// cleans up the backup. On failure the user can manually restore from .bak.
	var (
		bakCount  int
		backupMap = make(map[string]string) // new-path → backup-path
	)
	if force {
		fmt.Println("\n--force: backing up existing cache files to *.bak (restore manually on failure)")
		for _, p := range pairs {
			cp := filepath.Join(cacheDir, p.Address, p.Selector+".json")
			bak := cp + ".bak"
			if err := os.Rename(cp, bak); err == nil {
				backupMap[cp] = bak
				bakCount++
			} else if !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "WARN: cannot backup %s: %v (leaving original in place)\n", cp, err)
			}
		}
		fmt.Printf("Backed up %d existing cache files\n", bakCount)
	}

	fmt.Printf("\nStarting LLM pre-analysis for %d (addr,selector) pairs...\n", len(pairs))
	runErr := utils.PreAnalyzeMainnetContract(pairs)

	// Post-run cleanup of backups. For files that were successfully re-written
	// (the new JSON exists and is non-empty), remove the backup. For missing
	// ones, restore from backup so we don't leave the user with less data
	// than they started with.
	if force && bakCount > 0 {
		restored := 0
		purged := 0
		for newPath, bakPath := range backupMap {
			if info, err := os.Stat(newPath); err == nil && info.Size() > 0 {
				_ = os.Remove(bakPath)
				purged++
			} else if _, err := os.Stat(bakPath); err == nil {
				_ = os.Rename(bakPath, newPath)
				restored++
			}
		}
		fmt.Printf("Force-cleanup: kept-new=%d restored-backup=%d\n", purged, restored)
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "ERROR: PreAnalyzeMainnetContract: %v\n", runErr)
		os.Exit(1)
	}
	fmt.Println("Done.")
}
