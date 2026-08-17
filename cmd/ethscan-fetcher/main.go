package main

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/sha3"
)

// contractRow is a single filtered CSV record.
type contractRow struct {
	Address    string
	IsContract int
	CallCount  uint64
}

// etherscanResponse is the top-level Etherscan API response. Result is kept
// raw because Etherscan returns either an array of objects (verified/unverified
// object form) or an array of strings (plain "Contract source code not verified").
type etherscanResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

type etherscanSourceResult struct {
	SourceCode       string `json:"SourceCode"`
	ABI              string `json:"ABI"`
	ContractName     string `json:"ContractName"`
	CompilerVersion  string `json:"CompilerVersion"`
	OptimizationUsed string `json:"OptimizationUsed"`
	Runs             string `json:"Runs"`
	Proxy            string `json:"Proxy"`
	Implementation   string `json:"Implementation"`
	StorageLayout    string `json:"StorageLayout"`
}

type metaInfo struct {
	Address          string `json:"address"`
	ContractName     string `json:"contract_name"`
	CompilerVersion  string `json:"compiler_version"`
	Verified         bool   `json:"verified"`
	Proxy            string `json:"proxy"`
	Implementation   string `json:"implementation"`
	OptimizationUsed string `json:"optimization_used"`
	Runs             string `json:"runs"`
	Source           string `json:"source"`
}

type abiInput struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type abiItem struct {
	Type    string     `json:"type"`
	Name    string     `json:"name"`
	Inputs  []abiInput `json:"inputs"`
	Outputs []abiInput `json:"outputs"`
}

type funcEntry struct {
	Selector string     `json:"selector"`
	Name     string     `json:"name"`
	Inputs   []abiInput `json:"inputs"`
}

type fetchStatus int

const (
	statusVerified fetchStatus = iota
	statusUnverified
	statusFailed
)

type processedContract struct {
	address      string
	status       fetchStatus
	contractName string
	compilerVer  string
	hasStorage   bool
	source       string
}

// ---------------------------------------------------------------------------
// Shared HTTP client + resilient GET (mirrors Python check_verification.py)
// ---------------------------------------------------------------------------

// httpClient is initialized in main() so --proxy / HTTPS_PROXY can be applied.
var httpClient = &http.Client{Timeout: 20 * time.Second}

// initHTTPClient configures the package-level httpClient with an optional
// explicit proxy URL. If proxyURL is empty, the client falls back to Go's
// default transport (which honors HTTP_PROXY/HTTPS_PROXY/NO_PROXY env vars
// via http.ProxyFromEnvironment) — so users can also just set HTTPS_PROXY.
func initHTTPClient(proxyURL string) error {
	if proxyURL == "" {
		return nil // keep default transport (env-proxy aware)
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return fmt.Errorf("invalid --proxy %q: %w", proxyURL, err)
	}
	httpClient.Transport = &http.Transport{
		Proxy: http.ProxyURL(u),
		// Keep timeouts sane.
		DialContext:           (&net.Dialer{Timeout: 20 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ExpectContinueTimeout: 5 * time.Second,
	}
	return nil
}

// resilientGet mirrors Python check_verification.py resilient_get.
// Attempts maxAttempts times (default 8) with exponential backoff capped at 30s.
//   - HTTP error codes 429 / 5xx: retry through all attempts.
//   - Network-level errors (timeout, connection refused, etc.): fail fast after
//     2 consecutive failures (the source is likely completely unreachable — no
//     point in burning minutes on a full backoff chain the way check_verification.py
//     does for remote-only networks).
//
// Returns the raw *http.Response with body NOT YET READ, or error.
func resilientGet(url string, address, sourceName string, maxAttempts int) (*http.Response, error) {
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	var lastErr error
	netErrStreak := 0
	const fastFailNet = 2 // consecutive network errors to bail early
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("  [%s] retry %d/%d after %v (last: %v)", sourceName, attempt, maxAttempts-1, backoff, lastErr)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "nezha-ethscan-fetcher/1.0")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			netErrStreak++
			log.Printf("  [%s] 网络异常 %s (streak=%d): %v", sourceName, address, netErrStreak, err)
			if netErrStreak >= fastFailNet {
				log.Printf("  [%s] 连续 %d 次网络错误，提前放弃该源", sourceName, fastFailNet)
				break
			}
			continue
		}
		netErrStreak = 0
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("http %d", resp.StatusCode)
			resp.Body.Close()
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("request failed after %d attempts, last error: %w", maxAttempts, lastErr)
}

// ---------------------------------------------------------------------------
// RateLimiter (mirrors Python check_verification.py RateLimiter)
// ---------------------------------------------------------------------------

// RateLimiter mirrors Python check_verification.py RateLimiter.
// A *per-source* limiter; call .Wait() before each outbound request.
type RateLimiter struct {
	minInterval time.Duration
	lastCall    time.Time
	mu          sync.Mutex
}

func NewRateLimiter(rps float64) *RateLimiter {
	interval := 1.0 / rps
	return &RateLimiter{minInterval: time.Duration(interval * float64(time.Second))}
}

func (r *RateLimiter) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()
	elapsed := time.Since(r.lastCall)
	if elapsed < r.minInterval {
		time.Sleep(r.minInterval - elapsed)
	}
	r.lastCall = time.Now()
}

// ---------------------------------------------------------------------------
// Sourcify v2 API types and extraction
// ---------------------------------------------------------------------------

type sourcifyCompilation struct {
	Language         string          `json:"language"`
	CompilerVersion  string          `json:"compilerVersion"`
	Name             string          `json:"name"`
	CompilerSettings json.RawMessage `json:"compilerSettings"`
}

type sourcifySourceFile struct {
	Content string `json:"content"`
}

type sourcifyProxyResolution struct {
	ProxyType      string `json:"proxyType"`
	Implementation string `json:"implementation"`
}

type sourcifyResponse struct {
	Match           string                        `json:"match"`
	Compilation     sourcifyCompilation           `json:"compilation"`
	Sources         map[string]sourcifySourceFile `json:"sources"`
	ABI             json.RawMessage               `json:"abi"`
	StorageLayout   json.RawMessage               `json:"storageLayout"`
	ProxyResolution *sourcifyProxyResolution      `json:"proxyResolution"`
	StdJsonInput    json.RawMessage               `json:"stdJsonInput,omitempty"`
}

type sourcifyOptimizerSettings struct {
	Optimizer struct {
		Enabled bool `json:"enabled"`
		Runs    int  `json:"runs"`
	} `json:"optimizer"`
}

type sourcifyResultExtracted struct {
	SourceCode       string
	ABI              string
	ContractName     string
	CompilerVersion  string
	OptimizationUsed string
	Runs             string
	Proxy            string
	Implementation   string
	StorageLayout    string
}

type stdJsonInputSources struct {
	Sources map[string]sourcifySourceFile `json:"sources"`
}

// sourcifyFetch queries the Sourcify v2 API for a single contract address.
// Returns (extracted_data, is_unverified, error).
// If HTTP 404 or match is absent: is_unverified=true, err=nil.
// If match=="match" or "partial_match": returns extracted data.
func sourcifyFetch(baseURL, address string, rl *RateLimiter) (*sourcifyResultExtracted, bool, error) {
	rl.Wait()
	url := fmt.Sprintf("%s/%s?fields=all", strings.TrimRight(baseURL, "/"), address)
	resp, err := resilientGet(url, address, "sourcify", 8)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode == 404 {
		return nil, true, nil
	}
	if resp.StatusCode != 200 {
		return nil, false, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var sr sourcifyResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return nil, false, fmt.Errorf("parse sourcify json: %w", err)
	}

	if sr.Match != "match" && sr.Match != "partial_match" {
		return nil, true, nil
	}

	ext := &sourcifyResultExtracted{
		ContractName:    sr.Compilation.Name,
		CompilerVersion: sr.Compilation.CompilerVersion,
	}

	// --- Proxy / Implementation ---
	if sr.ProxyResolution != nil {
		ext.Proxy = sr.ProxyResolution.ProxyType
		ext.Implementation = sr.ProxyResolution.Implementation
	}

	// --- Sources ---
	sources := sr.Sources
	if len(sources) == 0 && len(sr.StdJsonInput) > 0 {
		var stdin stdJsonInputSources
		if err := json.Unmarshal(sr.StdJsonInput, &stdin); err == nil && len(stdin.Sources) > 0 {
			sources = stdin.Sources
		}
	}
	if len(sources) == 0 {
		return nil, false, fmt.Errorf("sourcify: no source files found in response")
	}
	filenames := make([]string, 0, len(sources))
	for name := range sources {
		filenames = append(filenames, name)
	}
	sort.Strings(filenames)
	var sb strings.Builder
	for _, name := range filenames {
		sb.WriteString(fmt.Sprintf("// === File: %s ===\n", name))
		sb.WriteString(sources[name].Content)
		sb.WriteString("\n")
	}
	ext.SourceCode = sb.String()

	// --- ABI ---
	if len(sr.ABI) > 0 {
		abiBytes, err := json.Marshal(json.RawMessage(sr.ABI))
		if err == nil {
			ext.ABI = string(abiBytes)
		}
	}

	// --- Storage layout ---
	if len(sr.StorageLayout) > 0 {
		var layoutMap map[string]interface{}
		if err := json.Unmarshal(sr.StorageLayout, &layoutMap); err == nil {
			if storageArr, ok := layoutMap["storage"].([]interface{}); ok && len(storageArr) > 0 {
				// sr.StorageLayout is already a json.RawMessage with the right shape.
				ext.StorageLayout = string(sr.StorageLayout)
			}
		}
	}

	// --- Optimization settings ---
	ext.OptimizationUsed = "0"
	if len(sr.Compilation.CompilerSettings) > 0 {
		var opt sourcifyOptimizerSettings
		if err := json.Unmarshal(sr.Compilation.CompilerSettings, &opt); err == nil {
			if opt.Optimizer.Enabled {
				ext.OptimizationUsed = "1"
			}
			if opt.Optimizer.Runs > 0 {
				ext.Runs = strconv.Itoa(opt.Optimizer.Runs)
			}
		}
	}

	return ext, false, nil
}

// ---------------------------------------------------------------------------
// Unified Etherscan-like fetcher (Etherscan + Blockscout share the shape)
// ---------------------------------------------------------------------------

// etherscanLikeFetch calls EITHER Etherscan OR Blockscout's getsourcecode endpoint
// (they share the response shape). On network/429/5xx error returns err.
// If the contract is simply unverified, returns (nil, "", true, nil).
// Returns (result, abiJSON, isUnverified, err).
func etherscanLikeFetch(baseURL, address, apiKey, sourceLabel string, rl *RateLimiter) (*etherscanSourceResult, string, bool, error) {
	rl.Wait()
	// Use & if baseURL already has a query string (e.g. Etherscan V2
	// "https://api.etherscan.io/v2/api?chainid=1"), otherwise start with ?.
	sep := "?"
	if strings.Contains(baseURL, "?") {
		sep = "&"
	}
	url := fmt.Sprintf("%s%smodule=contract&action=getsourcecode&address=%s", baseURL, sep, address)
	if apiKey != "" {
		url += "&apikey=" + apiKey
	}
	resp, err := resilientGet(url, address, sourceLabel, 8)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", false, err
	}
	if resp.StatusCode != 200 {
		return nil, "", false, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var esResp etherscanResponse
	if err := json.Unmarshal(body, &esResp); err != nil {
		return nil, "", false, fmt.Errorf("parse json: %w", err)
	}
	objs, strMsg := extractResults(esResp.Result)
	abiNotVerified := len(objs) > 0 && objs[0].ABI == "Contract source code not verified"
	unverifiedMsg := strings.Contains(strMsg, "Contract source code not verified")
	if esResp.Status == "1" && len(objs) > 0 && isVerified(objs[0]) {
		return &objs[0], objs[0].ABI, false, nil
	}
	if unverifiedMsg || abiNotVerified {
		return nil, "", true, nil
	}
	// Status=="0" with a benign NOTOK message (e.g. "No cases found") — treat as unverified
	if esResp.Status == "0" && strings.Contains(esResp.Message, "NOTOK") == false && strings.Contains(strings.ToLower(esResp.Message), "invalid") == false {
		return nil, "", true, nil
	}
	return nil, "", false, fmt.Errorf("API error status=%s msg=%s", esResp.Status, esResp.Message)
}

// ---------------------------------------------------------------------------
// sourceFetchResult: unified return value for each source's fetch function
// ---------------------------------------------------------------------------

type sourceFetchResult struct {
	label      string
	esr        *etherscanSourceResult
	ext        *sourcifyResultExtracted
	unverified bool
	netErr     bool
	err        error
}

// isNetworkError heuristically reports whether an error from a fetch function
// indicates "endpoint unreachable from this network" as opposed to a semantic
// API error (NOTOK, parse failure, etc.). Matches on context deadline/timeouts,
// connection refused, DNS errors, "no such host", and similar transport errors.
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, tok := range []string{
		"context deadline exceeded",
		"timeout",
		"connection refused",
		"no such host",
		"dns",
		"dial tcp",
		"connection reset by peer",
		"http: nil resp",
	} {
		if strings.Contains(msg, tok) {
			return true
		}
	}
	// error wrapping: walk via errors.As for timeout types
	var netErr interface {
		Timeout() bool
	}
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// main + CLI flags
// ---------------------------------------------------------------------------

func main() {
	var (
		csvPath       string
		etherscanKey  string
		outDir        string
		top           int
		etherscanURL  string
		blockscoutURL string
		sourcifyURL   string
		rps           float64
		proxyURL      string
	)
	flag.StringVar(&csvPath, "csv", "scripts/datasets/contracts_10blocks.csv", "Path to contracts CSV")
	flag.StringVar(&etherscanKey, "apikey", "", "Etherscan API key (optional; if empty, Etherscan source is skipped)")
	flag.StringVar(&outDir, "outdir", "cache/mainnet_rw", "Output directory")
	flag.IntVar(&top, "top", 20, "Number of top contracts to fetch (0 = all)")
	flag.StringVar(&etherscanURL, "api-url", "https://api.etherscan.io/v2/api?chainid=1", "Etherscan API V2 base URL (chainid=1 for mainnet)")
	flag.StringVar(&blockscoutURL, "blockscout-url", "https://eth.blockscout.com/api", "Blockscout API base URL")
	flag.StringVar(&sourcifyURL, "sourcify-url", "https://sourcify.dev/server/v2/contract/1", "Sourcify v2 API base URL (chain-id 1 prefix)")
	flag.Float64Var(&rps, "rps", 5.0, "Requests per second for each source rate limiter")
	flag.StringVar(&proxyURL, "proxy", "", "HTTP/HTTPS proxy URL for all sources (e.g. http://127.0.0.1:7890). Empty = use HTTPS_PROXY env var or direct.")
	flag.Parse()

	if err := initHTTPClient(proxyURL); err != nil {
		log.Fatalf("init http client: %v", err)
	}
	if proxyURL != "" {
		log.Printf("INFO: using explicit proxy %s for all sources", proxyURL)
	} else if os.Getenv("HTTPS_PROXY") != "" || os.Getenv("HTTP_PROXY") != "" {
		log.Printf("INFO: using proxy from HTTPS_PROXY/HTTP_PROXY env var")
	}

	if etherscanKey == "" {
		log.Printf("WARNING: --apikey not provided; Etherscan source will be skipped (will use Sourcify -> Blockscout)")
	} else {
		log.Printf("INFO: --apikey provided; source order Sourcify -> Blockscout -> Etherscan")
	}

	rows, err := readCSV(csvPath, top)
	if err != nil {
		log.Fatalf("Read CSV: %v", err)
	}
	if len(rows) == 0 {
		log.Fatal("No contract rows to process (is_contract=1 AND call_count>0)")
	}

	log.Printf("Loaded %d contracts from %s (outdir=%s, rps=%.1f)", len(rows), csvPath, outDir, rps)

	rls := map[string]*RateLimiter{
		"etherscan":  NewRateLimiter(rps),
		"blockscout": NewRateLimiter(rps),
		"sourcify":   NewRateLimiter(rps),
	}

	// Track consecutive per-source NETWORK failures across contracts. Once a
	// source hits 2 consecutive contract-level network failures, the source is
	// disabled for the rest of the run (avoids burning minutes retrying an
	// endpoint that is fully unreachable from this network).
	sourceConsecNetFails := map[string]int{}
	sourceDisabled := map[string]bool{}
	const sourceNetFailThreshold = 2

	var processed []processedContract

	for i, row := range rows {
		address := strings.ToLower(strings.TrimSpace(row.Address))
		log.Printf("[%d/%d] Fetching %s (call_count=%d)...", i+1, len(rows), address, row.CallCount)

		pc, hadNetFail := processContract(outDir, address, etherscanURL, etherscanKey, blockscoutURL, sourcifyURL, rls, sourceDisabled)
		processed = append(processed, pc)

		// Update global per-source disable counters.
		for _, name := range []string{"sourcify", "blockscout", "etherscan"} {
			if !hadNetFail[name] {
				sourceConsecNetFails[name] = 0
				continue
			}
			sourceConsecNetFails[name]++
			if sourceConsecNetFails[name] >= sourceNetFailThreshold && !sourceDisabled[name] {
				log.Printf("源 %s 已连续 %d 笔出现网络错误，本轮剩余合约禁用该源", name, sourceConsecNetFails[name])
				sourceDisabled[name] = true
			}
		}
	}

	printSummary(processed, len(rows))
}

// ---------------------------------------------------------------------------
// processContract: multi-source waterfall Sourcify -> Blockscout -> Etherscan
// Sourcify is first because (as of today) it is the ONLY reachable source from
// the user's network, and it doesn't need an API key. Fallbacks keep the same
// getSourcecode JSON shape for the latter two; any source error drops to next.
// ---------------------------------------------------------------------------

func processContract(
	outDir, address, etherscanURL, etherscanKey, blockscoutURL, sourcifyURL string,
	rls map[string]*RateLimiter, sourceDisabled map[string]bool,
) (processedContract, map[string]bool) {
	pc := processedContract{address: address, status: statusFailed}
	// hadNetFail tracks which sources returned a network-level error (not an
	// "unverified" answer) in the top-level waterfall. Used by main() to
	// disable unreachable sources after a threshold.
	hadNetFail := map[string]bool{}

	// Build the ordered list of source attempts: SOURCIFY FIRST.
	var sources []struct {
		name string
		fn   func() sourceFetchResult
	}

	// 1) Sourcify (no API key; fastest reachable source from the user's network)
	if !sourceDisabled["sourcify"] {
		sources = append(sources, struct {
			name string
			fn   func() sourceFetchResult
		}{
			name: "sourcify",
			fn: func() sourceFetchResult {
				ext, unverif, err := sourcifyFetch(sourcifyURL, address, rls["sourcify"])
				if err != nil {
					return sourceFetchResult{label: "sourcify", err: err, netErr: isNetworkError(err)}
				}
				if unverif {
					return sourceFetchResult{label: "sourcify", unverified: true}
				}
				return sourceFetchResult{label: "sourcify", ext: ext}
			},
		})
	}

	// 2) Blockscout (no API key; same getsourcecode JSON shape as Etherscan)
	if !sourceDisabled["blockscout"] {
		sources = append(sources, struct {
			name string
			fn   func() sourceFetchResult
		}{
			name: "blockscout",
			fn: func() sourceFetchResult {
				esr, _, unverif, err := etherscanLikeFetch(blockscoutURL, address, "", "blockscout", rls["blockscout"])
				if err != nil {
					return sourceFetchResult{label: "blockscout", err: err, netErr: isNetworkError(err)}
				}
				if unverif {
					return sourceFetchResult{label: "blockscout", unverified: true}
				}
				return sourceFetchResult{label: "blockscout", esr: esr}
			},
		})
	}

	// 3) Etherscan (skipped if no API key or disabled by session-level ban)
	if etherscanKey != "" && !sourceDisabled["etherscan"] {
		sources = append(sources, struct {
			name string
			fn   func() sourceFetchResult
		}{
			name: "etherscan",
			fn: func() sourceFetchResult {
				esr, _, unverif, err := etherscanLikeFetch(etherscanURL, address, etherscanKey, "etherscan", rls["etherscan"])
				if err != nil {
					return sourceFetchResult{label: "etherscan", err: err, netErr: isNetworkError(err)}
				}
				if unverif {
					return sourceFetchResult{label: "etherscan", unverified: true}
				}
				return sourceFetchResult{label: "etherscan", esr: esr}
			},
		})
	}

	// Fallback loop: try each source in order.
	anyUnverified := false

	for _, src := range sources {
		res := src.fn()
		if res.err != nil {
			log.Printf("  [%s] ERROR: %v — fallback to next source", res.label, res.err)
			if res.netErr {
				hadNetFail[res.label] = true
			}
			continue
		}
		if res.unverified {
			log.Printf("  [%s] unverified — continue to next source", res.label)
			anyUnverified = true
			continue
		}
		// VERIFIED from this source
		if res.esr != nil {
			pc.status = statusVerified
			pc.contractName = res.esr.ContractName
			pc.compilerVer = res.esr.CompilerVersion
			pc.source = res.label
			writeVerifiedContract(outDir, address, *res.esr, &pc, res.label)
			return pc, hadNetFail
		}
		if res.ext != nil {
			pc.status = statusVerified
			pc.contractName = res.ext.ContractName
			pc.compilerVer = res.ext.CompilerVersion
			pc.source = res.label
			esr2 := etherscanSourceResult{
				SourceCode:       res.ext.SourceCode,
				ABI:              res.ext.ABI,
				ContractName:     res.ext.ContractName,
				CompilerVersion:  res.ext.CompilerVersion,
				OptimizationUsed: res.ext.OptimizationUsed,
				Runs:             res.ext.Runs,
				Proxy:            res.ext.Proxy,
				Implementation:   res.ext.Implementation,
				StorageLayout:    res.ext.StorageLayout,
			}
			writeVerifiedContract(outDir, address, esr2, &pc, res.label)
			return pc, hadNetFail
		}
	}

	// No source returned verified.
	if anyUnverified {
		pc.status = statusUnverified
		if err := writeUnverifiedMeta(outDir, address); err != nil {
			log.Printf("  ERROR writing unverified meta for %s: %v", address, err)
			pc.status = statusFailed
		} else {
			log.Printf("  unverified (all sources reported unverified)")
		}
	} else {
		log.Printf("  FAILED: all sources errored for %s", address)
	}
	return pc, hadNetFail
}

// ---------------------------------------------------------------------------
// Write helpers
// ---------------------------------------------------------------------------

func writeVerifiedContract(outDir, address string, res etherscanSourceResult, pc *processedContract, source string) {
	dir := filepath.Join(outDir, address)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("  ERROR mkdir %s: %v", dir, err)
		pc.status = statusFailed
		return
	}

	// source.sol — concatenated plain Solidity (multiple files joined with comment headers).
	if err := os.WriteFile(filepath.Join(dir, "source.sol"), []byte(res.SourceCode), 0o644); err != nil {
		log.Printf("  ERROR write source.sol: %v", err)
	}

	// abi.json — pretty-printed ABI array.
	var abiArr []json.RawMessage
	if err := json.Unmarshal([]byte(res.ABI), &abiArr); err != nil {
		log.Printf("  WARNING: ABI not valid JSON for %s: %v", address, err)
	} else if pretty, err := json.MarshalIndent(abiArr, "", "  "); err == nil {
		os.WriteFile(filepath.Join(dir, "abi.json"), pretty, 0o644)
	}

	// funcs.json — selector -> function mapping computed from the ABI.
	funcs, err := computeFuncSelectors(res.ABI)
	if err != nil {
		log.Printf("  WARNING: compute funcs for %s: %v", address, err)
	} else if pretty, err := json.MarshalIndent(funcs, "", "  "); err == nil {
		os.WriteFile(filepath.Join(dir, "funcs.json"), pretty, 0o644)
	}

	// storage.json — only when a non-empty, valid storage layout is present.
	// Format is compatible with utils.parseSolcStorageLayout: {"storage":[...],"types":{...}}
	if strings.TrimSpace(res.StorageLayout) != "" {
		var layout interface{}
		if err := json.Unmarshal([]byte(res.StorageLayout), &layout); err != nil {
			log.Printf("  WARNING: storage layout not valid JSON for %s: %v", address, err)
		} else if pretty, err := json.MarshalIndent(layout, "", "  "); err == nil {
			if err := os.WriteFile(filepath.Join(dir, "storage.json"), pretty, 0o644); err == nil {
				pc.hasStorage = true
			}
		}
	}

	// meta.json — metadata summary (NEW: includes source field).
	meta := metaInfo{
		Address:          address,
		ContractName:     res.ContractName,
		CompilerVersion:  res.CompilerVersion,
		Verified:         true,
		Proxy:            res.Proxy,
		Implementation:   res.Implementation,
		OptimizationUsed: res.OptimizationUsed,
		Runs:             res.Runs,
		Source:           source,
	}
	writeJSONFile(filepath.Join(dir, "meta.json"), meta)

	log.Printf("  [%s] verified: %s (%s) storage=%v", source, res.ContractName, res.CompilerVersion, pc.hasStorage)
}

func writeUnverifiedMeta(outDir, address string) error {
	dir := filepath.Join(outDir, address)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(dir, "meta.json"), metaInfo{Address: address, Verified: false, Source: "none"})
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

// readCSV parses the contracts CSV, keeps rows where is_contract=1 AND
// call_count>0, sorts by call_count descending, and returns the top N.
func readCSV(path string, top int) ([]contractRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // tolerate variable column counts

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := make(map[string]int)
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for _, col := range []string{"address", "is_contract", "call_count"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("csv missing required column: %s", col)
		}
	}

	var rows []contractRow
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read csv row: %w", err)
		}
		if len(record) <= idx["call_count"] {
			continue
		}

		isContract, _ := strconv.Atoi(strings.TrimSpace(record[idx["is_contract"]]))
		callCount, _ := strconv.ParseUint(strings.TrimSpace(record[idx["call_count"]]), 10, 64)
		if isContract != 1 || callCount == 0 {
			continue
		}

		rows = append(rows, contractRow{
			Address:    strings.TrimSpace(record[idx["address"]]),
			IsContract: isContract,
			CallCount:  callCount,
		})
	}

	// Defensive sort (CSV is documented as already sorted by call_count desc).
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].CallCount > rows[j].CallCount
	})

	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// Etherscan result parsing helpers (kept unchanged)
// ---------------------------------------------------------------------------

// extractResults parses the raw Etherscan result field into either an array of
// source-result objects or, when Etherscan returned a plain string array (e.g.
// ["Contract source code not verified"]), the first string message.
func extractResults(raw json.RawMessage) ([]etherscanSourceResult, string) {
	var objs []etherscanSourceResult
	if err := json.Unmarshal(raw, &objs); err == nil {
		return objs, ""
	}
	var strs []string
	if err := json.Unmarshal(raw, &strs); err == nil {
		if len(strs) > 0 {
			return nil, strs[0]
		}
		return nil, ""
	}
	return nil, string(raw)
}

// isVerified reports whether an Etherscan result represents a verified contract
// with a usable ABI.
func isVerified(r etherscanSourceResult) bool {
	if r.ABI == "" || r.ABI == "Contract source code not verified" {
		return false
	}
	return json.Valid([]byte(r.ABI))
}

// ---------------------------------------------------------------------------
// Function selector computation (kept unchanged)
// ---------------------------------------------------------------------------

// computeFuncSelectors builds the selector->function mapping from the ABI JSON
// string. Only entries of type "function" (or with type omitted, which defaults
// to "function") are included. Each selector is the first 4 bytes of
// keccak256("name(type1,type2,...)").
func computeFuncSelectors(abiJSON string) ([]funcEntry, error) {
	var items []abiItem
	if err := json.Unmarshal([]byte(abiJSON), &items); err != nil {
		return nil, err
	}
	funcs := make([]funcEntry, 0)
	for _, item := range items {
		if item.Type != "function" && item.Type != "" {
			continue
		}
		if item.Name == "" {
			continue // constructor / fallback / receive have no name
		}
		types := make([]string, len(item.Inputs))
		for i, in := range item.Inputs {
			types[i] = in.Type
		}
		sig := fmt.Sprintf("%s(%s)", item.Name, strings.Join(types, ","))
		inputs := item.Inputs
		if inputs == nil {
			inputs = []abiInput{}
		}
		funcs = append(funcs, funcEntry{
			Selector: selectorFromSignature(sig),
			Name:     item.Name,
			Inputs:   inputs,
		})
	}
	return funcs, nil
}

// selectorFromSignature returns the 4-byte function selector as "0x........".
func selectorFromSignature(sig string) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(sig))
	return "0x" + hex.EncodeToString(h.Sum(nil)[:4])
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

func writeJSONFile(path string, v interface{}) error {
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, pretty, 0o644)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func shortAddr(addr string) string {
	if len(addr) > 7 {
		return addr[:7] + "..."
	}
	return addr
}

func printSummary(processed []processedContract, total int) {
	countVerified := 0
	countUnverified := 0
	countFailed := 0
	countStorage := 0
	bySource := make(map[string]int)

	for _, pc := range processed {
		switch pc.status {
		case statusVerified:
			countVerified++
			if pc.hasStorage {
				countStorage++
			}
			if pc.source != "" {
				bySource[pc.source]++
			}
		case statusUnverified:
			countUnverified++
		case statusFailed:
			countFailed++
		}
	}

	fmt.Println()
	fmt.Println("======= ETHSCAN-FETCHER SUMMARY =======")
	fmt.Printf("%-25s : %d\n", "Total addresses processed", total)
	fmt.Printf("%-25s : %d\n", "Verified contracts", countVerified)
	fmt.Printf("%-25s : %d\n", "Unverified contracts", countUnverified)
	fmt.Printf("%-25s : %d\n", "Failed (API error)", countFailed)
	fmt.Printf("%-25s : %d\n", "With storage layout", countStorage)

	if len(bySource) > 0 {
		fmt.Println()
		fmt.Println("Verified by source:")
		keys := make([]string, 0, len(bySource))
		for k := range bySource {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %-15s : %d\n", k, bySource[k])
		}
	}

	fmt.Println()
	fmt.Println("Verified contracts:")
	for _, pc := range processed {
		if pc.status != statusVerified {
			continue
		}
		storage := "no"
		if pc.hasStorage {
			storage = "yes"
		}
		src := pc.source
		if src == "" {
			src = "unknown"
		}
		fmt.Printf("  %s  %-20s (%s)  source=%-10s  storage=%s\n", shortAddr(pc.address), pc.contractName, pc.compilerVer, src, storage)
	}

	fmt.Println()
	fmt.Println("Unverified contracts:")
	for _, pc := range processed {
		if pc.status == statusUnverified {
			fmt.Printf("  %s\n", pc.address)
		}
	}

	if countFailed > 0 {
		fmt.Println()
		fmt.Println("Failed contracts:")
		for _, pc := range processed {
			if pc.status == statusFailed {
				fmt.Printf("  %s\n", pc.address)
			}
		}
	}
}
