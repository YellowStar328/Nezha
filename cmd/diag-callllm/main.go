package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func truncateStr(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func main() {
	addr := "0xdac17f958d2ee523a2206206994597c13d831ec7"
	selector := "0xa9059cbb"
	if len(os.Args) >= 3 {
		addr = os.Args[1]
		selector = os.Args[2]
	}

	endpoint := os.Getenv("LLM_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://developer.amd.com.cn/radeon/api/v1/chat/completions"
	}
	apiKey := os.Getenv("LLM_APIKEY")
	if apiKey == "" {
		apiKey = "rc-e8fe1d82122e04c4fd1d6de8dd79ab2a9d9f6a441565df73"
	}
	model := os.Getenv("LLM_MODEL")
	if model == "" {
		model = "DeepSeek-V4-Flash"
	}

	prompt := buildPromptMirror(addr, selector)
	fmt.Printf("=== prompt length: %d bytes (%d runes) ===\n", len(prompt), len([]rune(prompt)))
	if len(prompt) > 0 {
		head := prompt
		if len(head) > 500 {
			head = head[:500]
		}
		fmt.Printf("---prompt HEAD---\n%s\n", head)
		tail := prompt
		if len(tail) > 300 {
			tail = tail[len(tail)-300:]
		}
		fmt.Printf("---prompt TAIL---\n%s\n---END---\n", tail)
	}

	reqBody := map[string]interface{}{
		"model":       model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"temperature": 0.0,
		"max_tokens":  5000,
	}
	rb, _ := json.Marshal(reqBody)
	fmt.Printf("=== request body JSON size: %d bytes ===\n", len(rb))

	req, _ := http.NewRequest("POST", endpoint, strings.NewReader(string(rb)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 180 * time.Second}
	t0 := time.Now()
	resp, err := client.Do(req)
	dt := time.Since(t0)
	if err != nil {
		fmt.Printf("client.Do error: %v (after %v)\n", err, dt)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("=== HTTP %d (in %v) ===\n", resp.StatusCode, dt)
	fmt.Printf("Response headers:\n")
	for k, vs := range resp.Header {
		fmt.Printf("  %s: %s\n", k, strings.Join(vs, ", "))
	}
	fmt.Printf("--- response body head 800 bytes ---\n")
	head := body
	if len(head) > 800 {
		head = head[:800]
	}
	fmt.Printf("%s\n", head)
	if len(body) > 800 {
		fmt.Printf("... [total body %d bytes; tail 300]\n", len(body))
		fmt.Printf("%s\n", body[len(body)-300:])
	}
}

func buildPromptMirror(addr, selector string) string {
	baseDir := "cache/mainnet_rw/" + addr
	readFile := func(name string) string {
		b, err := os.ReadFile(baseDir + "/" + name)
		if err != nil {
			return ""
		}
		return string(b)
	}
	source := readFile("source.sol")
	abi := readFile("abi.json")
	storage := readFile("storage.json")
	funcs := readFile("funcs.json")
	meta := readFile("meta.json")

	var sb strings.Builder
	sb.WriteString("# Solidity Contract Static Analysis Prompt (Mainnet Mirror)\n\n")
	sb.WriteString("Contract address: ")
	sb.WriteString(addr)
	sb.WriteString("\nFunction selector: ")
	sb.WriteString(selector)
	sb.WriteString("\n\n")
	if meta != "" {
		sb.WriteString("## meta.json\n```json\n")
		sb.WriteString(truncateStr(meta, 4000))
		sb.WriteString("\n```\n\n")
	}
	if funcs != "" {
		sb.WriteString("## funcs.json (selector → function)\n```json\n")
		sb.WriteString(truncateStr(funcs, 4000))
		sb.WriteString("\n```\n\n")
	}
	if storage != "" {
		sb.WriteString("## storage.json (layout)\n```json\n")
		sb.WriteString(truncateStr(storage, 30000))
		sb.WriteString("\n```\n\n")
	}
	if abi != "" {
		sb.WriteString("## abi.json\n```json\n")
		sb.WriteString(truncateStr(abi, 30000))
		sb.WriteString("\n```\n\n")
	}
	if source != "" {
		sb.WriteString("## source.sol\n```solidity\n")
		sb.WriteString(truncateStr(source, 200000))
		sb.WriteString("\n```\n\n")
	}
	sb.WriteString("\nPlease analyze the Solidity contract above and determine the conservative\n")
	sb.WriteString("read/write storage keys and cross-contract calls for the function matching\n")
	sb.WriteString("the selector. Respond ONLY with a valid JSON object in the exact shape:\n\n")
	sb.WriteString("{\n")
	sb.WriteString("  \"reads\":  [\"slotName or slotName[keyExpr]\", \"...\"],\n")
	sb.WriteString("  \"writes\": [\"slotName or slotName[keyExpr]\", \"...\"],\n")
	sb.WriteString("  \"crossCalls\": [{\"contract\":\"<addr or alias>\",\"function\":\"<selector or signature>\",\"args\":[\"<expr>\",\"...\"],\"value\":\"<expr>\"}]\n")
	sb.WriteString("}\n\nRules:\n- Keys are conservative superset.\n")
	sb.WriteString("- Global state reads must appear in reads; writes must appear in writes.\n")
	sb.WriteString("- If a write target depends on runtime value, enumerate all plausible keys.\n")
	sb.WriteString("- No markdown, no prose outside the JSON object.\n")
	return sb.String()
}
