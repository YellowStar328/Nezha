package exporter

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

type HexOrNum string

func (h *HexOrNum) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, (*string)(h))
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		if val, err := n.Int64(); err == nil {
			*h = HexOrNum(fmt.Sprintf("0x%x", val))
			return nil
		}
		*h = HexOrNum(n.String())
		return nil
	}
	return fmt.Errorf("cannot unmarshal into HexOrNum: %s", string(data))
}

type PrestateTracerResult struct {
	Accounts map[string]*PrestateAccount `json:"accounts"`
	DiffMode bool                        `json:"-"`
	Added    map[string]*PrestateAccount `json:"-"`
	Removed  map[string]*PrestateAccount `json:"-"`
}

type PrestateAccount struct {
	Balance  HexOrNum          `json:"balance"`
	Nonce    HexOrNum          `json:"nonce"`
	Code     string            `json:"code"`
	Storage  map[string]string `json:"storage"`
	CodeHash string            `json:"codeHash"`
}

type DiffTracerResult struct {
	OldState map[string]*PrestateAccount `json:"-"`
	NewState map[string]*PrestateAccount `json:"-"`
}

func ParsePrestateTracerResult(rawJSON json.RawMessage, diffMode bool) (*PrestateTracerResult, error) {
	// diffMode response: {"pre": {addr: acct}, "post": {addr: acct}}
	if diffMode {
		var wrapper struct {
			Pre  map[string]*PrestateAccount `json:"pre"`
			Post map[string]*PrestateAccount `json:"post"`
		}
		if err := json.Unmarshal(rawJSON, &wrapper); err != nil {
			return nil, fmt.Errorf("unmarshal diffMode prestate tracer result: %w", err)
		}
		return &PrestateTracerResult{
			Accounts: wrapper.Pre,  // pre-state
			Added:    wrapper.Post, // post-state (final values after tx)
			DiffMode: true,
		}, nil
	}

	// default mode response: {addr: acct} (flat map of pre-state)
	var accounts map[string]*PrestateAccount
	if err := json.Unmarshal(rawJSON, &accounts); err != nil {
		return nil, fmt.Errorf("unmarshal prestate tracer result: %w", err)
	}
	return &PrestateTracerResult{
		Accounts: accounts,
		DiffMode: false,
	}, nil
}

func (r *PrestateTracerResult) GetAccounts() map[string]*PrestateAccount {
	if r.DiffMode {
		merged := make(map[string]*PrestateAccount)
		for addr, acct := range r.Accounts {
			merged[addr] = acct
		}
		for addr, acct := range r.Added {
			merged[addr] = acct
		}
		for addr, acct := range r.Removed {
			merged[addr] = acct
		}
		return merged
	}
	return r.Accounts
}

func AcctKey(addr string, field string) string {
	addr = strings.ToLower(addr)
	if !strings.HasPrefix(addr, "0x") {
		addr = "0x" + addr
	}
	return fmt.Sprintf("acct:%s:%s", addr, field)
}

func SlotKey(addr string, slot string) string {
	addr = strings.ToLower(addr)
	slot = strings.ToLower(slot)
	if !strings.HasPrefix(addr, "0x") {
		addr = "0x" + addr
	}
	if !strings.HasPrefix(slot, "0x") {
		slot = "0x" + slot
	}
	return fmt.Sprintf("slot:%s:%s", addr, slot)
}

func ParseKey(key string) (kind string, addr string, field string, err error) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("invalid key format: %s", key)
	}
	return parts[0], parts[1], parts[2], nil
}

func HexToBigInt(hex string) *big.Int {
	v := new(big.Int)
	v.SetString(hex, 0)
	return v
}

func BigIntToHex(v *big.Int) string {
	if v == nil {
		return "0x0"
	}
	return fmt.Sprintf("0x%x", v)
}
