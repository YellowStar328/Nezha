package utils

import (
	"encoding/hex"
	"strings"
	"testing"
)

// TestDerivedKeyDeriversAgainstAbortLog drives the PRODUCTION derivers with
// the real block-24000000 calldata and asserts the derived keys exactly match
// the missing keys recorded in llm_abort_diff.log for the 7 key-exceed aborts.
func TestDerivedKeyDeriversAgainstAbortLog(t *testing.T) {
	dr, err := NewDatasetReader("../scripts/datasets/test-24000000-24000009")
	if err != nil {
		t.Skipf("dataset not available: %v", err)
	}
	txs, err := dr.LoadBlockTxs(24000000)
	if err != nil {
		t.Fatalf("load block: %v", err)
	}

	type expect struct {
		txIdx       int
		account     string
		field       string
		missingKeys []string
	}
	cases := []expect{
		{txIdx: 176, account: "msg.sender", field: "map", missingKeys: []string{
			"slot:0x0de8bf93da2f7eecb3d9169422413a9bef4ef628:0x6a4c6b1df6bd89027ccfaff976b2445bd785674dfc3afbaaeba5957248d19a73",
		}},
		{txIdx: 187, account: "msg.sender", field: "map", missingKeys: []string{
			"slot:0x0de8bf93da2f7eecb3d9169422413a9bef4ef628:0x3cf79b4fb2695bf69eae32a73921697251cb4e94f8974f3b0b8594b83e647dde",
		}},
		{txIdx: 207, account: "global", field: "usedNonces", missingKeys: []string{
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0x450b372f2428f69e5a4294ff6457c3565870fae5ae9d2bd53c9de9a3d62a9c07",
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0xe02b1e83e1e04adbc50a55e8ca8dcb102afb9a3d974fcc42100380e5f67d8b30",
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0xee01226252351611ebf84fc893d9de7477a97e335dd65a37f0ada02ea12ffce3",
		}},
		{txIdx: 209, account: "global", field: "usedNonces", missingKeys: []string{
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0x450b372f2428f69e5a4294ff6457c3565870fae5ae9d2bd53c9de9a3d62a9c07",
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0xe02b1e83e1e04adbc50a55e8ca8dcb102afb9a3d974fcc42100380e5f67d8b30",
			"slot:0x0a992d191deec32afe36203ad87d7d289a738f81:0xee01226252351611ebf84fc893d9de7477a97e335dd65a37f0ada02ea12ffce3",
		}},
	}

	for _, c := range cases {
		tx := txs[c.txIdx]
		raw, err := hex.DecodeString(strings.TrimPrefix(tx.Input, "0x"))
		if err != nil {
			t.Fatalf("tx %d: bad input hex: %v", c.txIdx, err)
		}
		args := MainnetTxArgs{
			MsgSender:   strings.ToLower(tx.From),
			Selector:    strings.ToLower(tx.Input[0:10]),
			RawCalldata: raw,
		}
		acc := LLMFieldAccess{Account: c.account, Field: c.field}
		to := strings.ToLower(tx.To)

		deriver, ok := derivedKeyDerivers[to+":"+strings.ToLower(args.Selector)]
		if !ok {
			t.Fatalf("tx %d: no deriver registered for %s:%s", c.txIdx, tx.To, args.Selector)
		}
		dk, ok := deriver(to, acc, args)
		if !ok {
			t.Fatalf("tx %d: deriver returned not-ok", c.txIdx)
		}
		got := append(append([]string{}, dk.ReadKeys...), dk.WriteKeys...)
		t.Logf("tx %d (%s:%s) derived %d keys: %v", c.txIdx, tx.To, args.Selector, len(got), got)
		gotSet := map[string]bool{}
		for _, k := range got {
			gotSet[k] = true
		}
		for _, mk := range c.missingKeys {
			if !gotSet[mk] {
				t.Errorf("tx %d: derived keys MISSING %s", c.txIdx, mk)
			}
		}
	}
}
