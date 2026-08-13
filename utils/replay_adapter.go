package utils

import (
	"Nezha/core"
)

// ReplayContractName is the synthetic contract name assigned to every
// RWNode produced from an Ethereum mainnet replay. The existing schedulers
// (ClassicalGraph / Depurge) use RWNode.CompositeKey() which prepends the
// contract name to avoid cross-contract false conflicts; since Ethereum
// RW keys are already globally unique (addresses + slots), we use a fixed
// namespace so the existing CompositeKey() collision logic works correctly.
const ReplayContractName = "eth"

// RWSetsToRWNodes converts an ordered slice of speculative ReplayRWSet into
// the shape required by ClassicalGraph / Depurge: `txs [][]*RWNode`.
//
// It produces one dummy placeholder value (`[]byte{}`) per key because the
// existing schedulers only need **key identity** for conflict detection,
// not values. Pre/Post values are irrelevant to DAG edge construction.
func RWSetsToRWNodes(specs []*core.ReplayRWSet) [][]*core.RWNode {
	txs := make([][]*core.RWNode, len(specs))
	for i, spec := range specs {
		if spec == nil {
			txs[i] = nil
			continue
		}
		ref := spec.Ref
		nodes := make([]*core.RWNode, 0, len(spec.ReadKeys)+len(spec.WriteKeys))

		transInfo := core.TransInfo{
			ID:        specID(ref),
			Timestamp: uint32(ref.BlockNum),
		}

		// Reads (Label="r"). Dedup across read+write by letting read come first.
		seen := make(map[string]bool, len(spec.ReadKeys)+len(spec.WriteKeys))
		for _, k := range spec.ReadKeys {
			if seen[k] {
				continue
			}
			seen[k] = true
			nodes = append(nodes, &core.RWNode{
				RWSet:        core.RWSet{Key: []byte(k), Value: []byte{}},
				TransInfo:    transInfo,
				Label:        "r",
				ContractName: ReplayContractName,
			})
		}
		// Writes (Label="w").
		for _, k := range spec.WriteKeys {
			if seen[k] {
				// A key that's both read and written is tracked as Label="w".
				// Rewrite label on existing match, dedup write.
				for idx := range nodes {
					if string(nodes[idx].RWSet.Key) == k && nodes[idx].Label == "r" {
						nodes[idx].Label = "w"
						break
					}
				}
				continue
			}
			seen[k] = true
			nodes = append(nodes, &core.RWNode{
				RWSet:        core.RWSet{Key: []byte(k), Value: []byte{}},
				TransInfo:    transInfo,
				Label:        "w",
				ContractName: ReplayContractName,
			})
		}
		txs[i] = nodes
	}
	return txs
}

// CanonicalToKeySet flattens a CanonicalRWSet into (readSet, writeSet) maps
// for O(1) membership lookups during Validate.
func CanonicalToKeySet(c *core.CanonicalRWSet) (map[string]bool, map[string]bool) {
	rs := make(map[string]bool, len(c.ReadKeys))
	ws := make(map[string]bool, len(c.WriteKeys))
	for _, k := range c.ReadKeys {
		rs[k] = true
	}
	for _, k := range c.WriteKeys {
		ws[k] = true
	}
	return rs, ws
}

// SpecToKeySet mirrors CanonicalToKeySet for speculative outputs.
func SpecToKeySet(s *core.ReplayRWSet) (map[string]bool, map[string]bool) {
	rs := make(map[string]bool, len(s.ReadKeys))
	ws := make(map[string]bool, len(s.WriteKeys))
	for _, k := range s.ReadKeys {
		rs[k] = true
	}
	for _, k := range s.WriteKeys {
		ws[k] = true
	}
	return rs, ws
}

// specID returns a stable string identifier for a tx reference, used as
// RWNode.TransInfo.ID so schedulers can map graph nodes back to txs.
func specID(r core.ReplayRef) string {
	return r.TxHash
}
