package utils

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/btcsuite/btcd/btcec"
)

// DerivedKey holds the concrete conservative keys produced by a calldata-
// derived key deriver. ReadKeys are added to the read set, WriteKeys to the
// write set — matching the direction of the LLM access being converted.
type DerivedKey struct {
	ReadKeys  []string
	WriteKeys []string
}

// KeyDeriver converts a single abstract LLM access into concrete slot keys by
// parsing the transaction calldata. It returns (nil, false) when the access is
// not one it can handle or the calldata cannot be parsed. Because a registered
// deriver is the ONLY way such accesses are resolved, a failure there must
// mark the whole tx as unresolved (EVM PreExecute fallback) — never silently
// drop the key.
type KeyDeriver func(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs) (*DerivedKey, bool)

// derivedKeyDerivers maps "addr:selector" (all lowercase) to a calldata-
// derived key deriver. New contracts only need a registration here — the core
// loop in mainnet_contract_mgr.go stays untouched.
var derivedKeyDerivers = map[string]KeyDeriver{
	// MessageTransmitter.receiveMessage: usedNonces/enabledAttesters keys are
	// derived from the message + attestation calldata params.
	"0x0a992d191deec32afe36203ad87d7d289a738f81:0x57ecfd28": deriveReceiveMessageKeys,
	// CoinTool_App.t: map[msg.sender][_salt] — the nested mapping inner key is
	// a dynamic bytes calldata param.
	"0x0de8bf93da2f7eecb3d9169422413a9bef4ef628:0xb1ae2ed1": deriveCoinToolTKeys,
}

// deriveReceiveMessageKeys parses the calldata of MessageTransmitter.receiveMessage
// (bytes message, bytes attestation) and derives:
//
//	usedNonces[_hashSourceAndNonce(sourceDomain, nonce)]  -> slot 10 (bytes32-keyed mapping)
//	enabledAttesters._indexes[attester]                   -> slot 6  (AddressSet._indexes)
//
// for every attester recovered from the attestation signatures via secp256k1
// ecrecover over keccak256(message).
func deriveReceiveMessageKeys(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs) (*DerivedKey, bool) {
	// Only the global usedNonces (mapping) access is expanded here; all other
	// fields are handled by the generic static machinery.
	if acc.Field != "usedNonces" || acc.Account != "global" {
		return nil, false
	}
	raw := args.RawCalldata
	// calldata = selector(4) || offset_message(32) || offset_attestation(32) || ...
	// Offsets are relative to the start of the argument data (byte 4).
	if len(raw) < 68 {
		return nil, false
	}
	msgOff := abiOffsetWord(raw, 4)
	attOff := abiOffsetWord(raw, 36)
	msgData, ok := parseDynamicBytes(raw, 4+msgOff)
	if !ok {
		return nil, false
	}
	attData, ok := parseDynamicBytes(raw, 4+attOff)
	if !ok {
		return nil, false
	}
	if len(msgData) < 20 {
		return nil, false
	}
	// Message layout: [0:4] version, [4:8] sourceDomain (uint32 BE),
	// [12:20] nonce (uint64 BE). _hashSourceAndNonce(source, nonce) =
	// keccak256(abi.encodePacked(source, nonce)) = keccak256(12 bytes).
	srcAndNonce := make([]byte, 0, 12)
	srcAndNonce = append(srcAndNonce, msgData[4:8]...)
	srcAndNonce = append(srcAndNonce, msgData[12:20]...)
	usedNonceKey := mappingSlotKeyBytes32(contractAddr, keccak256Bytes(srcAndNonce), 10)

	// Recover every attester and derive the AddressSet._indexes slot keys
	// (enabledAttesters struct at slot 5, _indexes mapping at slot 6).
	digest := keccak256Bytes(msgData)
	var indexKeys []string
	for i := 0; i+65 <= len(attData); i += 65 {
		attester, err := recoverAttesterAddress(digest, attData[i:i+65])
		if err != nil {
			return nil, false
		}
		indexKeys = append(indexKeys, mappingSlotKey(contractAddr, "0x"+hex.EncodeToString(attester), 6))
	}
	if len(indexKeys) == 0 {
		return nil, false
	}
	return &DerivedKey{
		ReadKeys:  append([]string{usedNonceKey}, indexKeys...),
		WriteKeys: []string{usedNonceKey},
	}, true
}

// deriveCoinToolTKeys parses the calldata of CoinTool_App.t(uint256 total,
// bytes data, bytes calldata _salt) and derives the nested mapping value slot
// for map[msg.sender][_salt] (base slot 1), whose inner key is the dynamic
// bytes _salt from calldata.
func deriveCoinToolTKeys(contractAddr string, acc LLMFieldAccess, args MainnetTxArgs) (*DerivedKey, bool) {
	if acc.Field != "map" {
		return nil, false
	}
	outerAddr, ok := resolveAccountAddr(acc.Account, args)
	if !ok {
		return nil, false
	}
	raw := args.RawCalldata
	// t(uint256 total, bytes data, bytes calldata _salt):
	// selector(4) || total(32) || offset_data(32) || offset_salt(32) || ...
	// Offsets are relative to the start of the argument data (byte 4).
	if len(raw) < 100 {
		return nil, false
	}
	saltOff := abiOffsetWord(raw, 68)
	salt, ok := parseDynamicBytes(raw, 4+saltOff)
	if !ok {
		return nil, false
	}
	key := nestedBytesMappingSlotKey(contractAddr, outerAddr, salt, 1)
	return &DerivedKey{ReadKeys: []string{key}, WriteKeys: []string{key}}, true
}

// abiOffsetWord reads the 32-byte big-endian word at raw[at:at+32] and returns
// its numeric value. ABI dynamic-argument offsets are encoded as a full 32-byte
// word (value in the trailing bytes), NOT a Uint32 at the word start.
func abiOffsetWord(raw []byte, at int) uint64 {
	if at+32 > len(raw) {
		return 0
	}
	return binary.BigEndian.Uint64(raw[at+24 : at+32])
}

// parseDynamicBytes reads a `bytes` value located at ABI byte offset off.
// ABI encodes the length as a full 32-byte word — the numeric value lives in
// the TRAILING bytes of the word (like abiOffsetWord), followed by the raw
// data right-padded to a 32-byte multiple.
func parseDynamicBytes(raw []byte, off uint64) ([]byte, bool) {
	if off+32 > uint64(len(raw)) {
		return nil, false
	}
	length := binary.BigEndian.Uint64(raw[off+24 : off+32])
	padded := ((length + 31) / 32) * 32
	end := off + 32 + padded
	if end > uint64(len(raw)) {
		return nil, false
	}
	return raw[off+32 : off+32+length], true
}

// recoverAttesterAddress recovers the 20-byte address from a 65-byte
// Ethereum-style signature (r||s||v, v ∈ {27,28}) over digest using pure-Go
// btcec — the same approach as the vendored go-ethereum crypto package's
// non-cgo ecrecover.
func recoverAttesterAddress(digest, sig []byte) ([]byte, error) {
	if len(sig) != 65 {
		return nil, fmt.Errorf("invalid signature length %d", len(sig))
	}
	v := sig[64]
	var recid byte
	switch {
	case v == 27 || v == 28:
		recid = v - 27
	case v <= 3:
		recid = v
	default:
		return nil, fmt.Errorf("invalid signature recovery id %d", v)
	}
	btcsig := make([]byte, 65)
	btcsig[0] = recid + 27
	copy(btcsig[1:], sig[:64])
	pub, _, err := btcec.RecoverCompact(btcec.S256(), btcsig, digest)
	if err != nil {
		return nil, err
	}
	// Address = last 20 bytes of keccak256(public_key_x || public_key_y).
	uncompressed := pub.SerializeUncompressed() // 0x04 || X(32) || Y(32)
	sum := keccak256Bytes(uncompressed[1:])
	return sum[12:], nil
}
