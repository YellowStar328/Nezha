package core

import (
	"fmt"
	"math/big"
	"reflect"

	"Nezha/ethereum/go-ethereum/common"
)

type RWNode struct {
	RWSet      RWSet
	TransInfo  TransInfo
	Label      string
	Sequence   int32
	isAssigned bool
}

type TransInfo struct {
	ID        string
	Timestamp uint32
}

type RWSet struct {
	Key   []byte
	Value []byte
}

func CreateRWNode(id string, time uint32, rAddr [][]byte, rValue [][]byte, wAddr [][]byte, wValue [][]byte) []*RWNode {
	var rwNodes []*RWNode
	// transInfo := TransInfo{ConvertByte2String(transaction.ID), transaction.Header.Timestamp}
	transInfo := TransInfo{id, time}

	// TODO: obtain read&write set of transaction
	for i := 0; i < len(rAddr); i++ {
		rSet := RWSet{rAddr[i], rValue[i]}
		rNode := RWNode{rSet, transInfo, "r", 0, false}
		rwNodes = append(rwNodes, &rNode)
	}

	for j := 0; j < len(wAddr); j++ {
		wSet := RWSet{wAddr[j], wValue[j]}
		wNode := RWNode{wSet, transInfo, "w", 0, false}
		rwNodes = append(rwNodes, &wNode)
	}

	return rwNodes
}

func (rw *RWNode) assignSequence(edge []*RWNode) {
	for _, e := range edge {
		if reflect.DeepEqual(rw, e) {
			continue
		}
		e.Sequence = rw.Sequence
	}
}

func ConvertByte2String(bytes []byte) string {
	newString := fmt.Sprintf("%x", bytes)
	return newString
}

// TransactionContext 记录预执行的交易上下文（用于后续验证）
// 注意：不直接引用 utils.Transaction，避免循环依赖
type TransactionContext struct {
	TxID          string
	Function      string              // 交易函数名
	Addr1         uint64              // 交易参数
	Addr2         uint64              // 交易参数
	PreReadSet    map[string][]byte   // key -> value
	PreWriteSet   map[string][]byte   // key -> final value
	PreWriteDelta map[string]*big.Int // key -> delta (write - read)
	FromAddr      common.Address
	ContractAddr  common.Address
}

// RWNodesToContext 辅助函数：将 []*RWNode 转换为 TransactionContext
func RWNodesToContext(
	txID string,
	function string,
	addr1 uint64,
	addr2 uint64,
	rwNodes []*RWNode,
	fromAddr common.Address,
	contractAddr common.Address,
) *TransactionContext {
	readSet := make(map[string][]byte)
	writeSet := make(map[string][]byte)
	writeDelta := make(map[string]*big.Int)

	two256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	two255 := new(big.Int).Rsh(two256, 1)

	for _, rw := range rwNodes {
		key := ConvertByte2String(rw.RWSet.Key)
		if rw.Label == "r" {
			readSet[key] = rw.RWSet.Value
		} else if rw.Label == "w" {
			writeSet[key] = rw.RWSet.Value

			if readVal, ok := readSet[key]; ok {
				readBig := new(big.Int).SetBytes(readVal)
				writeBig := new(big.Int).SetBytes(rw.RWSet.Value)

				delta := new(big.Int).Sub(writeBig, readBig)

				if delta.Sign() < 0 {
					delta = new(big.Int).Add(delta, two256)
				}

				if delta.Cmp(two255) >= 0 {
					delta = new(big.Int).Sub(delta, two256)
				}

				writeDelta[key] = delta
			} else {
				writeDelta[key] = new(big.Int).SetBytes(rw.RWSet.Value)
			}
		}
	}

	return &TransactionContext{
		TxID:          txID,
		Function:      function,
		Addr1:         addr1,
		Addr2:         addr2,
		PreReadSet:    readSet,
		PreWriteSet:   writeSet,
		PreWriteDelta: writeDelta,
		FromAddr:      fromAddr,
		ContractAddr:  contractAddr,
	}
}
