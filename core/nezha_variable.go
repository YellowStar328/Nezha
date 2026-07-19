package core

import (
	"math"
	"reflect"
	"sort"
	"strings"
)

// NezhaVariableData Nezha_variable 算法的数据结构
type NezhaVariableData struct {
	Queues map[string]*VariableQueue
	Edges  map[string]*VariableEdge
}

// VariableQueue 可变读写集版本的队列结构
type VariableQueue struct {
	rSlice   []*RWNode
	wSlice   []*RWNode
	maxRead  int32
	maxWrite int32
}

// VariableEdge 可变读写集版本的边结构
type VariableEdge struct {
	set       []*RWNode
	isAborted bool
}

const variableInitialSequence = 10

// NewNezhaVariableData 创建 Nezha_variable 算法实例
func NewNezhaVariableData() *NezhaVariableData {
	return &NezhaVariableData{
		Queues: make(map[string]*VariableQueue),
		Edges:  make(map[string]*VariableEdge),
	}
}

// CreateVariableGraph 构建可变读写集版本的冲突图
func CreateVariableGraph(rwNodes [][]*RWNode) *NezhaVariableData {
	var edges = make(map[string]*VariableEdge)
	var queueArray = make(map[string][]*RWNode)
	var queues = make(map[string]*VariableQueue)

	for _, rw := range rwNodes {
		id := rw[0].TransInfo.ID
		edge := &VariableEdge{rw, false}
		edges[id] = edge

		for _, n := range rw {
			key := n.RWSet.Key
			newKey := ConvertByte2String(key)
			queueArray[newKey] = append(queueArray[newKey], n)
		}
	}

	for key := range queueArray {
		rSlice, wSlice := variableInitialSorting(queueArray[key])
		newQueue := &VariableQueue{rSlice, wSlice, 0, 0}
		queues[key] = newQueue
	}

	return &NezhaVariableData{queues, edges}
}

// variableInitialSorting 初始排序，将读操作放在写操作前面
func variableInitialSorting(queue []*RWNode) ([]*RWNode, []*RWNode) {
	var rSlice []*RWNode
	var wSlice []*RWNode

	for _, rw := range queue {
		if strings.Compare(rw.Label, "r") == 0 {
			rSlice = append(rSlice, rw)
		} else {
			wSlice = append(wSlice, rw)
		}
	}

	return rSlice, wSlice
}

// QueuesSort 确定不同队列之间的排序顺序
func (nv *NezhaVariableData) QueuesSort() []string {
	var sortedQueues = make(map[string]int)
	var sortedStrings []string

	for k := range nv.Queues {
		sortedStrings = append(sortedStrings, k)
	}

	sort.Strings(sortedStrings)

	for i, s := range sortedStrings {
		sortedQueues[s] = i
	}

	var depQueue = make(map[int][]int)

	for key := range nv.Queues {
		qIndex := sortedQueues[key]
		temp := map[string]struct{}{}

		for _, w := range nv.Queues[key].wSlice {
			rwKey := w.RWSet.Key
			id := w.TransInfo.ID

			for _, n := range nv.Edges[id].set {
				rwKey2 := n.RWSet.Key
				if reflect.DeepEqual(n.Label, "r") && !reflect.DeepEqual(rwKey, rwKey2) {
					newKey := ConvertByte2String(rwKey2)
					temp[newKey] = struct{}{}
				}
			}
		}

		if len(temp) > 0 {
			for kk := range temp {
				index := sortedQueues[kk]
				depQueue[qIndex] = append(depQueue[qIndex], index)
			}
		} else {
			depQueue[qIndex] = []int{}
		}
	}

	var deps [][]int

	for i := 0; i < len(depQueue); i++ {
		deps = append(deps, depQueue[i])
	}

	var al AlGraph
	var sequence []string

	al.Init(deps)
	order := al.AdvancedTopologicalSort()

	for _, o := range order {
		if len(sortedStrings) > 0 {
			str := sortedStrings[o]
			sequence = append(sequence, str)
		}
	}

	return sequence
}

// DeSS 获得确定性的全序
func (nv *NezhaVariableData) DeSS(sequence []string) map[int32][][]*RWNode {
	var commitOrder = make(map[int32][][]*RWNode)

	for _, s := range sequence {
		queue := nv.Queues[s]
		nv.sortInQueue(queue)
	}

	for t := range nv.Edges {
		if nv.Edges[t].isAborted {
			continue
		}

		edge := nv.Edges[t].set
		var wNodes []*RWNode

		for _, e := range edge {
			if e.Label == "w" {
				wNodes = append(wNodes, e)
			}
		}

		seq := edge[0].Sequence
		commitOrder[seq] = append(commitOrder[seq], wNodes)
	}

	return commitOrder
}

// sortInQueue 对每个队列中的读写单元进行排序
func (nv *NezhaVariableData) sortInQueue(queue *VariableQueue) {
	var tmpRQueue []*RWNode
	var tmpWQueue []*RWNode
	var tmpWQueue2 = make(map[int32][]*RWNode)

	for _, r := range queue.rSlice {
		if r.Sequence != 0 {
			id := r.TransInfo.ID
			if nv.Edges[id].isAborted {
				continue
			}
			tmpRQueue = append(tmpRQueue, r)
		}
	}

	if len(tmpRQueue) == 0 {
		for _, r := range queue.rSlice {
			r.Sequence = variableInitialSequence
			r.isAssigned = true
			id := r.TransInfo.ID
			edge := nv.Edges[id].set
			r.assignSequence(edge)
			queue.maxRead = r.Sequence
		}
	} else {
		min := math.MaxInt32

		for _, tr := range tmpRQueue {
			tr.isAssigned = true
			if int(tr.Sequence) < min {
				min = int(tr.Sequence)
			}
			if tr.Sequence > queue.maxRead {
				queue.maxRead = tr.Sequence
			}
		}

		for _, r := range queue.rSlice {
			if r.Sequence != 0 {
				continue
			}
			r.Sequence = int32(min)
			r.isAssigned = true
			id := r.TransInfo.ID
			edge := nv.Edges[id].set
			r.assignSequence(edge)
		}
	}

	for _, w := range queue.wSlice {
		if w.Sequence != 0 {
			id := w.TransInfo.ID
			if nv.Edges[id].isAborted {
				continue
			}
			if w.Sequence <= queue.maxRead {
				edge := nv.Edges[id].set
				isSame := false
				isBefore := false

				for _, rw := range edge {
					if rw.Label == "r" && rw.isAssigned {
						if reflect.DeepEqual(w.RWSet.Key, rw.RWSet.Key) {
							isSame = true
							break
						} else {
							isBefore = true
						}
					}
				}

				if isSame {
					tmpWQueue = append(tmpWQueue, w)
				} else if isBefore {
					nv.Edges[id].isAborted = true
				} else {
					tmpWQueue2[w.Sequence] = append(tmpWQueue2[w.Sequence], w)
				}
			} else {
				tmpWQueue2[w.Sequence] = append(tmpWQueue2[w.Sequence], w)
			}
		}
	}

	var keys []int

	for key := range tmpWQueue2 {
		keys = append(keys, int(key))
	}

	sort.Ints(keys)

	if queue.maxRead == 0 {
		queue.maxWrite = variableInitialSequence - 1
	} else {
		queue.maxWrite = queue.maxRead
	}

	for i, w := range tmpWQueue {
		id := w.TransInfo.ID
		if i == 0 {
			w.Sequence = queue.maxWrite + 1
			queue.maxWrite += 1
			queue.maxRead = queue.maxWrite
			w.isAssigned = true
			edge := nv.Edges[id].set
			w.assignSequence(edge)
		} else {
			// nv.Edges[id].isAborted = true
			w.Sequence = queue.maxWrite + 1
			queue.maxWrite += 1
			queue.maxRead = queue.maxWrite
			w.isAssigned = true
			edge := nv.Edges[id].set
			w.assignSequence(edge)
		}
	}

	for _, w := range queue.wSlice {
		if w.Sequence != 0 {
			continue
		}
		for i := queue.maxWrite + 1; i < math.MaxInt32; i++ {
			if !variableIsExist(keys, int(i)) {
				w.Sequence = i
				queue.maxWrite = w.Sequence
				w.isAssigned = true
				id := w.TransInfo.ID
				edge := nv.Edges[id].set
				w.assignSequence(edge)
				break
			}
		}
	}

	if len(keys) > 0 {
		maxSeq := keys[len(keys)-1]
		if queue.maxWrite < int32(maxSeq) {
			queue.maxWrite = int32(maxSeq)
		}
	}

	for _, n := range keys {
		for i, ww := range tmpWQueue2[int32(n)] {
			if int32(n) > queue.maxRead && i == 0 {
				continue
			}
			ww.Sequence = queue.maxWrite + 1
			queue.maxWrite += 1
			ww.isAssigned = true
			id := ww.TransInfo.ID
			edge := nv.Edges[id].set
			ww.assignSequence(edge)
		}
	}
}

// variableIsExist 检查键是否在切片中
func variableIsExist(slice []int, n int) bool {
	for _, e := range slice {
		if reflect.DeepEqual(n, e) {
			return true
		}
	}
	return false
}

// ProcessTransactions 处理交易的主函数
func (nv *NezhaVariableData) ProcessTransactions(txs [][]*RWNode) (commitOrder map[int32][][]*RWNode, abortedNum int) {
	graph := CreateVariableGraph(txs)

	sequence := graph.QueuesSort()
	commitOrder = graph.DeSS(sequence)

	abortedNum = graph.GetAbortedNums()
	return commitOrder, abortedNum
}

// GetAbortedNums 返回中止的交易数量
func (nv *NezhaVariableData) GetAbortedNums() int {
	count := 0

	for k := range nv.Edges {
		if nv.Edges[k].isAborted {
			count++
		}
	}

	return count
}
