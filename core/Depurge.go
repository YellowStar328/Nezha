package core

import (
	"container/list"
)

type TransactionQueue struct {
	queue *list.List
}

func newTransactionQueue() *TransactionQueue {
	return &TransactionQueue{
		queue: list.New(),
	}
}

func (q *TransactionQueue) push(txID string) {
	q.queue.PushBack(txID)
}

func (q *TransactionQueue) pop() string {
	elem := q.queue.Front()
	if elem == nil {
		return ""
	}
	q.queue.Remove(elem)
	return elem.Value.(string)
}

func (q *TransactionQueue) front() string {
	elem := q.queue.Front()
	if elem == nil {
		return ""
	}
	return elem.Value.(string)
}

func (q *TransactionQueue) isEmpty() bool {
	return q.queue.Len() == 0
}

func (q *TransactionQueue) len() int {
	return q.queue.Len()
}

type DepurgeScheduler struct {
	keyQueues    map[string]*TransactionQueue
	txKeyMap     map[string][]string
	txExecuted   map[string]bool
	readyQueue   *list.List
	txReadyCount map[string]int
}

func NewDepurgeScheduler() *DepurgeScheduler {
	return &DepurgeScheduler{
		keyQueues:    make(map[string]*TransactionQueue),
		txKeyMap:     make(map[string][]string),
		txExecuted:   make(map[string]bool),
		readyQueue:   list.New(),
		txReadyCount: make(map[string]int),
	}
}

func (ds *DepurgeScheduler) addTransaction(txID string, keys []string) {
	ds.txKeyMap[txID] = keys
	ds.txExecuted[txID] = false
	ds.txReadyCount[txID] = len(keys)

	for _, key := range keys {
		if _, ok := ds.keyQueues[key]; !ok {
			ds.keyQueues[key] = newTransactionQueue()
		}
		ds.keyQueues[key].push(txID)

		if ds.keyQueues[key].front() == txID {
			ds.txReadyCount[txID]--
		}
	}

	if ds.txReadyCount[txID] == 0 {
		ds.readyQueue.PushBack(txID)
	}
}

func (ds *DepurgeScheduler) execute(txID string) {
	if ds.txExecuted[txID] {
		return
	}

	ds.txExecuted[txID] = true

	keys := ds.txKeyMap[txID]
	for _, key := range keys {
		queue := ds.keyQueues[key]
		if queue.front() == txID {
			queue.pop()

			nextTx := queue.front()
			if nextTx != "" && !ds.txExecuted[nextTx] {
				ds.txReadyCount[nextTx]--
				if ds.txReadyCount[nextTx] == 0 {
					ds.readyQueue.PushBack(nextTx)
				}
			}
		}
	}
}

func (ds *DepurgeScheduler) schedule() [][]string {
	var levels [][]string

	for ds.readyQueue.Len() > 0 {
		levelSize := ds.readyQueue.Len()
		level := make([]string, 0, levelSize)

		for i := 0; i < levelSize; i++ {
			elem := ds.readyQueue.Front()
			if elem == nil {
				break
			}
			txID := elem.Value.(string)
			ds.readyQueue.Remove(elem)

			level = append(level, txID)
			ds.execute(txID)
		}

		levels = append(levels, level)
	}

	return levels
}

func Depurge_schedule(contexts map[string]*TransactionContext) [][]string {
	scheduler := NewDepurgeScheduler()
	contextSlice := make([]*TransactionContext, 0, len(contexts))
	for _, ctx := range contexts {
		contextSlice = append(contextSlice, ctx)
	}

	for _, ctx := range contextSlice {
		allKeys := make(map[string]bool)

		for key := range ctx.PreReadSet {
			allKeys[key] = true
		}
		for key := range ctx.PreWriteSet {
			allKeys[key] = true
		}

		keys := make([]string, 0, len(allKeys))
		for key := range allKeys {
			keys = append(keys, key)
		}

		scheduler.addTransaction(ctx.TxID, keys)
	}

	return scheduler.schedule()
}
