package core

import (
	"container/list"
	"sort"
	"sync"
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

func (q *TransactionQueue) contains(txID string) bool {
	for elem := q.queue.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.(string) == txID {
			return true
		}
	}
	return false
}

func (q *TransactionQueue) remove(txID string) bool {
	for elem := q.queue.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.(string) == txID {
			q.queue.Remove(elem)
			return true
		}
	}
	return false
}

// DepurgeScheduler schedules transactions based on conservative key dependencies.
//
// Thread safety: all public methods are guarded by mu (RWMutex). Read methods
// (GetConservativeKeys, IsExecuted, GetReadyCount, GetReadyQueueLen,
// GetPruneReadyQueueLen) take RLock; mutating methods (Execute, Abort, Prune,
// PopReady, PopPruneReady, PushReady) take Lock.
//
// The COMPOSITION of calls (e.g. PopReady then Execute) is NOT atomic — that's
// by design: each txID is processed by exactly one worker at a time, and the
// scheduler's job is to release the next txID's dependency count atomically
// per-method. This is sufficient because:
//   - txReadyCount[nextTx]-- is atomic under Lock
//   - queue.pop() / queue.front() are atomic under Lock
//   - txExecuted[txID] = true is atomic under Lock
//
// Setup methods (addTransaction, buildLevels, schedule) are NOT locked because
// they are only called from Depurge_schedule, which runs single-threaded
// before any worker goroutines start.
type DepurgeScheduler struct {
	mu              sync.RWMutex
	keyQueues       map[string]*TransactionQueue
	txKeyMap        map[string][]string
	txExecuted      map[string]bool
	readyQueue      *list.List
	pruneReadyQueue *list.List
	txReadyCount    map[string]int
}

func NewDepurgeScheduler() *DepurgeScheduler {
	return &DepurgeScheduler{
		keyQueues:       make(map[string]*TransactionQueue),
		txKeyMap:        make(map[string][]string),
		txExecuted:      make(map[string]bool),
		readyQueue:      list.New(),
		pruneReadyQueue: list.New(),
		txReadyCount:    make(map[string]int),
	}
}

func (ds *DepurgeScheduler) addTransaction(txID string, keys []string) {
	sort.Strings(keys)
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

func (ds *DepurgeScheduler) buildLevels() [][]string {
	var levels [][]string
	tempReady := list.New()
	tempExecuted := make(map[string]bool)
	inTempReady := make(map[string]bool)

	for elem := ds.readyQueue.Front(); elem != nil; elem = elem.Next() {
		txID := elem.Value.(string)
		tempReady.PushBack(txID)
		tempExecuted[txID] = false
		inTempReady[txID] = true
	}

	for tempReady.Len() > 0 {
		levelSize := tempReady.Len()
		level := make([]string, 0, levelSize)

		for i := 0; i < levelSize; i++ {
			elem := tempReady.Front()
			if elem == nil {
				break
			}
			txID := elem.Value.(string)
			tempReady.Remove(elem)
			inTempReady[txID] = false

			level = append(level, txID)
			tempExecuted[txID] = true

			keys := ds.txKeyMap[txID]
			for _, key := range keys {
				queue := ds.keyQueues[key]
				if queue.front() == txID {
					nextTx := ""
					for e := queue.queue.Front(); e != nil; e = e.Next() {
						if e.Value.(string) != txID && !tempExecuted[e.Value.(string)] {
							nextTx = e.Value.(string)
							break
						}
					}
					if nextTx != "" && !inTempReady[nextTx] {
						tempExecuted[nextTx] = false
						tempReady.PushBack(nextTx)
						inTempReady[nextTx] = true
					}
				}
			}
		}

		sort.Strings(level)
		levels = append(levels, level)
	}

	return levels
}

func (ds *DepurgeScheduler) Execute(txID string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
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
					isAlreadyInReady := false
					for elem := ds.readyQueue.Front(); elem != nil; elem = elem.Next() {
						if elem.Value.(string) == nextTx {
							isAlreadyInReady = true
							break
						}
					}
					if !isAlreadyInReady {
						ds.readyQueue.PushBack(nextTx)
					}
				}
			}
		}
	}
}

func (ds *DepurgeScheduler) Abort(txID string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
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
					isAlreadyInReady := false
					for elem := ds.readyQueue.Front(); elem != nil; elem = elem.Next() {
						if elem.Value.(string) == nextTx {
							isAlreadyInReady = true
							break
						}
					}
					isAlreadyInPruneReady := false
					for elem := ds.pruneReadyQueue.Front(); elem != nil; elem = elem.Next() {
						if elem.Value.(string) == nextTx {
							isAlreadyInPruneReady = true
							break
						}
					}
					if !isAlreadyInReady && !isAlreadyInPruneReady {
						ds.pruneReadyQueue.PushBack(nextTx)
					}
				}
			}
		}
	}
}

func (ds *DepurgeScheduler) Prune(txID string, realKeys []string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	conservativeKeys := ds.txKeyMap[txID]
	conservativeKeySet := make(map[string]bool)
	for _, k := range conservativeKeys {
		conservativeKeySet[k] = true
	}

	realKeySet := make(map[string]bool)
	for _, k := range realKeys {
		realKeySet[k] = true
	}

	for _, key := range conservativeKeys {
		if !realKeySet[key] {
			queue := ds.keyQueues[key]
			if queue.contains(txID) {
				wasFront := queue.front() == txID
				queue.remove(txID)

				if wasFront {
					nextTx := queue.front()
					if nextTx != "" && !ds.txExecuted[nextTx] {
						ds.txReadyCount[nextTx]--
						if ds.txReadyCount[nextTx] == 0 {
							isAlreadyInReady := false
							for elem := ds.readyQueue.Front(); elem != nil; elem = elem.Next() {
								if elem.Value.(string) == nextTx {
									isAlreadyInReady = true
									break
								}
							}
							isAlreadyInPruneReady := false
							for elem := ds.pruneReadyQueue.Front(); elem != nil; elem = elem.Next() {
								if elem.Value.(string) == nextTx {
									isAlreadyInPruneReady = true
									break
								}
							}
							if !isAlreadyInReady && !isAlreadyInPruneReady {
								ds.pruneReadyQueue.PushBack(nextTx)
							}
						}
					}
				}
			}
		}
	}
}

func (ds *DepurgeScheduler) GetReadyCount(txID string) int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.txReadyCount[txID]
}

func (ds *DepurgeScheduler) GetConservativeKeys(txID string) []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.txKeyMap[txID]
}

func (ds *DepurgeScheduler) IsExecuted(txID string) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.txExecuted[txID]
}

func (ds *DepurgeScheduler) GetReadyQueueLen() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.readyQueue.Len()
}

func (ds *DepurgeScheduler) PopReady() string {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	elem := ds.readyQueue.Front()
	if elem == nil {
		return ""
	}
	ds.readyQueue.Remove(elem)
	return elem.Value.(string)
}

func (ds *DepurgeScheduler) PushReady(txID string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.readyQueue.PushBack(txID)
}

func (ds *DepurgeScheduler) GetPruneReadyQueueLen() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.pruneReadyQueue.Len()
}

func (ds *DepurgeScheduler) PopPruneReady() string {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	elem := ds.pruneReadyQueue.Front()
	if elem == nil {
		return ""
	}
	ds.pruneReadyQueue.Remove(elem)
	return elem.Value.(string)
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
			ds.Execute(txID)
		}

		sort.Strings(level)
		levels = append(levels, level)
	}

	return levels
}

func Depurge_schedule(contexts map[string]*TransactionContext) (*DepurgeScheduler, [][]string) {
	scheduler := NewDepurgeScheduler()

	var ctxSlice []*TransactionContext
	for _, ctx := range contexts {
		ctxSlice = append(ctxSlice, ctx)
	}

	sort.Slice(ctxSlice, func(i, j int) bool {
		return ctxSlice[i].TxID < ctxSlice[j].TxID
	})

	for _, ctx := range ctxSlice {
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
		sort.Strings(keys)

		scheduler.addTransaction(ctx.TxID, keys)
	}

	levels := scheduler.buildLevels()

	return scheduler, levels
}
