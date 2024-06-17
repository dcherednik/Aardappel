package tx_queue

import "aardappel/internal/types"

type QueueItem struct {
	item  types.TxData
	index int
	order uint32
}

func (lhs QueueItem) Less(rhs *QueueItem) bool {
	return lhs.item.Step > rhs.item.Step ||
		lhs.item.Step == rhs.item.Step && lhs.item.TxId > rhs.item.TxId ||
		lhs.item.Step == rhs.item.Step && lhs.item.TxId == rhs.item.TxId && lhs.order > rhs.order
}

type PriorityQueue struct {
	items []*QueueItem
	order uint32
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{make([]*QueueItem, 0), 0}
}

func (pq PriorityQueue) Len() int { return len(pq.items) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.items[i].Less(pq.items[j])
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(value interface{}) {
	item := QueueItem{*value.(*types.TxData), len(pq.items), pq.order}
	pq.order++
	pq.items = append(pq.items, &item)
}

func (pq *PriorityQueue) Get() *types.TxData {
	if pq.Len() > 0 {
		return &pq.items[len(pq.items)-1].item
	}
	return nil
}

func (pq *PriorityQueue) Pop() interface{} {
	currentItems := pq.items
	pqLen := len(currentItems)
	item := currentItems[pqLen-1]
	currentItems[pqLen-1] = nil
	item.index = -1
	pq.items = currentItems[0 : pqLen-1]
	return item
}
