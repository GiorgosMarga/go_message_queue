package queue

import "github.com/GiorgosMarga/ibmmq/internal/message"

const (
	MinimumCap = 16
)

type PriorityQueue struct {
	items     []*message.Message
	insertPtr int
}

func NewPriorityQueue() *PriorityQueue {
	return NewPriorityQueueWithCap(MinimumCap)
}

func NewPriorityQueueWithCap(capacity int) *PriorityQueue {
	return &PriorityQueue{
		items:     make([]*message.Message, capacity),
		insertPtr: 0,
	}

}

func (pq *PriorityQueue) heapify(idx int) {
	leftIdx := (idx * 2) + 1
	rightIdx := (idx * 2) + 2
	updateIdx := idx
	if leftIdx < pq.insertPtr && pq.items[leftIdx].Priority > pq.items[idx].Priority {
		updateIdx = leftIdx
	}
	if rightIdx < pq.insertPtr && pq.items[rightIdx].Priority > pq.items[idx].Priority {
		updateIdx = rightIdx
	}

	if updateIdx != idx {
		pq.items[updateIdx], pq.items[idx] = pq.items[idx], pq.items[updateIdx]
		pq.heapify(updateIdx)
	}
}
func (pq *PriorityQueue) resize(newSize int) {
	tempBuf := make([]*message.Message, newSize)
	copy(tempBuf, pq.items[:pq.insertPtr])
	pq.items = tempBuf
}
func (pq *PriorityQueue) Enqueue(msg *message.Message) error {
	if pq.insertPtr == len(pq.items) {
		pq.resize(pq.insertPtr * 2)
	}
	pq.items[pq.insertPtr] = msg
	pq.insertPtr += 1

	idx := pq.insertPtr - 1
	for idx >= 0 {
		parentIdx := (idx - 1) / 2
		if pq.items[parentIdx].Priority < pq.items[idx].Priority {
			pq.items[parentIdx], pq.items[idx] = pq.items[idx], pq.items[parentIdx]
			idx = parentIdx
		} else {
			break
		}
	}
	return nil
}

func (pq *PriorityQueue) Dequeue() (*message.Message, error) {
	if pq.insertPtr == 0 {
		return nil, ErrEmptyQueue
	}
	res := pq.items[0]
	pq.insertPtr -= 1
	pq.items[0], pq.items[pq.insertPtr] = pq.items[pq.insertPtr], nil

	if pq.insertPtr > 0 {
		pq.heapify(0)
	}
	// If usage < 25%, halve the capacity
	// Keep a minimum capacity to avoid over-shrinking
	if pq.insertPtr > 0 && pq.insertPtr <= len(pq.items)/4 && len(pq.items) > MinimumCap {
		pq.resize(len(pq.items) / 2)
	}

	return res, nil
}

func (pq *PriorityQueue) Peek() (*message.Message, error) {
	if pq.insertPtr == 0 {
		return nil, ErrEmptyQueue
	}
	return pq.items[0], nil
}

func (pq *PriorityQueue) Size() int {
	return pq.insertPtr
}

// Ack confirms the processing of a message.Message
func (pq *PriorityQueue) Ack(msgID int) error {
	return nil
}

func (pq *PriorityQueue) Close() error {
	return nil
}
