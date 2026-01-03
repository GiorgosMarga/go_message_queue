package queue

import (
	"testing"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueueWithCap(16)

	for i := range 1000 {
		if err := pq.Enqueue(&message.Message{
			Id:       i,
			Priority: uint16(i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for range 1000 {
		if _, err := pq.Dequeue(); err != nil {
			t.Fatal(err)
		}
	}
}
