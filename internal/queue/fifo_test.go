package queue

import (
	"testing"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

func TestFifo(t *testing.T) {
	fifo := NewFifo()
	items := 10_000_000
	for i := range items {
		fifo.Enqueue(&message.Message{
			Id:        uint64(i),
			Type:      message.MessageEnqueue,
			Body:      []byte("hello"),
			Timestamp: time.Now(),
			Priority:  0,
		})
	}
	for i := range items {
		m, err := fifo.Dequeue()
		if err != nil {
			t.Error(err)
		}

		if uint64(i) != m.Id {
			t.Error()
		}
	}

}

func BenchmarkFifo(b *testing.B) {
	items := 10_000_000

	for b.Loop() {
		fifo := NewFifo()
		for i := range items {
			fifo.Enqueue(&message.Message{
				Id:        uint64(i),
				Type:      message.MessageEnqueue,
				Body:      []byte("hello"),
				Timestamp: time.Now(),
				Priority:  0,
			})
		}

		for range items {
			fifo.Dequeue()
		}
	}
}
