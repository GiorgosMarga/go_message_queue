package queue

import (
	"testing"
	"time"
)

func TestFifo(t *testing.T) {
	fifo := New()
	items := 10_000_000
	for i := range items {
		fifo.Enqueue(&Message{
			Id:        i,
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

		if i != m.Id {
			t.Error()
		}
	}

}

func BenchmarkFifo(b *testing.B) {
	items := 10_000_000

	for b.Loop() {
		fifo := New()
		for i := range items {
			fifo.Enqueue(&Message{
				Id:        i,
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
