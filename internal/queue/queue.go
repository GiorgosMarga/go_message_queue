package queue

import (
	"errors"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

var (
	ErrEmptyQueue = errors.New("empty queue")
)

const (
	HeaderSize = 5
)

type Queue interface {
	// Enqueue adds a message to the queue
	Enqueue(msg *message.Message) error

	// Dequeue removes and returns a message from the queue.
	Dequeue() (*message.Message, error)

	// Peek returns the next message without removing it
	Peek() (*message.Message, error)

	// Size returns the current number of messages in the queue
	Size() int

	// Ack confirms the processing of a message
	Ack(msgID int) error

	// Close gracefully shuts down the queue
	Close() error
}
