package queue

import "time"

type Message struct {
	Id        int
	Body      []byte
	Timestamp time.Time
	Priority  int
}

type Queue interface {
	// Enqueue adds a message to the queue
	Enqueue(msg *Message) error

	// Dequeue removes and returns a message from the queue.
	Dequeue() (*Message, error)

	// Peek returns the next message without removing it
	Peek() (*Message, error)

	// Size returns the current number of messages in the queue
	Size() int

	// Ack confirms the processing of a message
	Ack(msgID string) error

	// Close gracefully shuts down the queue
	Close() error
}
