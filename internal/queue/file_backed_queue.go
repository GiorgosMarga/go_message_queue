package queue

import (
	"fmt"

	"github.com/GiorgosMarga/ibmmq/internal/message"
	"github.com/GiorgosMarga/ibmmq/internal/wal"
)

var (
	ErrCorruptedFile = fmt.Errorf("corrupted file")
)

type FileBackedQueue struct {
	log *wal.WriteAheadLog
	q   Queue
}

func NewFileBackedQueue(filePath string, isPriority bool) (*FileBackedQueue, error) {
	log, err := wal.NewWAL(filePath)
	if err != nil {
		return nil, err
	}
	q := &FileBackedQueue{
		log: log,
	}

	if isPriority {
		q.q = NewPriorityQueue()
	} else {
		q.q = NewFifo()
	}

	if err := q.restoreState(); err != nil {
		return nil, err
	}

	return q, nil
}

func (fbq *FileBackedQueue) restoreState() error {
	b, err := fbq.log.ReadAll()
	if err != nil {
		return err
	}

	msgs, err := fbq.log.SyncMessagesFromLog(b)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		if err := fbq.q.Enqueue(msg); err != nil {
			return err
		}
	}
	return nil
}

func (fbq *FileBackedQueue) Enqueue(msg *message.Message) error {
	// write file to log
	b, _ := msg.ToBytes()
	if err := fbq.log.Write(b); err != nil {
		return err
	}

	if err := fbq.q.Enqueue(msg); err != nil {
		return err
	}
	return nil
}

// Dequeue removes and returns a message.Message from the queue.
// Use Ack() to remove from the log
func (fbq *FileBackedQueue) Dequeue() (*message.Message, error) {
	return fbq.q.Dequeue()
}

// Peek returns the next message.Message without removing it
func (fbq *FileBackedQueue) Peek() (*message.Message, error) {
	return fbq.Peek()
}

// Size returns the current number of messages in the queue
func (fbq *FileBackedQueue) Size() int {
	return fbq.Size()
}

// Ack confirms the processing of a message.Message
func (fbq *FileBackedQueue) Ack(msgID int) error {
	// remove file from log
	return fbq.log.Ack(msgID)
}

// Close gracefully shuts down the queue
func (fbq *FileBackedQueue) Close() error {
	return fbq.log.Close()
}
