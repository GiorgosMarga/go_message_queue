package queue

import (
	"encoding/binary"
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
	var q Queue
	if isPriority {
		q = NewPriorityQueue()
	} else {
		q = NewFifo()
	}

	log, err := wal.NewWAL(filePath, 1<<10, 10, true)
	if err != nil {
		return nil, err
	}
	fbq := &FileBackedQueue{
		log: log,
		q:   q,
	}

	if err := fbq.restoreState(); err != nil {
		return nil, err
	}

	return fbq, nil
}

func (fbq *FileBackedQueue) restoreState() error {
	return nil
}

func (fbq *FileBackedQueue) Enqueue(msg *message.Message) error {
	// write file to log
	if err := fbq.log.Write(msg.Bytes(), false); err != nil {
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
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(msgID))
	return fbq.log.Write(buf, true)
}

func (fbq *FileBackedQueue) GetLiveMessages() []*message.Message {
	return fbq.q.GetLiveMessages()
}

// Close gracefully shuts down the queue
func (fbq *FileBackedQueue) Close() error {
	return fbq.log.Close()
}
