package queue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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

	b, err := q.log.Sync()
	if err != nil {
		return nil, err
	}
	if err := q.syncMessagesFromLog(b); err != nil {
		return nil, err
	}

	return q, nil
}
func (fbq *FileBackedQueue) syncMessagesFromLog(b []byte) error {
	msgs := make(map[uint64]*Message)
	r := bytes.NewReader(b)

	header := make([]byte, wal.HeaderSize)
	for {
		_, err := r.Read(header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return ErrCorruptedFile
		}

		switch header[0] {
		case wal.AckType:
			idBuf := make([]byte, 8)
			_, err := r.Read(idBuf)
			if err != nil {
				return ErrCorruptedFile
			}
			id := binary.LittleEndian.Uint64(idBuf)
			_, ok := msgs[id]
			if !ok {
				return ErrCorruptedFile
			}
			delete(msgs, id)
		case wal.EnqueueType:
			msgSize := binary.LittleEndian.Uint16(header[1:])
			msgBuf := make([]byte, msgSize)
			n, err := r.Read(msgBuf)
			if err != nil || n != int(msgSize) {
				return ErrCorruptedFile
			}
			newMsg := &Message{}
			if err := newMsg.Decode(msgBuf); err != nil {
				return ErrCorruptedFile
			}
			msgs[uint64(newMsg.Id)] = newMsg
		default:
			return ErrCorruptedFile
		}
	}

	for _, msg := range msgs {
		if err := fbq.q.Enqueue(msg); err != nil {
			return err
		}
	}
	return nil
}
func (fbq *FileBackedQueue) Enqueue(msg *Message) error {
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

// Dequeue removes and returns a message from the queue.
// Use Ack() to remove from the log
func (fbq *FileBackedQueue) Dequeue() (*Message, error) {
	return fbq.q.Dequeue()
}

// Peek returns the next message without removing it
func (fbq *FileBackedQueue) Peek() (*Message, error) {
	return fbq.Peek()
}

// Size returns the current number of messages in the queue
func (fbq *FileBackedQueue) Size() int {
	return fbq.Size()
}

// Ack confirms the processing of a message
func (fbq *FileBackedQueue) Ack(msgID int) error {
	// remove file from log
	return fbq.log.Ack(msgID)
}

// Close gracefully shuts down the queue
func (fbq *FileBackedQueue) Close() error {
	return fbq.log.Close()
}
