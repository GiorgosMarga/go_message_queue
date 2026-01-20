package queue

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/message"
	"github.com/GiorgosMarga/ibmmq/internal/wal"
)

var (
	ErrCorruptedFile = fmt.Errorf("corrupted file")
)

type Log interface {
	Write([]byte, bool) error
	ReadAll() ([][]byte, error)
	RestoreState() ([][]byte, error)
	Compact([][]byte) error
	Close() error
}

type FileBackedQueue struct {
	log Log
	q   Queue
	// compact every 100 dequeues
	deqsBeforeCompact int
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
func (fbq *FileBackedQueue) compact() error {
	fmt.Println("Compacting")
	msgs := fbq.GetLiveMessages()
	data := make([][]byte, len(msgs))
	for i, msg := range msgs {
		data[i] = msg.Bytes()
	}
	if err := fbq.log.Compact(data); err != nil {
		fmt.Println(err)
	}
	return nil
}

func (fbq *FileBackedQueue) restoreState() error {
	data, err := fbq.log.RestoreState()
	if err != nil {
		return err
	}
	msgs := make(map[uint64]*message.Message, len(data))
	acked := make(map[uint64]struct{})
	for _, msgBuf := range data {
		m, err := message.MsgFromBytes(msgBuf)
		if err != nil {
			return err
		}
		switch m.Type {
		case message.MessageEnqueue:
			if _, ok := acked[m.Id]; ok {
				continue
			}
			msgs[m.Id] = m
		case message.MessageAck:
			if _, ok := acked[m.Id]; ok {
				return fmt.Errorf("double ack id")
			}
			if _, ok := msgs[m.Id]; ok {
				delete(msgs, m.Id)
			} else {
				acked[m.Id] = struct{}{}
			}
		default:
			return fmt.Errorf("unexpected message type")
		}
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

	msg.Type = message.MessageEnqueue
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
	msg, err := fbq.q.Dequeue()
	if err != nil {
		return nil, err
	}
	fbq.deqsBeforeCompact++
	if fbq.deqsBeforeCompact%100 == 0 {
		if err := fbq.compact(); err != nil {
			return nil, err
		}
	}
	return msg, err
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
	msg := &message.Message{
		Id:        uint64(msgID),
		Type:      message.MessageAck,
		Timestamp: time.Now().Unix(),
		Body:      buf,
		Priority:  0,
	}
	return fbq.log.Write(msg.Bytes(), true)
}

func (fbq *FileBackedQueue) GetLiveMessages() []*message.Message {
	return fbq.q.GetLiveMessages()
}

// Close gracefully shuts down the queue
func (fbq *FileBackedQueue) Close() error {
	return fbq.log.Close()
}
