package queue

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Message struct {
	Id        int
	Body      []byte
	Timestamp time.Time
	Priority  uint16
}

func (m *Message) ToBytes() ([]byte, error) {
	b := make([]byte, 4096)
	idx := 0
	binary.LittleEndian.PutUint64(b, uint64(m.Id))
	idx += 8
	binary.LittleEndian.PutUint16(b[idx:], uint16(len(m.Body)))
	idx += 2
	copy(b[idx:], m.Body)
	idx += len(m.Body)
	tb, err := m.Timestamp.MarshalBinary()
	if err != nil {
		return nil, err
	}
	copy(b[idx:], tb)
	idx += len(tb)
	binary.LittleEndian.PutUint16(b[idx:], uint16(m.Priority))
	idx += 2
	return b[:idx], err
}

// TODO: change this
func (m *Message) Decode(b []byte) error {
	m.Id = int(binary.LittleEndian.Uint64(b))
	dataSize := binary.LittleEndian.Uint16(b[8:])
	m.Body = make([]byte, dataSize)
	copy(m.Body, b[10:10+dataSize])
	err := m.Timestamp.UnmarshalBinary(b[10+dataSize:25+dataSize])
	if err != nil {
		return err
	}
	fmt.Println(m.Timestamp)
	m.Priority = binary.LittleEndian.Uint16(b[25+dataSize:])
	return nil
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
