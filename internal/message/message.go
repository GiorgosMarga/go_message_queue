package message

import (
	"encoding/binary"
	"time"
)

type MessageType byte

const (
	MessageAck MessageType = iota
	MessageEnqueue
)

type Message struct {
	Id        uint64
	Type      MessageType
	Body      []byte
	Timestamp time.Time
	Priority  uint16
}

func (m *Message) Bytes() []byte {
	// type + id + len(data) + data + timestamp + priority
	bufSize := 1 + 8 + 4 + len(m.Body) + 15 + 2
	b := make([]byte, bufSize)
	b[0] = byte(m.Type)
	binary.LittleEndian.PutUint64(b[1:], uint64(m.Id))
	binary.LittleEndian.PutUint16(b[9:], uint16(len(m.Body)))
	copy(b[11:], m.Body)
	tb, _ := m.Timestamp.MarshalBinary()
	copy(b[11+len(m.Body):], tb)
	binary.LittleEndian.PutUint16(b[1+len(tb)+len(m.Body):], uint16(m.Priority))
	return b
}

func MsgFromBytes(b []byte) (*Message, error) {
	m := &Message{}
	m.Type = MessageType(b[0])
	m.Id = binary.LittleEndian.Uint64(b[1:])
	dataSize := binary.LittleEndian.Uint16(b[9:])
	m.Body = make([]byte, dataSize)
	copy(m.Body, b[11:11+dataSize])
	err := m.Timestamp.UnmarshalBinary(b[11+dataSize : 26+dataSize])
	if err != nil {
		return nil, err
	}
	m.Priority = binary.LittleEndian.Uint16(b[26+dataSize:])
	return m, nil
}
