package message

import (
	"encoding/binary"
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
	Timestamp int64
	Priority  uint16
}

func (m *Message) Bytes() []byte {
	// type + id + len(data) + data + timestamp + priority
	bufSize := 1 + 8 + 4 + len(m.Body) + 8 + 2
	b := make([]byte, bufSize)
	offset := 0
	b[offset] = byte(m.Type)
	offset += 1
	binary.LittleEndian.PutUint64(b[offset:], uint64(m.Id))
	offset += 8
	binary.LittleEndian.PutUint16(b[offset:], uint16(len(m.Body)))
	offset += 2
	copy(b[offset:], m.Body)
	offset += len(m.Body)
	binary.LittleEndian.PutUint64(b[offset:], uint64(m.Timestamp))
	offset += 8
	binary.LittleEndian.PutUint16(b[offset:], uint16(m.Priority))
	return b
}

func MsgFromBytes(b []byte) (*Message, error) {
	m := &Message{}
	offset := 0
	m.Type = MessageType(b[offset])
	offset += 1
	m.Id = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	dataSize := binary.LittleEndian.Uint16(b[offset:])
	offset += 2
	m.Body = make([]byte, dataSize)
	copy(m.Body, b[offset:offset+int(dataSize)])
	offset += len(m.Body)
	m.Timestamp = int64(binary.LittleEndian.Uint64(b[offset:]))
	offset += 8
	m.Priority = binary.LittleEndian.Uint16(b[offset:])
	return m, nil
}
