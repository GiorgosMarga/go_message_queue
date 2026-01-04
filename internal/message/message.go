package message

import (
	"encoding/binary"
	"time"
)

type Message struct {
	Id        int
	Body      []byte
	Timestamp time.Time
	Priority  uint16
}

func (m *Message) Bytes() []byte {
	b := make([]byte, 4096)
	idx := 0
	binary.LittleEndian.PutUint64(b, uint64(m.Id))
	idx += 8
	binary.LittleEndian.PutUint16(b[idx:], uint16(len(m.Body)))
	idx += 2
	copy(b[idx:], m.Body)
	idx += len(m.Body)
	tb, _ := m.Timestamp.MarshalBinary()
	copy(b[idx:], tb)
	idx += len(tb)
	binary.LittleEndian.PutUint16(b[idx:], uint16(m.Priority))
	idx += 2
	return b[:idx]
}

// TODO: change this
func (m *Message) Decode(b []byte) error {
	m.Id = int(binary.LittleEndian.Uint64(b))
	dataSize := binary.LittleEndian.Uint16(b[8:])
	m.Body = make([]byte, dataSize)
	copy(m.Body, b[10:10+dataSize])
	err := m.Timestamp.UnmarshalBinary(b[10+dataSize : 25+dataSize])
	if err != nil {
		return err
	}
	m.Priority = binary.LittleEndian.Uint16(b[25+dataSize:])
	return nil
}
