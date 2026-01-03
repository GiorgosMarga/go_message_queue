package server

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

type ConsumerServer struct {
	queueAddr string
	conn      net.Conn
}

func NewConsumerServer(qAddr string) *ConsumerServer {
	return &ConsumerServer{
		queueAddr: qAddr,
	}
}

func (ps *ConsumerServer) CreateConn() error {
	conn, err := net.Dial("tcp", ps.queueAddr)
	if err != nil {
		return err
	}
	ps.conn = conn
	return nil
}

func (ps *ConsumerServer) ConsumeMessage() (*message.Message, error) {
	// Header: length + msg type
	b := make([]byte, 5)
	binary.LittleEndian.PutUint32(b, 0)
	b[4] = ConsumeMsg
	_, err := ps.conn.Write(b)
	if err != nil {
		return nil, err
	}

	headerResponse := make([]byte, 3)
	_, err = ps.conn.Read(headerResponse)
	if err != nil {
		return nil, err
	}

	if headerResponse[0] == EmptyQueueResp {
		return nil, fmt.Errorf("no message")
	}
	size := binary.LittleEndian.Uint16(headerResponse[1:])

	msgBuf := make([]byte, size)
	_, err = ps.conn.Read(msgBuf)
	if err != nil {
		return nil, err
	}
	m := &message.Message{}
	if err := m.Decode(msgBuf); err != nil {
		return nil, err
	}

	ackBuf := make([]byte, 13) // header + id
	binary.LittleEndian.PutUint32(ackBuf, 8)
	ackBuf[4] = AckMsg
	binary.LittleEndian.PutUint64(ackBuf[5:], uint64(m.Id))
	_, err = ps.conn.Write(ackBuf)
	return m, err
}
