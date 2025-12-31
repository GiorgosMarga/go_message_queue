package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/queue"
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

func (ps *ConsumerServer) ConsumeMessage() (*queue.Message, error) {
	// Header: length + msg type
	b := make([]byte, 5)
	binary.LittleEndian.PutUint32(b, 0)
	b[4] = ConsumeMsg
	_, err := ps.conn.Write(b)
	if err != nil {
		return nil, err
	}

	msgBuf := make([]byte, 4096)
	n, err := ps.conn.Read(msgBuf)
	if err != nil {
		return nil, err
	}

	if msgBuf[0] == EmptyQueueResp {
		return nil, fmt.Errorf("no message")
	}

	m := &queue.Message{}
	if err := m.Decode(msgBuf[:n]); err != nil {
		return nil, err
	}

	ackBuf := make([]byte, 13) // header + id
	binary.LittleEndian.PutUint32(ackBuf, 8)
	ackBuf[4] = AckMsg
	binary.LittleEndian.PutUint64(ackBuf[5:], uint64(m.Id))
	time.Sleep(time.Duration(0) * time.Second)
	_, err = ps.conn.Write(ackBuf)
	return m, err
}
