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
	_, err := ps.conn.Write([]byte{1})
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4096)
	n, err := ps.conn.Read(b)
	if err != nil {
		return nil, err
	}

	if b[0] == 3 {
		return nil, fmt.Errorf("no message")
	}

	m := &queue.Message{}
	if err := m.Decode(b[:n]); err != nil {
		return nil, err
	}
	b[0] = AckMsg
	binary.LittleEndian.PutUint64(b[1:], uint64(m.Id))
	time.Sleep(10 * time.Second)
	ps.conn.Write(b[:9])
	return m, nil
}
