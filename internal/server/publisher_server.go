package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
)

type PublisherServer struct {
	queueAddr string
	conn      net.Conn
}

func NewPublisherServer(qAddr string) *PublisherServer {
	return &PublisherServer{
		queueAddr: qAddr,
	}
}

func (ps *PublisherServer) CreateConn() error {
	conn, err := net.Dial("tcp", ps.queueAddr)
	if err != nil {
		return err
	}
	ps.conn = conn
	return nil
}

func (ps *PublisherServer) PublishMessage(body []byte) error {
	b := make([]byte, 4096)
	id := rand.Intn(100)
	binary.LittleEndian.PutUint64(b, uint64(id))
	binary.LittleEndian.PutUint16(b[8:], uint16(len(body)))
	copy(b[10:], body)

	n, err := ps.conn.Write(b[:10+len(body)])
	if err != nil {
		return err
	}
	if n != 10+len(body) {
		return fmt.Errorf("did not write the entire buffer")
	}
	return nil
}
