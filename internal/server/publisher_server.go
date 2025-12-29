package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"
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
	b[0] = 0
	binary.LittleEndian.PutUint64(b[1:], uint64(id))
	binary.LittleEndian.PutUint16(b[9:], uint16(len(body)))
	copy(b[11:], body)
	t := time.Now()
	tb, err := t.MarshalBinary()
	if err != nil {
		return err
	}
	fmt.Println(len(tb))
	copy(b[11+len(body):], tb)
	binary.LittleEndian.PutUint16(b[11+len(body)+len(tb):], uint16(10))

	n, err := ps.conn.Write(b[:11+len(body)+len(tb)+2])
	if err != nil {
		return err
	}
	if n != 13+len(body)+len(tb) {
		return fmt.Errorf("did not write the entire buffer")
	}
	return nil
}
