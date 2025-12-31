package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/queue"
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

func (ps *PublisherServer) PublishMessage(body []byte, priority uint16) error {
	// buffer to write is 4 bytes for the content size + 1 byte for the msg type + the `msg` bytes
	msg := &queue.Message{
		Id:        rand.Intn(100),
		Body:      body,
		Timestamp: time.Now(),
		Priority:  priority,
	}
	bmsg, err := msg.ToBytes()
	if err != nil {
		return err
	}

	b := make([]byte, 5+len(bmsg))
	binary.LittleEndian.PutUint32(b, uint32(len(bmsg)))
	b[4] = PublishMsg
	copy(b[5:], bmsg)

	fmt.Printf("Sending message with size: %d %d\n", len(b), len(b)-5)

	_, err = ps.conn.Write(b)
	return err
}
