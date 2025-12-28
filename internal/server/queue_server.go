package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/GiorgosMarga/ibmmq/internal/queue"
)

type QueueServer struct {
	listenAddr string
	queue      *queue.Fifo
}

func NewQueueServer(lAddr string) *QueueServer {
	if !strings.HasPrefix(lAddr, ":") {
		lAddr = ":" + lAddr
	}
	return &QueueServer{
		listenAddr: lAddr,
		queue:      queue.New(),
	}
}

func (qs *QueueServer) Start() error {
	ln, err := net.Listen("tcp", qs.listenAddr)
	if err != nil {
		return err
	}
	fmt.Printf("Queue server is listening on address %s...\n", qs.listenAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go qs.handleConn(conn)
	}
}

func (qs *QueueServer) handleConn(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("[QS]: new connection: %v\n", conn.LocalAddr())
	b := make([]byte, 4096)
	for {
		n, err := conn.Read(b)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			fmt.Println(err)
			continue
		}
		id := binary.LittleEndian.Uint64(b[:n])
		fmt.Println(id)
		bodySize := binary.LittleEndian.Uint16(b[8:n])
		fmt.Println(bodySize)
		fmt.Println(string(b[10:n]))
	}

}
