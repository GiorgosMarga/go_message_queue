package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/queue"
)

type InFlightMessage struct {
	to  net.Conn
	msg *queue.Message
}

type QueueServer struct {
	listenAddr   string
	queue        queue.Queue
	mode         int
	inFlightMsgs map[int]InFlightMessage
	mtx          *sync.Mutex
}

const (
	PublishMsg = iota
	ConsumeMsg
	DisconnectMsg
	AckMsg
	EmptyQueueResp
)

const (
	NonPersistent = iota
	Persistent    = iota
)

func NewQueueServer(lAddr string, mode int) *QueueServer {
	if !strings.HasPrefix(lAddr, ":") {
		lAddr = ":" + lAddr
	}
	qs := &QueueServer{
		listenAddr: lAddr,
		queue:      queue.New(),
		mode:       mode,
		mtx:        &sync.Mutex{},
	}

	if mode == Persistent {
		qs.inFlightMsgs = make(map[int]InFlightMessage)
		go qs.manageInFlightMsgs()
	}
	return qs
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

func (qs *QueueServer) manageInFlightMsgs() error {
	interval := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-interval.C:
			qs.mtx.Lock()
			for id := range qs.inFlightMsgs {
				fmt.Printf("Sending msg: [%d]\n", id)
			}
			qs.mtx.Unlock()
		default:
			time.Sleep(150 * time.Millisecond)
		}

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
		switch b[0] {
		case PublishMsg:
			if err := qs.handlePublishMsg(b[1:n]); err != nil {
				fmt.Println(err)
			}
		case ConsumeMsg:
			if err := qs.handleConsumeMsg(conn); err != nil {
				fmt.Println(err)
			}
		case AckMsg:
			if err := qs.handleAckMsg(b[1:n]); err != nil {
				fmt.Println(err)
			}
		case DisconnectMsg:
			return
		default:
			fmt.Printf("[QS]: unknown message type: %d\n", b[0])
		}
	}
}

func (qs *QueueServer) handlePublishMsg(b []byte) error {
	msg := &queue.Message{}
	if err := msg.Decode(b); err != nil {
		return err
	}
	fmt.Printf("[QS]: %+v\n", msg)
	if err := qs.queue.Enqueue(msg); err != nil {
		return err
	}
	return nil
}

func (qs *QueueServer) handleConsumeMsg(conn net.Conn) error {
	b := make([]byte, 4096)
	msg, err := qs.queue.Dequeue()
	if err != nil {
		binary.LittleEndian.PutUint16(b, uint16(EmptyQueueResp))
		_, err := conn.Write(b[:2])
		if err != nil {
			return err
		}
		return nil
	}

	if qs.mode == Persistent {
		qs.inFlightMsgs[msg.Id] = InFlightMessage{
			to:  conn,
			msg: msg,
		}
	}

	msgB, err := msg.ToBytes()
	if err != nil {
		return err
	}
	_, err = conn.Write(msgB)
	return err
}

func (qs *QueueServer) handleAckMsg(b []byte) error {
	qs.mtx.Lock()
	defer qs.mtx.Unlock()
	if qs.mode == NonPersistent {
		return fmt.Errorf("[QS]: received ack in non-persistent mode")
	}
	id := binary.LittleEndian.Uint64(b)

	if _, ok := qs.inFlightMsgs[int(id)]; !ok {
		return fmt.Errorf("[QS]: ack id doesn't exist")
	}

	delete(qs.inFlightMsgs, int(id))
	return nil
}
