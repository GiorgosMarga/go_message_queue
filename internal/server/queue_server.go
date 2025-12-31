package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/queue"
)

type InFlightMessage struct {
	client       *Client
	msg          *queue.Message
	lastSendTry  time.Time
	totalRetries int
}

type QueueServer struct {
	listenAddr   string
	queue        queue.Queue
	mode         int
	inFlightMsgs map[int]*InFlightMessage
	mtx          *sync.Mutex
	ctx          context.Context
}

type Client struct {
	conn     net.Conn
	sendChan chan []byte
	quitChan chan struct{}
}

const (
	PublishMsg = iota
	ConsumeMsg
	DisconnectMsg
	AckMsg

	EmptyQueueResp

	NonPersistent
	Persistent
	WithPriority
)

func NewQueueServer(lAddr string, mode int) *QueueServer {
	if !strings.HasPrefix(lAddr, ":") {
		lAddr = ":" + lAddr
	}
	qs := &QueueServer{
		listenAddr: lAddr,
		mode:       mode,
		mtx:        &sync.Mutex{},
	}

	if mode == Persistent {
		qs.inFlightMsgs = make(map[int]*InFlightMessage)
	}
	qs.queue = queue.NewPriorityQueueWithCap(1024)
	return qs
}

func (qs *QueueServer) Start() error {
	ln, err := net.Listen("tcp", qs.listenAddr)
	if err != nil {
		return err
	}
	fmt.Printf("Queue server is listening on address %s...\n", qs.listenAddr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if qs.mode == Persistent {
		go qs.manageInFlightMsgs(ctx)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go qs.handleConn(conn)
	}
}

func (c *Client) writeLoop() {
	for {
		select {
		case b := <-c.sendChan:
			if _, err := c.conn.Write(b); err != nil {
				fmt.Println(err)
				continue
			}
		case <-c.quitChan:
			fmt.Println("Stopping write loop")
			return
		}
	}
}

func (qs *QueueServer) manageInFlightMsgs(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	timeout := 3 * time.Second
	maxRetries := 5
	for {
		select {
		case <-ticker.C:
			qs.mtx.Lock()
			msgs := make([]*InFlightMessage, 0, len(qs.inFlightMsgs))
			for _, msg := range qs.inFlightMsgs {
				if time.Since(msg.lastSendTry) < timeout {
					continue
				}
				if msg.totalRetries == maxRetries {
					continue
				}

				msg.lastSendTry = time.Now()
				msg.totalRetries += 1
				msgs = append(msgs, msg)
			}
			qs.mtx.Unlock()

			for _, msg := range msgs {
				fmt.Println("Resending message: ", msg.msg.Id)
				b, err := msg.msg.ToBytes()
				if err != nil {
					fmt.Println(err)
					continue
				}
				select {
				case msg.client.sendChan <- b:
				default:
					fmt.Println("Slow client should close connection")
				}
			}

		case <-ctx.Done():
			fmt.Println("Stop re-trying...")
			ticker.Stop()
			return nil
		}
	}
}

func (qs *QueueServer) handleConn(conn net.Conn) {
	client := &Client{
		conn:     conn,
		sendChan: make(chan []byte, 256),
		quitChan: make(chan struct{}),
	}
	defer func(c *Client) {
		close(c.quitChan)
		conn.Close()
	}(client)

	go client.writeLoop()

	fmt.Printf("[QS]: new connection: %v\n", conn.LocalAddr())
	msgHeader := make([]byte, 5)
	for {
		_, err := io.ReadFull(conn, msgHeader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
				return
			}
			fmt.Println("Header read:", err)
			continue
		}
		msgLen := binary.LittleEndian.Uint16(msgHeader)
		msgType := msgHeader[4]

		b := make([]byte, msgLen)

		if _, err = io.ReadFull(conn, b); err != nil {
			fmt.Println("Error reading: ", err)
			return
		}

		switch msgType {
		case PublishMsg:
			if err := qs.handlePublishMsg(b); err != nil {
				fmt.Println(err)
			}
		case ConsumeMsg:
			if err := qs.handleConsumeMsg(client); err != nil {
				fmt.Println(err)
			}
		case AckMsg:
			if err := qs.handleAckMsg(b); err != nil {
				fmt.Println(err)
			}
		case DisconnectMsg:
			return
		default:
			fmt.Printf("[QS]: unknown message type: %d\n", msgType)
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

func (qs *QueueServer) handleConsumeMsg(client *Client) error {
	msg, err := qs.queue.Dequeue()

	// 1. Handle Empty Queue
	if err != nil {
		resp := make([]byte, 2)
		binary.LittleEndian.PutUint16(resp, uint16(EmptyQueueResp))

		select {
		case client.sendChan <- resp:
		default:
			return fmt.Errorf("buffer overflow on empty response")
		}
		return nil
	}

	// 2. Thread-Safe In-Flight Registration
	if qs.mode == Persistent {
		qs.mtx.Lock()
		qs.inFlightMsgs[msg.Id] = &InFlightMessage{
			client:      client,
			msg:         msg,
			lastSendTry: time.Now(),
		}
		qs.mtx.Unlock()
	}

	msgB, err := msg.ToBytes()
	if err != nil {
		return err
	}

	select {
	case client.sendChan <- msgB:
		return nil
	default:
		// If we fail to send, we should probably remove it from inFlightMsgs
		// or the manager will keep trying to send to a dead client.
		qs.mtx.Lock()
		delete(qs.inFlightMsgs, msg.Id)
		qs.mtx.Unlock()

		return fmt.Errorf("buffer overflow, closing connection")
	}
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
