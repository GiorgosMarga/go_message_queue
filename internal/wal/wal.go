package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

const (
	HeaderSize = 3 // msg type(u8) + msg length(u16)
	IDSize     = 8 // msg type(u8) + msg length(u16)
)
const (
	EnqueueType = iota
	AckType
)

var (
	ErrCorruptedFile = errors.New("corrupted file")
)

type WriteAheadLog struct {
	f        *os.File
	mtx      *sync.Mutex
	quitChan chan struct{}
}

func NewWAL(filePath string) (*WriteAheadLog, error) {
	f, err := os.OpenFile(fmt.Sprintf("log/%s", filePath), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	wal := &WriteAheadLog{
		f:        f,
		mtx:      &sync.Mutex{},
		quitChan: make(chan struct{}),
	}
	go wal.compact()
	return wal, nil
}
func (wal *WriteAheadLog) SyncMessagesFromLog() (map[uint64]*message.Message, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	msgs := make(map[uint64]*message.Message)

	r := bufio.NewReader(wal.f)
	header := make([]byte, HeaderSize)
	for {
		_, err := r.Read(header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, ErrCorruptedFile
		}

		switch header[0] {
		case AckType:
			idBuf := make([]byte, 8)
			_, err := r.Read(idBuf)
			if err != nil {
				return nil, ErrCorruptedFile
			}
			id := binary.LittleEndian.Uint64(idBuf)
			_, ok := msgs[id]
			if !ok {
				return nil, ErrCorruptedFile
			}
			delete(msgs, id)
		case EnqueueType:
			msgSize := binary.LittleEndian.Uint16(header[1:])
			msgBuf := make([]byte, msgSize)
			n, err := r.Read(msgBuf)
			if err != nil || n != int(msgSize) {
				return nil, ErrCorruptedFile
			}
			newMsg := &message.Message{}
			if err := newMsg.Decode(msgBuf); err != nil {
				return nil, ErrCorruptedFile
			}
			msgs[uint64(newMsg.Id)] = newMsg
		default:
			return nil, ErrCorruptedFile
		}
	}
	return msgs, nil
}
func (wal *WriteAheadLog) compact() {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msgs, err := wal.SyncMessagesFromLog()
			if err != nil {
				fmt.Println(err)
				continue
			}

			wal.mtx.Lock()
			defer wal.mtx.Unlock()

			tFile, err := os.Create("/log/file.temp")
			if err != nil {
				fmt.Println(err)
				continue
			}

			for _, msg := range msgs {
				bufMsg, _ := msg.ToBytes()
				buf := make([]byte, len(bufMsg)+HeaderSize)
				buf[0] = EnqueueType
				binary.LittleEndian.PutUint16(buf, uint16(len(bufMsg)))
				copy(buf[HeaderSize:], bufMsg)
				_, err := tFile.Write(buf)
				if err != nil {
					fmt.Println(err)
				}
			}

			if err := tFile.Sync(); err != nil {
				fmt.Println(err)
				continue
			}

			if err := wal.f.Close(); err != nil {
				fmt.Println(err)
				continue
			}

			if err := os.Remove("/log/file.mq"); err != nil {
				fmt.Println(err)
				continue
			}

			if err := os.Rename("/log/file.temp", "/log/file.mq"); err != nil {
				fmt.Println(err)
				continue
			}

			wal.f = tFile

		case <-wal.quitChan:
			fmt.Println("Stopping compact go-routine")
			return
		}
	}

}

func (wal *WriteAheadLog) ReadAll() ([]byte, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	return io.ReadAll(wal.f)
}
func (wal *WriteAheadLog) Write(b []byte) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	buf := make([]byte, len(b)+HeaderSize)
	// write header
	buf[0] = EnqueueType
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(b)))
	// copy body
	copy(buf[HeaderSize:], b)
	if _, err := wal.f.Write(buf); err != nil {
		return err
	}
	return wal.f.Sync()
}
func (wal *WriteAheadLog) Ack(msgID int) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	buf := make([]byte, IDSize+HeaderSize)
	// write header
	buf[0] = AckType
	binary.LittleEndian.PutUint64(buf[HeaderSize:], uint64(msgID))
	if _, err := wal.f.Write(buf); err != nil {
		return err
	}
	return wal.f.Sync()
}

func (wal *WriteAheadLog) Close() error {
	close(wal.quitChan)

	// to ensure not in the middle of a write
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	return wal.Close()
}
