package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
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
	f   *os.File
	mtx *sync.Mutex
}

func NewWAL(filePath string) (*WriteAheadLog, error) {
	f, err := os.OpenFile(fmt.Sprintf("log/%s", filePath), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{
		f:   f,
		mtx: &sync.Mutex{},
	}, nil
}

func (wal *WriteAheadLog) Sync() ([]byte, error) {
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
	return wal.Close()
}
