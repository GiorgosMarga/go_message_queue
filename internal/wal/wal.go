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

type DataProvider interface {
	GetLiveMessages() []*message.Message
}

type WriteAheadLog struct {
	f        *os.File
	mtx      *sync.Mutex
	provider DataProvider
	quitChan chan struct{}
}

func NewWAL(filePath string, dp DataProvider) (*WriteAheadLog, error) {
	f, err := os.OpenFile(fmt.Sprintf("log/%s", filePath), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	wal := &WriteAheadLog{
		f:        f,
		mtx:      &sync.Mutex{},
		quitChan: make(chan struct{}),
		provider: dp,
	}
	go wal.compact()
	return wal, nil
}
func (wal *WriteAheadLog) SyncMessagesFromLog() (map[uint64]*message.Message, error) {
	readerFile, err := os.Open(wal.f.Name())
	if err != nil {
		return nil, err
	}

	msgs := make(map[uint64]*message.Message)

	r := bufio.NewReader(readerFile)
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
				fmt.Println(err)

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
			n, err := io.ReadFull(r, msgBuf)
			if err != nil || n != int(msgSize) {
				return nil, ErrCorruptedFile
			}
			newMsg := &message.Message{}
			if err := newMsg.Decode(msgBuf); err != nil {
				fmt.Println(err)
				return nil, ErrCorruptedFile
			}
			fmt.Println(newMsg)
			msgs[uint64(newMsg.Id)] = newMsg
		default:
			return nil, ErrCorruptedFile
		}
	}
	return msgs, nil
}

func (wal *WriteAheadLog) appendData(data []byte, logType int) error {
	buf := make([]byte, HeaderSize+len(data))
	buf[0] = byte(logType)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(data)))
	copy(buf[HeaderSize:], data)
	_, err := wal.f.Write(buf)
	return err
}
func (wal *WriteAheadLog) compact() {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// continue
			stat, _ := wal.f.Stat()
			if stat.Size() < (1 << 10) {
				fmt.Printf("Skipping because size (%d) < %d\n", stat.Size(), 1<<10)
				continue
			}
			msgs := wal.provider.GetLiveMessages()
			wal.performSwap(msgs)
		case <-wal.quitChan:
			fmt.Println("Stopping compact go-routine")
			return
		}
	}

}

func (wal *WriteAheadLog) performSwap(msgs []*message.Message) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	fmt.Println("Performing swap")

	tempPath := "log/file.temp"
	finalPath := "log/file.mq"

	oldStat, err := wal.f.Stat()
	if err != nil {
		fmt.Println("Compaction failed to get stats of the old file:", err)
		return
	}
	oldSize := oldStat.Size()

	tFile, err := os.Create(tempPath)
	if err != nil {
		fmt.Println("Compaction failed to create temp file:", err)
		return
	}

	for _, msg := range msgs {
		bufMsg, _ := msg.ToBytes()
		buf := make([]byte, len(bufMsg)+HeaderSize)
		buf[0] = EnqueueType
		binary.LittleEndian.PutUint16(buf[1:], uint16(len(bufMsg)))
		copy(buf[HeaderSize:], bufMsg)
		_, err := tFile.Write(buf)
		if err != nil {
			fmt.Println("Compaction failed to write buffer:", err)
			return
		}
	}

	if err := tFile.Sync(); err != nil {
		fmt.Println("Compaction failed to sync temp file:", err)
		return
	}

	if err := wal.f.Close(); err != nil {
		fmt.Println("Compaction failed to close original file:", err)
		return
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		fmt.Println("Compaction failed during rename:", err)
		wal.f, _ = os.OpenFile(finalPath, os.O_APPEND|os.O_RDWR, 0o666)
		return
	}

	newFile, err := os.OpenFile(finalPath, os.O_APPEND|os.O_RDWR, 0o666)
	if err != nil {
		fmt.Println("Compaction failed to reopen log: ", err)
		return
	}

	wal.f = newFile
	newStat, err := wal.f.Stat()
	if err != nil {
		fmt.Println("Compaction failed to stat the new log: ", err)
		return
	}
	fmt.Printf("Compaction successful (%d->%d)\n", oldSize, newStat.Size())
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
