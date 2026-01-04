package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

const (
	SyncInterval  = 500 * time.Millisecond
	SegmentPrefix = "segment-"
)

type DataProvider interface {
	GetLiveMessages() []*message.Message
}

type WALEntry struct {
	// Log Serial Number
	lsn  uint64
	Data []byte
	// Checksum
	crc          uint32
	isCheckpoint bool
}
type WriteAheadLog struct {
	directory        string
	lastSegmentIdx   uint64
	lastSequenceNo   uint64
	currentSegment   *os.File
	writerBuf        *bufio.Writer
	mtx              *sync.Mutex
	maxSegmentSize   int
	maxTotalSegments int
	shouldFsync      bool
	fsyncTimer       *time.Timer
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewWAL(path string, maxSegmentSize, maxTotalSegments int, shouldFsync bool) (*WriteAheadLog, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	segments, err := filepath.Glob(filepath.Join(path, SegmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentId uint64
	if len(segments) == 0 {
		f, err := createSegmentFile(path, 0)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	} else {
		lastSegmentId, err = findLastIdxFromSegments(segments)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("%s%d", SegmentPrefix, lastSegmentId)), os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	timer := time.NewTimer(SyncInterval)
	ctx, cancel := context.WithCancel(context.Background())
	wal := &WriteAheadLog{
		currentSegment:   f,
		writerBuf:        bufio.NewWriter(f),
		directory:        path,
		lastSegmentIdx:   lastSegmentId,
		lastSequenceNo:   0,
		mtx:              &sync.Mutex{},
		maxSegmentSize:   maxSegmentSize,
		maxTotalSegments: maxTotalSegments,
		shouldFsync:      shouldFsync,
		fsyncTimer:       timer,
		ctx:              ctx,
		cancel:           cancel,
	}

	go wal.syncLoop()
	return wal, nil
}

func (wal *WriteAheadLog) ReadAll() ([][]byte, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	result := make([][]byte, 0, 1024)
	headerBuf := make([]byte, 4)
	for {
		_, err := wal.currentSegment.Read(headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			return nil, err
		}
		entrySize := binary.LittleEndian.Uint32(headerBuf)
		entryBuf := make([]byte, entrySize)
		_, err = wal.currentSegment.Read(entryBuf)
		if err != nil {
			return nil, err
		}
		entry, err := entryFromBytes(entryBuf)
		if err != nil {
			return nil, err
		}
		result = append(result, entry.Data)
	}
}

func (wal *WriteAheadLog) Write(data []byte, isCheckpoint bool) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	wal.lastSequenceNo++
	entry := &WALEntry{
		lsn:          wal.lastSequenceNo,
		Data:         data,
		isCheckpoint: isCheckpoint,
	}

	buf := entry.bytes()

	fit, err := wal.fitInSegment(len(buf))
	if err != nil {
		return err
	}
	if !fit {
		if err := wal.rotateSegment(); err != nil {
			return err
		}
	}

	_, err = wal.writerBuf.Write(buf)
	return err
}
func (wal *WriteAheadLog) rotateSegment() error {
	segments, err := filepath.Glob(filepath.Join(wal.directory, SegmentPrefix+"*"))
	if err != nil {
		fmt.Println(err)
		return err
	}
	if len(segments) == wal.maxTotalSegments {
		if err := deleteOldestSegment(wal.directory, segments); err != nil {
			return err
		}
	}
	if err := wal.Close(); err != nil {
		return err
	}

	wal.lastSegmentIdx++
	wal.currentSegment, err = createSegmentFile(wal.directory, int(wal.lastSegmentIdx))
	if err != nil {
		return err
	}
	wal.writerBuf = bufio.NewWriter(wal.currentSegment)
	return err
}
func (wal *WriteAheadLog) syncLoop() {
	for {
		select {
		case <-wal.fsyncTimer.C:
			wal.mtx.Lock()
			if err := wal.Sync(); err != nil {
				fmt.Println("[SyncLoop]:", err)
			}
			wal.mtx.Unlock()

		case <-wal.ctx.Done():
			fmt.Println("[SyncLoop]: Done")
			return
		}
	}

}

func (wal *WriteAheadLog) Sync() error {
	if err := wal.writerBuf.Flush(); err != nil {
		return err
	}

	if wal.shouldFsync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
	}
	wal.fsyncTimer.Reset(SyncInterval)
	return nil
}

func (wal *WriteAheadLog) fitInSegment(newEntrySize int) (bool, error) {
	stat, err := wal.currentSegment.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size()+int64(newEntrySize) >= int64(wal.maxSegmentSize) {
		return false, nil
	}
	return true, nil
}

func (wal *WriteAheadLog) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.currentSegment.Close()
}
