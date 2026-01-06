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

func (wal *WriteAheadLog) RestoreState() ([][]byte, error) {
	result := make([][]byte, 0, 1024)
	segments, err := filepath.Glob(filepath.Join(wal.directory, SegmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	for _, segment := range segments {
		fmt.Println("[WAL]: Restoring from segment", segment)
		f, err := os.Open(segment)
		if err != nil {
			return nil, err
		}

		r := bufio.NewReader(f)
		entries, err := wal.readAll(r)
		if err != nil {
			switch {
			case errors.Is(err, ErrCorruptedFile):
				continue
			default:
				return nil, err
			}
		}
		result = append(result, entries...)

		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return result, nil
}
func (wal *WriteAheadLog) ReadAll() ([][]byte, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	f, err := os.Open(wal.currentSegment.Name())
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return wal.readAll(f)
}

func (wal *WriteAheadLog) Write(data []byte, isCheckpoint bool) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	return wal.write(data, isCheckpoint)
}
func (wal *WriteAheadLog) write(data []byte, isCheckpoint bool) error {

	wal.lastSequenceNo++
	entry := &WALEntry{
		lsn:          wal.lastSequenceNo,
		Data:         data,
		isCheckpoint: isCheckpoint,
	}

	if err := wal.writeEntry(wal.writerBuf, entry); err != nil {
		return err
	}

	if isCheckpoint {
		if err := wal.Sync(); err != nil {
			return err
		}
	}
	return nil
}
func (wal *WriteAheadLog) writeEntry(r io.Writer, entry *WALEntry) error {
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
	binary.Write(r, binary.LittleEndian, uint32(len(buf)))
	_, err = r.Write(buf)

	return err
}

func (wal *WriteAheadLog) Repair() error {
	f, err := os.Open(wal.currentSegment.Name())
	if err != nil {
		return err
	}
	entries := make([]*WALEntry, 0)
	for {
		entry, err := wal.readEntry(f)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return nil
			default:
				return wal.repairSegment(entries)
			}
		}
		entries = append(entries, entry)
	}

}

func (wal *WriteAheadLog) repairSegment(entries []*WALEntry) error {
	tempF, err := os.OpenFile(filepath.Join(wal.directory, "repair.tmp"), os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer tempF.Close()
	for _, entry := range entries {
		if err := wal.writeEntry(tempF, entry); err != nil {
			return err
		}
	}

	if err := os.Rename(tempF.Name(), wal.currentSegment.Name()); err != nil {
		return err
	}
	return nil
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
	wal.currentSegment, err = createSegmentFile(wal.directory, wal.lastSegmentIdx)
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

func (wal WriteAheadLog) readAll(f io.Reader) ([][]byte, error) {
	result := make([][]byte, 0, 1024)
	for {
		entry, err := wal.readEntry(f)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return result, nil
			default:
				return nil, err
			}
		}
		if len(entry.Data) == 0 && entry.isCheckpoint {
			continue
		}
		result = append(result, entry.Data)
	}
}

func (wal *WriteAheadLog) readEntry(r io.Reader) (*WALEntry, error) {
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	entryBuf := make([]byte, size)
	_, err := io.ReadFull(r, entryBuf)
	if err != nil {
		return nil, err
	}

	return entryFromBytes(entryBuf)

}

func (wal *WriteAheadLog) Compact(data [][]byte) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	if err := wal.Sync(); err != nil {
		return err
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	// clear segments
	wal.lastSegmentIdx++
	deleteBeforeIdx := wal.lastSegmentIdx
	newSegment, err := createSegmentFile(wal.directory, wal.lastSegmentIdx)
	if err != nil {
		return err
	}
	wal.currentSegment = newSegment
	wal.writerBuf = bufio.NewWriter(wal.currentSegment)

	// write checkpoint
	if err := wal.write(nil, true); err != nil {
		return err
	}

	for _, d := range data {
		if err := wal.write(d, false); err != nil {
			return err
		}
	}

	if err := wal.Sync(); err != nil {
		return err
	}
	n, err := deleteSegmentsBeforeIdx(wal.directory, deleteBeforeIdx)
	if err != nil {
		return err
	}

	fmt.Printf("[WAL]: Removed %d old segments\n", n)
	return nil
}

func (wal *WriteAheadLog) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.currentSegment.Close()
}
