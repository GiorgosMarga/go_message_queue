package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func createSegmentFile(directory string, segmentId uint64) (*os.File, error) {
	filename := fmt.Sprintf("%s%d", SegmentPrefix, segmentId)
	return os.Create(filepath.Join(directory, filename))
}

func findLastIdxFromSegments(segments []string) (uint64, error) {
	var lastIdx uint64 = 0
	for _, segmentName := range segments {
		id := strings.Split(segmentName, "-")[1]

		num, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return 0, err
		}
		lastIdx = max(lastIdx, num)
	}
	return lastIdx, nil
}

func findOldestSegmentIdx(segments []string) (uint64, error) {
	if len(segments) == 0 {
		return 0, fmt.Errorf("no segments")
	}
	var oldestSegment uint64 = math.MaxUint64

	for _, segment := range segments {
		id, err := strconv.ParseUint(strings.Split(segment, "-")[1], 10, 64)
		if err != nil {
			return 0, err
		}
		oldestSegment = min(oldestSegment, id)
	}
	return oldestSegment, nil

}
func deleteOldestSegment(directory string, segments []string) error {
	oldestSegmentIdx, err := findOldestSegmentIdx(segments)
	if err != nil {
		return err
	}

	if err := os.Remove(filepath.Join(directory, fmt.Sprintf("%s%d", SegmentPrefix, oldestSegmentIdx))); err != nil {
		return err
	}
	return nil
}

func deleteSegmentsBeforeIdx(directory string, idx uint64) (int, error) {
	deleted := 0

	segments, err := filepath.Glob(filepath.Join(directory, SegmentPrefix+"*"))
	if err != nil {
		return -1, err
	}

	for _, segment := range segments {
		segmentIdx, err := strconv.ParseUint(strings.Split(segment, "-")[1], 10, 64)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if segmentIdx < idx {
			if err := os.Remove(segment); err != nil {
				return -1, err
			}
		}
		deleted++
	}
	return deleted, nil
}

func (entry *WALEntry) bytes() []byte {
	// lsn + len(data) + ischeckpoint + crc
	bufSize := 8 + 4 + len(entry.Data) + 1 + 4
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint64(buf, entry.lsn)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(entry.Data)))
	copy(buf[12:], entry.Data)
	if entry.isCheckpoint {
		buf[12+len(entry.Data)] = 1
	} else {
		buf[12+len(entry.Data)] = 0
	}
	entry.crc = crc32.ChecksumIEEE(buf[:12+len(entry.Data)+1])
	binary.LittleEndian.PutUint32(buf[12+len(entry.Data)+1:], entry.crc)
	return buf
}

func entryFromBytes(buf []byte) (*WALEntry, error) {
	entry := &WALEntry{}
	entry.lsn = binary.LittleEndian.Uint64(buf)
	dataSize := binary.LittleEndian.Uint32(buf[8:])
	dataBuf := make([]byte, dataSize)
	copy(dataBuf, buf[12:12+dataSize])
	entry.Data = dataBuf
	if buf[12+dataSize] == 1 {
		entry.isCheckpoint = true
	} else {
		entry.isCheckpoint = false
	}
	checksum := crc32.ChecksumIEEE(buf[:12+dataSize+1])
	entry.crc = binary.LittleEndian.Uint32(buf[12+1+dataSize:])
	if entry.crc != checksum {
		return nil, fmt.Errorf("wrong checksum")
	}
	return entry, nil
}
