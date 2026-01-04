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

func createSegmentFile(directory string, segmentId int) (*os.File, error) {
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

func (entry *WALEntry) bytes() []byte {
	bufSize := 4 + 8 + 4 + len(entry.Data) + 1 + 4
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint32(buf, uint32(bufSize))
	binary.LittleEndian.PutUint64(buf[4:], entry.lsn)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(entry.Data)))
	copy(buf[16:], entry.Data)
	if entry.isCheckpoint {
		buf[16+len(entry.Data)] = 1
	} else {
		buf[16+len(entry.Data)] = 0
	}
	entry.crc = crc32.ChecksumIEEE(buf[:16+len(entry.Data)+1])
	binary.LittleEndian.PutUint32(buf[16+len(entry.Data)+1:], entry.crc)
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
	checksum := crc32.ChecksumIEEE(buf[:12+dataSize])
	entry.crc = binary.LittleEndian.Uint32(buf[12+1+dataSize:])
	if entry.crc != checksum {
		fmt.Println("wrong checksum")
		return nil, fmt.Errorf("wrong checksum")
	}
	return entry, nil
}
