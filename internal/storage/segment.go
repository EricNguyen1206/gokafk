package storage

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// headerSize is the size of each frame header in bytes:
// [Offset: 8 bytes][Timestamp: 8 bytes][Size: 4 bytes]
const headerSize = 20

// Segment implements the Store interface as an append-only log file
// with an in-memory sparse index for O(1) offset lookups.
type Segment struct {
	mu             sync.RWMutex
	file           *os.File
	path           string
	offset         int64
	index          map[int64]int64 // messageOffset -> bytePosition
	timestampIndex map[int64]int64 // messageOffset -> timestamp (Unix millis)
	bytePos        int64
	closed         bool
}

// Opens or creates a segment file for the given topic
// If the file already exists, the in-memory index is rebuilt by scanning the log.
func NewSegment(dir string, topicId uint16) (*Segment, error) {
	// Check folder exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("segment mkdir: %w", err)
	}
	// Open file for read+write+append+create
	filepath := fmt.Sprintf("%s/topic_%d.log", dir, topicId)
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("segment open: %w", err)
	}
	seg := &Segment{
		file:           file,
		path:           filepath,
		index:          make(map[int64]int64),
		timestampIndex: make(map[int64]int64),
	}

	// Rebuild index from existing data
	if err := seg.rebuildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("segment rebuild index: %w", err)
	}

	slog.Info("segment ready", "path", seg.path, "offset", seg.offset)
	return seg, nil
}

func (s *Segment) rebuildIndex() error {
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	var bytePos int64 = 0
	headerBuf := make([]byte, headerSize) // 8 bytes Offset + 8 bytes Timestamp + 4 bytes Size

	for {
		// Read header
		n, err := s.file.Read(headerBuf)
		if n == 0 || err != nil {
			break // EOF or error
		}
		if n < headerSize {
			break // corrupted file, stop rebuilding
		}

		// Parse header
		offset := int64(binary.BigEndian.Uint64(headerBuf[0:8]))
		timestamp := int64(binary.BigEndian.Uint64(headerBuf[8:16]))
		size := int32(binary.BigEndian.Uint32(headerBuf[16:20]))

		// Update indices
		s.index[offset] = bytePos
		s.timestampIndex[offset] = timestamp

		// Jump over payload to the next frame
		bytePos += int64(headerSize) + int64(size)
		if _, err := s.file.Seek(bytePos, 0); err != nil {
			break
		}

		s.offset = offset + 1
	}

	s.bytePos = bytePos

	// Seek to end of file for future appends
	if _, err := s.file.Seek(0, 2); err != nil {
		return err
	}

	return nil
}

// write data to the log and returns the asssigned offset
func (s *Segment) Append(data []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return -1, fmt.Errorf("segment closed")
	}

	offset := s.offset
	timestamp := time.Now().UnixMilli()

	// Build binary frame: [Offset: 8 bytes][Timestamp: 8 bytes][Size: 4 bytes][Payload]
	size := len(data)
	frame := make([]byte, headerSize+size)
	binary.BigEndian.PutUint64(frame[0:8], uint64(offset))
	binary.BigEndian.PutUint64(frame[8:16], uint64(timestamp))
	binary.BigEndian.PutUint32(frame[16:20], uint32(size))
	copy(frame[headerSize:], data)

	n, err := s.file.Write(frame)
	if err != nil {
		return -1, fmt.Errorf("segment append: %w", err)
	}

	s.index[offset] = s.bytePos
	s.timestampIndex[offset] = timestamp
	s.bytePos += int64(n)
	s.offset++

	return offset, nil
}

// Read retrieves the message at the given offset using the sparse index
func (s *Segment) Read(offset int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bytePos, ok := s.index[offset]
	if !ok {
		return nil, ErrOffsetNotFound
	}

	// Open a separate read handle to avoid seek conclicts with writes
	readFile, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("segment read open: %w", err)
	}
	defer readFile.Close()

	_, err = readFile.Seek(bytePos, 0)
	if err != nil {
		return nil, fmt.Errorf("segment read seek: %w", err)
	}

	headerBuf := make([]byte, headerSize) // 8 bytes Offset + 8 bytes Timestamp + 4 bytes Size
	if _, err := readFile.Read(headerBuf); err != nil {
		return nil, fmt.Errorf("segment read header: %w", err)
	}

	size := int32(binary.BigEndian.Uint32(headerBuf[16:20]))
	payload := make([]byte, size)
	if _, err := readFile.Read(payload); err != nil {
		return nil, fmt.Errorf("segment read payload: %w", err)
	}

	return payload, nil
}

// return the next offset that will be assigned to the next message
func (s *Segment) CurrentOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset
}

// TimestampAt returns the timestamp (Unix millis) of the message at the given offset.
func (s *Segment) TimestampAt(offset int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ts, ok := s.timestampIndex[offset]
	if !ok {
		return 0, ErrOffsetNotFound
	}
	return ts, nil
}

// FindOffsetByTimestamp returns the first offset whose timestamp >= ts (Unix millis).
// Returns -1 if no matching offset is found.
func (s *Segment) FindOffsetByTimestamp(ts int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.timestampIndex) == 0 {
		return -1, nil
	}

	// Collect and sort offsets for ordered traversal
	offsets := make([]int64, 0, len(s.timestampIndex))
	for o := range s.timestampIndex {
		offsets = append(offsets, o)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	// Binary search for first offset where timestamp >= ts
	idx := sort.Search(len(offsets), func(i int) bool {
		return s.timestampIndex[offsets[i]] >= ts
	})

	if idx < len(offsets) {
		return offsets[idx], nil
	}
	return -1, nil
}

// Close flushes and closed the segment file.
// Safe to call multiple times.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.file.Close()
}
