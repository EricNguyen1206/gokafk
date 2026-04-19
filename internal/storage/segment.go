package storage

// TODO [Phase 1 - Task 6]: Rewrite Segment to implement Store interface
// See: docs/plans/2026-04-18-phase1-foundation-refactor.md — Task 6
//
// Changes needed:
// 1. Replace sync.Mutex → sync.RWMutex (reads don't block each other)
// 2. Add sparse index: map[int64]int64 (messageOffset → bytePosition in file)
// 3. Append() returns (int64, error) instead of void — return assigned offset
// 4. Read() uses index for O(1) lookup instead of scanning from line 0
// 5. Add rebuildIndex() — scan file on startup to reconstruct index
// 6. Add Close() — idempotent, track closed state
// 7. Add CurrentOffset() int64
// 8. Open file with O_RDWR (not O_WRONLY) — needed for rebuild
// 9. All errors must be returned, never ignored
// 10. Ensure Segment satisfies Store interface: var _ Store = (*Segment)(nil)
//
// Write tests in segment_test.go:
// - TestSegment_AppendAndRead (verify offset return + O(1) read)
// - TestSegment_ReadOffsetNotFound
// - TestSegment_ConcurrentAppend (race detector)
// - TestSegment_CloseIdempotent
// - TestSegment_RebuildIndex (close + reopen, verify data intact)

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

const (
	SEGMENT_SIZE = 1024 * 1024 * 1024 // 1GB
)

// Segment implements the Store interface as an append-only log file
// with an in-memory sparse index for O(1) offset lookups.
type Segment struct {
	mu      sync.RWMutex
	file    *os.File
	path    string
	offset  int64
	index   map[int64]int64 // messageOffset -> bytePosition
	bytePos int64
	closed  bool
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
		file:  file,
		path:  filepath,
		index: make(map[int64]int64),
	}

	// Rebuild index from existing data
	if err := seg.rebuildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("segment rebuild index: %w", err)
	}

	slog.Info("segment ready", "path", seg.path, "offset", seg.offset)
	return seg, nil
}

// rebuildIndex scans the log file line-by-line to reconstruct the in-memory index of segment.
func (s *Segment) rebuildIndex() error {
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(s.file)
	var bytePos int64 = 0
	for scanner.Scan() {
		line := scanner.Bytes()
		s.index[s.offset] = bytePos
		// +1 for the newline character
		bytePos += int64(len(line)) + 1
		s.offset++
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

	cleanData := bytes.TrimSpace(data)
	line := append(cleanData, '\n')

	n, err := s.file.Write(line)

	if err != nil {
		return -1, fmt.Errorf("segment append: %w", err)
	}

	offset := s.offset
	s.index[offset] = s.bytePos
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

	scanner := bufio.NewScanner(readFile)
	if !scanner.Scan() {
		return nil, fmt.Errorf("segment scan: %w", scanner.Err())
	}

	return scanner.Bytes(), nil
}

// return the next offset that will be assigned to the next message
func (s *Segment) CurrentOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset
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
