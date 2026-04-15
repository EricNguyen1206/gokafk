package storage

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

type Segment struct {
	file          *os.File
	currentOffset int
	mu            sync.Mutex
}

func NewSegment(dir string, topicId uint16) (*Segment, error) {
	// Check folder exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	// Open file
	filepath := fmt.Sprintf("%s/topic_%d.log", dir, topicId)
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	slog.Info("Created segment successfully", "filepath", filepath)
	return &Segment{
		file:          file,
		currentOffset: 0,
	}, nil
}

func (s *Segment) Append(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cleanData := bytes.TrimSpace(data)
	s.file.Write(cleanData)
	s.file.Write([]byte("\n"))
	s.currentOffset += len(cleanData) + 1
}

func (s *Segment) ReadOffset(offset int) ([]byte, error) {
	file, err := os.OpenFile(s.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	currentLine := 0

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if currentLine == offset {
			return scanner.Bytes(), nil
		}
		currentLine++
	}

	return nil, fmt.Errorf("offset not found")
}
