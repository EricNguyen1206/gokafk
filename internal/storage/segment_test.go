package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestNewSegment(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Test creating a new segment
	topicID := uint16(1)
	segment, err := NewSegment(tempDir, topicID)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.file.Close()

	// Verify the segment was created
	filepath := fmt.Sprintf("%s/topic_%d.log", tempDir, topicID)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.Errorf("Segment file does not exist: %s", filepath)
	}

	// Verify the segment has the correct offset
	if segment.currentOffset != 0 {
		t.Errorf("Expected offset 0, got %d", segment.currentOffset)
	}
}

func TestSegment_Append(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Test appending data to a segment
	topicID := uint16(1)
	segment, err := NewSegment(tempDir, topicID)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.file.Close()

	// Test appending data
	data := []byte("hello")
	segment.Append(data)

	// Verify the data was appended
	filepath := fmt.Sprintf("%s/topic_%d.log", tempDir, topicID)
	file, err := os.Open(filepath)
	if err != nil {
		t.Fatalf("Failed to open segment file: %v", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read segment file: %v", err)
	}

	expectedContent := append(data, '\n')
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("Expected content %q, got %q", expectedContent, content)
	}

	// Verify the segment offset was updated
	if segment.currentOffset != len(expectedContent) {
		t.Errorf("Expected offset %d, got %d", len(expectedContent), segment.currentOffset)
	}
}
