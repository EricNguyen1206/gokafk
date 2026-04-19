package storage

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	if seg.CurrentOffset() != 0 {
		t.Errorf("initial offset: want 0, got %d", seg.CurrentOffset())
	}
}

func TestSegment_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	messages := []string{"hello", "world", "gokafk"}

	// Append
	for i, msg := range messages {
		offset, err := seg.Append([]byte(msg))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if offset != int64(i) {
			t.Errorf("Append[%d] offset: want %d, got %d", i, i, offset)
		}
	}

	// Read back
	for i, want := range messages {
		data, err := seg.Read(int64(i))
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		if string(data) != want {
			t.Errorf("Read[%d]: want %q, got %q", i, want, string(data))
		}
	}
}

func TestSegment_ReadOffsetNotFound(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	_, err = seg.Read(999)
	if err != ErrOffsetNotFound {
		t.Errorf("want ErrOffsetNotFound, got %v", err)
	}
}

func TestSegment_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := seg.Append([]byte(fmt.Sprintf("msg-%d", i)))
			if err != nil {
				t.Errorf("concurrent Append[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if seg.CurrentOffset() != int64(n) {
		t.Errorf("after %d appends: want offset %d, got %d", n, n, seg.CurrentOffset())
	}

	// Verify all readable
	for i := int64(0); i < int64(n); i++ {
		_, err := seg.Read(i)
		if err != nil {
			t.Errorf("Read[%d] after concurrent append: %v", i, err)
		}
	}
}

func TestSegment_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic or error
	if err := seg.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSegment_RebuildIndex(t *testing.T) {
	dir := t.TempDir()

	// Write some data
	seg1, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	messages := []string{"alpha", "beta", "gamma"}
	for _, msg := range messages {
		if _, err := seg1.Append([]byte(msg)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	seg1.Close()

	// Re-open — should rebuild index from file
	seg2, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("re-open NewSegment: %v", err)
	}
	defer seg2.Close()

	if seg2.CurrentOffset() != 3 {
		t.Errorf("rebuilt offset: want 3, got %d", seg2.CurrentOffset())
	}

	for i, want := range messages {
		data, err := seg2.Read(int64(i))
		if err != nil {
			t.Fatalf("Read[%d] after rebuild: %v", i, err)
		}
		if string(data) != want {
			t.Errorf("Read[%d] after rebuild: want %q, got %q", i, want, string(data))
		}
	}
}
