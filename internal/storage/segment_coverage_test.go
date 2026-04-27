package storage

import (
	"testing"
	"time"
)

func TestSegment_TimestampAt(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	_, err = seg.Append([]byte("msg-with-timestamp"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	ts, err := seg.TimestampAt(0)
	if err != nil {
		t.Fatalf("TimestampAt: %v", err)
	}

	now := time.Now().UnixMilli()
	if ts <= 0 {
		t.Errorf("timestamp should be positive, got %d", ts)
	}
	if diff := now - ts; diff > 1000 {
		t.Errorf("timestamp too far from now: diff=%dms", diff)
	}

	_, err = seg.TimestampAt(999)
	if err != ErrOffsetNotFound {
		t.Errorf("non-existent offset: want ErrOffsetNotFound, got %v", err)
	}
}

func TestSegment_FindOffsetByTimestamp(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	// Append messages with known timing
	_, err = seg.Append([]byte("msg-0"))
	if err != nil {
		t.Fatalf("Append[0]: %v", err)
	}
	ts0, _ := seg.TimestampAt(0)

	time.Sleep(10 * time.Millisecond)
	_, err = seg.Append([]byte("msg-1"))
	if err != nil {
		t.Fatalf("Append[1]: %v", err)
	}
	ts1, _ := seg.TimestampAt(1)

	time.Sleep(10 * time.Millisecond)
	_, err = seg.Append([]byte("msg-2"))
	if err != nil {
		t.Fatalf("Append[2]: %v", err)
	}
	ts2, _ := seg.TimestampAt(2)

	// Query for earliest — should return 0
	off, err := seg.FindOffsetByTimestamp(ts0)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp(ts0): %v", err)
	}
	if off != 0 {
		t.Errorf("FindOffsetByTimestamp(ts0): want 0, got %d", off)
	}

	// Query for ts1 — should return 1
	off, err = seg.FindOffsetByTimestamp(ts1)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp(ts1): %v", err)
	}
	if off != 1 {
		t.Errorf("FindOffsetByTimestamp(ts1): want 1, got %d", off)
	}

	// Query for ts2 — should return 2
	off, err = seg.FindOffsetByTimestamp(ts2)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp(ts2): %v", err)
	}
	if off != 2 {
		t.Errorf("FindOffsetByTimestamp(ts2): want 2, got %d", off)
	}

	// Query before all timestamps — should return 0
	off, err = seg.FindOffsetByTimestamp(ts0 - 10000)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp(before all): %v", err)
	}
	if off != 0 {
		t.Errorf("FindOffsetByTimestamp(before all): want 0, got %d", off)
	}

	// Query after all timestamps — should return -1
	off, err = seg.FindOffsetByTimestamp(ts2 + 10000)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp(after all): %v", err)
	}
	if off != -1 {
		t.Errorf("FindOffsetByTimestamp(after all): want -1, got %d", off)
	}
}

func TestSegment_FindOffsetByTimestamp_Empty(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	off, err := seg.FindOffsetByTimestamp(0)
	if err != nil {
		t.Fatalf("FindOffsetByTimestamp on empty: %v", err)
	}
	if off != -1 {
		t.Errorf("empty segment: want -1, got %d", off)
	}
}

func TestSegment_NewSegment_ReopenPreservesData(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 2)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := seg.Append([]byte("data")); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	seg.Close()

	seg2, err := NewSegment(dir, 2)
	if err != nil {
		t.Fatalf("reopen NewSegment: %v", err)
	}
	defer seg2.Close()

	if seg2.CurrentOffset() != 5 {
		t.Errorf("after reopen: want offset 5, got %d", seg2.CurrentOffset())
	}

	// New append should continue from offset 5
	off, err := seg2.Append([]byte("continued"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if off != 5 {
		t.Errorf("Append after reopen offset: want 5, got %d", off)
	}

	// Old data still readable
	data, err := seg2.Read(0)
	if err != nil {
		t.Fatalf("Read old data: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("Read old data: want 'data', got %q", string(data))
	}
}

func TestSegment_AppendAfterClose(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	seg.Close()

	_, err = seg.Append([]byte("should fail"))
	if err == nil {
		t.Error("Append after Close should return error")
	}
}
