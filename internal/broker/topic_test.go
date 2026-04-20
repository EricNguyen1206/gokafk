package broker

import (
	"fmt"
	"testing"

	"gokafk/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		BrokerPort:    0,
		DataDir:       "",
		NumPartitions: 3,
	}
}

func TestTopic_PartitionForKey_Deterministic(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("topic-1", dir, 3)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	key := []byte("user-123")

	// Same key must always route to same partition
	p1 := tp.PartitionFor(key)
	p2 := tp.PartitionFor(key)
	p3 := tp.PartitionFor(key)

	if p1 != p2 || p2 != p3 {
		t.Errorf("same key different partitions: %d, %d, %d", p1, p2, p3)
	}

	// Must be in range [0, numPartitions)
	if p1 < 0 || p1 >= 3 {
		t.Errorf("partition out of range: %d", p1)
	}
}

func TestTopic_PartitionForKey_Distribution(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("topic-1", dir, 3)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	counts := make(map[int]int)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		p := tp.PartitionFor(key)
		counts[p]++
	}

	// All 3 partitions should have at least some messages
	for p := 0; p < 3; p++ {
		if counts[p] == 0 {
			t.Errorf("partition %d got 0 messages — poor distribution", p)
		}
	}
}

func TestTopic_PartitionForNilKey_RoundRobin(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("topic-1", dir, 3)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	seen := make(map[int]bool)
	for i := 0; i < 6; i++ {
		p := tp.PartitionFor(nil)
		seen[p] = true
	}

	// After 6 round-robin iterations on 3 partitions, should have seen all 3
	if len(seen) != 3 {
		t.Errorf("round-robin didn't cover all partitions: saw %d", len(seen))
	}
}

func TestTopic_AppendToPartition(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("topic-1", dir, 3)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	partID := tp.PartitionFor([]byte("test-key"))
	offset, err := tp.AppendToPartition(partID, []byte("hello"))
	if err != nil {
		t.Fatalf("AppendToPartition: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset: want 0, got %d", offset)
	}

	data, err := tp.ReadFromPartition(partID, 0)
	if err != nil {
		t.Fatalf("ReadFromPartition: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("ReadFromPartition: want %q, got %q", "hello", string(data))
	}
}
