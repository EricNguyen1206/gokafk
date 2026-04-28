package broker

import (
	"testing"

	"gokafk/internal/config"
)

func TestGetOrCreateTopicWithPartitions_CustomCount(t *testing.T) {
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       t.TempDir(),
		NumPartitions: 3,
	})

	tp, err := b.getOrCreateTopicWithPartitions("orders", 5)
	if err != nil {
		t.Fatalf("getOrCreateTopicWithPartitions: %v", err)
	}
	if tp.NumPartitions() != 5 {
		t.Errorf("partitions: want 5, got %d", tp.NumPartitions())
	}
}

func TestGetOrCreateTopicWithPartitions_DefaultFallback(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BrokerPort = 0
	cfg.DataDir = t.TempDir()
	b := NewBroker(cfg)

	tp, err := b.getOrCreateTopic("legacy-topic")
	if err != nil {
		t.Fatalf("getOrCreateTopic: %v", err)
	}
	if tp.NumPartitions() != config.DefaultNumPartitions {
		t.Errorf("partitions: want %d (config default), got %d", config.DefaultNumPartitions, tp.NumPartitions())
	}
}

func TestGetOrCreateTopicWithPartitions_InvalidCount(t *testing.T) {
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       t.TempDir(),
		NumPartitions: 3,
	})

	_, err := b.getOrCreateTopicWithPartitions("bad-topic", 0)
	if err == nil {
		t.Error("expected error for partition count 0")
	}

	_, err = b.getOrCreateTopicWithPartitions("bad-topic", -1)
	if err == nil {
		t.Error("expected error for negative partition count")
	}
}

func TestGetOrCreateTopicWithPartitions_SameTopicReturnsExisting(t *testing.T) {
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       t.TempDir(),
		NumPartitions: 3,
	})

	tp1, _ := b.getOrCreateTopicWithPartitions("orders", 7)
	tp2, _ := b.getOrCreateTopicWithPartitions("orders", 5) // different count, should return existing

	if tp1 != tp2 {
		t.Error("should return same topic instance on repeated call")
	}
	if tp2.NumPartitions() != 7 {
		t.Errorf("should keep original partition count 7, got %d", tp2.NumPartitions())
	}
}

func TestTopic_PartitionRouting_With5Partitions(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("test-5", dir, 5)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	// Round-robin should cover all 5 partitions
	seen := make(map[int]bool)
	for i := 0; i < 10; i++ {
		p := tp.PartitionFor(nil)
		if p < 0 || p >= 5 {
			t.Errorf("partition out of range [0,5): %d", p)
		}
		seen[p] = true
	}
	if len(seen) != 5 {
		t.Errorf("round-robin didn't cover all 5 partitions: saw %d", len(seen))
	}
}

func TestTopic_PartitionRouting_With1Partition(t *testing.T) {
	dir := t.TempDir()
	tp, err := NewTopic("test-1", dir, 1)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	defer tp.Close()

	// Everything should go to partition 0
	for i := 0; i < 10; i++ {
		p := tp.PartitionFor([]byte("any-key"))
		if p != 0 {
			t.Errorf("with 1 partition, all messages should go to 0, got %d", p)
		}
	}
}
