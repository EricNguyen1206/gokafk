package broker

import (
	"testing"
)

func TestParseOffsetRecord(t *testing.T) {
	// Build a valid record: [KeyLen(4)][Key][ValLen(4)][Val]
	key := "my-group:orders:0"
	val := "42"
	data := make([]byte, 0, 8+len(key)+len(val))
	data = append(data, uint32Bytes(uint32(len(key)))...)
	data = append(data, []byte(key)...)
	data = append(data, uint32Bytes(uint32(len(val)))...)
	data = append(data, []byte(val)...)

	parsedKey, parsedVal, err := parseOffsetRecord(data)
	if err != nil {
		t.Fatalf("parseOffsetRecord: %v", err)
	}
	if parsedKey != key {
		t.Errorf("key: want %q, got %q", key, parsedKey)
	}
	if parsedVal != val {
		t.Errorf("val: want %q, got %q", val, parsedVal)
	}
}

func TestParseOffsetRecord_Errors(t *testing.T) {
	_, _, err := parseOffsetRecord([]byte{})
	if err == nil {
		t.Error("empty data should error")
	}

	_, _, err = parseOffsetRecord([]byte{0, 0, 0}) // 3 bytes < 4
	if err == nil {
		t.Error("too short for key length should error")
	}

	// Key length says 100 but only 4 bytes total
	_, _, err = parseOffsetRecord([]byte{0, 0, 0, 100})
	if err == nil {
		t.Error("truncated key should error")
	}
}

func TestParseOffsetKey(t *testing.T) {
	groupID, topicName, partition, offset, err := parseOffsetKey("my-group:orders:2", "1024")
	if err != nil {
		t.Fatalf("parseOffsetKey: %v", err)
	}
	if groupID != "my-group" {
		t.Errorf("groupID: want 'my-group', got %q", groupID)
	}
	if topicName != "orders" {
		t.Errorf("topicName: want 'orders', got %q", topicName)
	}
	if partition != 2 {
		t.Errorf("partition: want 2, got %d", partition)
	}
	if offset != 1024 {
		t.Errorf("offset: want 1024, got %d", offset)
	}
}

func TestParseOffsetKey_Errors(t *testing.T) {
	_, _, _, _, err := parseOffsetKey("invalid", "0")
	if err == nil {
		t.Error("missing colons should error")
	}

	_, _, _, _, err = parseOffsetKey("g:t:notanumber", "0")
	if err == nil {
		t.Error("non-numeric partition should error")
	}

	_, _, _, _, err = parseOffsetKey("g:t:0", "notanumber")
	if err == nil {
		t.Error("non-numeric offset should error")
	}
}

func TestOffsetCommitAndRecovery(t *testing.T) {
	b := newTestBroker(t)

	// Commit an offset
	err := b.commitConsumerOffset("group-1", "orders", 0, 100)
	if err != nil {
		t.Fatalf("commitConsumerOffset: %v", err)
	}

	// Verify it's in memory
	tp, _ := b.getOrCreateTopic("orders")
	cg := tp.GetOrCreateConsumerGroup("group-1")
	off := cg.GetPartitionOffset(0)
	if off != 100 {
		t.Errorf("in-memory offset: want 100, got %d", off)
	}

	// Recover — should reload from __consumer_offsets
	err = b.recoverConsumerOffsets()
	if err != nil {
		t.Fatalf("recoverConsumerOffsets: %v", err)
	}

	// Verify recovered
	tp2, _ := b.getOrCreateTopic("orders")
	cg2 := tp2.GetOrCreateConsumerGroup("group-1")
	off2 := cg2.GetPartitionOffset(0)
	if off2 != 100 {
		t.Errorf("recovered offset: want 100, got %d", off2)
	}
}

func TestOffsetCommit_MoveForward(t *testing.T) {
	b := newTestBroker(t)

	b.commitConsumerOffset("g", "t", 0, 50)
	b.commitConsumerOffset("g", "t", 0, 100)

	tp, _ := b.getOrCreateTopic("t")
	cg := tp.GetOrCreateConsumerGroup("g")
	if cg.GetPartitionOffset(0) != 100 {
		t.Errorf("want 100, got %d", cg.GetPartitionOffset(0))
	}

	// Lower offset should not move backward
	b.commitConsumerOffset("g", "t", 0, 30)
	if cg.GetPartitionOffset(0) != 100 {
		t.Errorf("should not move backward: want 100, got %d", cg.GetPartitionOffset(0))
	}
}

// helper
func uint32Bytes(v uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return b
}
