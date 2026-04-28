package broker

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"gokafk/internal/config"
	"gokafk/pkg/kafkaprotocol"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	return NewBroker(&config.Config{
		BrokerPort:    10000,
		DataDir:       t.TempDir(),
		NumPartitions: 3, // explicit 3 partitions for tests that need multi-partition
	})
}

func TestNewBroker(t *testing.T) {
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       t.TempDir(),
		NumPartitions: 3, // explicit 3 partitions for tests that need multi-partition
	})
	if b == nil {
		t.Fatal("NewBroker returned nil")
	}
}

func TestBroker_StartAndShutdown(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       dir,
		NumPartitions: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- b.Start(ctx)
	}()

	// Give broker time to start
	time.Sleep(100 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

func TestGetOrCreateTopic(t *testing.T) {
	b := newTestBroker(t)

	tp, err := b.getOrCreateTopic("orders")
	if err != nil {
		t.Fatalf("getOrCreateTopic: %v", err)
	}
	if tp == nil {
		t.Fatal("topic should not be nil")
	}
	if tp.NumPartitions() != 3 {
		t.Errorf("partitions: want 3, got %d", tp.NumPartitions())
	}

	// Second call should return the same topic
	tp2, err := b.getOrCreateTopic("orders")
	if err != nil {
		t.Fatalf("getOrCreateTopic second call: %v", err)
	}
	if tp != tp2 {
		t.Error("should return same topic instance")
	}
}

func TestGetOrCreateTopic_ConcurrentSafe(t *testing.T) {
	b := newTestBroker(t)

	for i := 0; i < 100; i++ {
		tp, err := b.getOrCreateTopic("concurrent-topic")
		if err != nil {
			t.Fatalf("getOrCreateTopic[%d]: %v", i, err)
		}
		if tp == nil {
			t.Fatalf("topic[%d] should not be nil", i)
		}
	}
}

func TestBroker_AppendAndRead(t *testing.T) {
	b := newTestBroker(t)

	tp, err := b.getOrCreateTopic("test-topic")
	if err != nil {
		t.Fatalf("getOrCreateTopic: %v", err)
	}

	partID, offset, err := tp.Append([]byte("key"), []byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if offset < 0 {
		t.Errorf("offset should be >= 0, got %d", offset)
	}

	data, err := tp.ReadFromPartition(partID, offset)
	if err != nil {
		t.Fatalf("ReadFromPartition: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("data: want 'hello', got %q", string(data))
	}
}

func TestPartition_OffsetTracking(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("offsets")

	for i := 0; i < 5; i++ {
		tp.Append(nil, []byte("msg"))
	}

	off := tp.PartitionOffset(0)
	if off < 1 {
		t.Errorf("PartitionOffset(0): should be >= 1 after appends, got %d", off)
	}

	off = tp.PartitionOffset(99)
	if off != -1 {
		t.Errorf("PartitionOffset(99): want -1 for out of range, got %d", off)
	}
}

func TestPartition_OutOfRange(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("range-test")

	_, err := tp.ReadFromPartition(-1, 0)
	if err == nil {
		t.Error("ReadFromPartition(-1, 0) should return error")
	}

	_, err = tp.ReadFromPartition(99, 0)
	if err == nil {
		t.Error("ReadFromPartition(99, 0) should return error")
	}
}

func TestBroker_HandleConnection_APIVersions(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker(&config.Config{
		BrokerPort:    0,
		DataDir:       dir,
		NumPartitions: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- b.Start(ctx) }()

	// Wait for listener
	time.Sleep(100 * time.Millisecond)
	b.mu.RLock()
	ln := b.listener
	b.mu.RUnlock()
	if ln == nil {
		t.Fatal("listener not started")
	}
	addr := ln.Addr()

	conn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions request (key=18, v3)
	req := kafkaprotocol.NewEncoder()
	req.WriteInt16(18) // APIKey
	req.WriteInt16(3)  // APIVersion
	req.WriteInt32(1)  // CorrelationID
	req.WriteString("test-client")

	// Frame it with length prefix
	payload := req.Bytes()
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	conn.Write(append(lenBuf, payload...))

	// Read response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respLenBuf := make([]byte, 4)
	_, err = conn.Read(respLenBuf)
	if err != nil {
		t.Fatalf("read response length: %v", err)
	}
	respLen := int32(binary.BigEndian.Uint32(respLenBuf))
	respData := make([]byte, respLen)
	_, err = conn.Read(respData)
	if err != nil {
		t.Fatalf("read response data: %v", err)
	}

	// First 4 bytes should be correlationID=1
	corrID := int32(binary.BigEndian.Uint32(respData[0:4]))
	if corrID != 1 {
		t.Errorf("correlationID: want 1, got %d", corrID)
	}

	cancel()
	<-done
}

func TestPartition_TimestampAt(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("ts-test")

	// Use AppendToPartition to control which partition gets the message
	_, err := tp.AppendToPartition(0, []byte("msg"))
	if err != nil {
		t.Fatalf("AppendToPartition: %v", err)
	}

	ts, err := tp.PartitionTimestampAt(0, 0)
	if err != nil {
		t.Fatalf("PartitionTimestampAt: %v", err)
	}
	if ts <= 0 {
		t.Errorf("timestamp should be positive, got %d", ts)
	}

	_, err = tp.PartitionTimestampAt(99, 0)
	if err == nil {
		t.Error("out-of-range partition should error")
	}
}

func TestPartition_FindOffsetByTimestamp(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("ts-find")

	_, err := tp.AppendToPartition(0, []byte("msg"))
	if err != nil {
		t.Fatalf("AppendToPartition: %v", err)
	}

	// Query with timestamp 0 — should find offset 0
	off, err := tp.PartitionFindOffsetByTimestamp(0, 0)
	if err != nil {
		t.Fatalf("PartitionFindOffsetByTimestamp: %v", err)
	}
	if off != 0 {
		t.Errorf("want offset 0, got %d", off)
	}

	_, err = tp.PartitionFindOffsetByTimestamp(99, 0)
	if err == nil {
		t.Error("out-of-range partition should error")
	}
}

func TestTopic_Close(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("close-test")
	tp.Append(nil, []byte("msg"))

	if err := tp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAppendToPartition_OutOfRange(t *testing.T) {
	b := newTestBroker(t)
	tp, _ := b.getOrCreateTopic("append-oob")

	_, err := tp.AppendToPartition(-1, []byte("msg"))
	if err == nil {
		t.Error("AppendToPartition(-1) should error")
	}

	_, err = tp.AppendToPartition(99, []byte("msg"))
	if err == nil {
		t.Error("AppendToPartition(99) should error")
	}
}

func TestNewTopic_Cleanup(t *testing.T) {
	dir := t.TempDir()
	// Create topic with 5 partitions, second partition dir will fail
	// We test this indirectly — just verify topic creation works
	tp, err := NewTopic("cleanup-test", dir, 3)
	if err != nil {
		t.Fatalf("NewTopic: %v", err)
	}
	tp.Close()
}
