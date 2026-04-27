package broker

import (
	"context"
	"testing"

	"gokafk/pkg/kafkaprotocol"
)

func TestRouteMessage_UnsupportedKey(t *testing.T) {
	b := newTestBroker(t)

	header := &kafkaprotocol.RequestHeader{APIKey: 99, CorrelationID: 1}
	resp, err := b.routeMessage(context.Background(), header, nil, nil)
	if err != nil {
		t.Fatalf("routeMessage unsupported key: %v", err)
	}
	if resp != nil {
		t.Errorf("unsupported key should return nil response, got %v", resp)
	}
}

func TestRouteMessage_Metadata(t *testing.T) {
	b := newTestBroker(t)

	header := &kafkaprotocol.RequestHeader{APIKey: kafkaprotocol.ApiKeyMetadata, CorrelationID: 1}
	resp, err := b.routeMessage(context.Background(), header, nil, nil)
	if err != nil {
		t.Fatalf("routeMessage metadata: %v", err)
	}
	if resp == nil {
		t.Error("metadata should return non-nil response")
	}
}

func TestRouteMessage_ApiVersions(t *testing.T) {
	b := newTestBroker(t)

	header := &kafkaprotocol.RequestHeader{APIKey: kafkaprotocol.ApiKeyApiVersions, CorrelationID: 1}
	resp, err := b.routeMessage(context.Background(), header, nil, nil)
	if err != nil {
		t.Fatalf("routeMessage apiversions: %v", err)
	}
	if resp == nil {
		t.Error("apiversions should return non-nil response")
	}
}

func TestRouteMessage_FindCoordinator(t *testing.T) {
	b := newTestBroker(t)

	header := &kafkaprotocol.RequestHeader{APIKey: kafkaprotocol.ApiKeyFindCoordinator, CorrelationID: 1}
	resp, err := b.routeMessage(context.Background(), header, nil, nil)
	if err != nil {
		t.Fatalf("routeMessage findcoordinator: %v", err)
	}
	if resp == nil {
		t.Error("findcoordinator should return non-nil response")
	}
}

func TestRouteMessage_Heartbeat(t *testing.T) {
	b := newTestBroker(t)

	header := &kafkaprotocol.RequestHeader{APIKey: kafkaprotocol.ApiKeyHeartbeat, CorrelationID: 1}
	resp, err := b.routeMessage(context.Background(), header, nil, nil)
	if err != nil {
		t.Fatalf("routeMessage heartbeat: %v", err)
	}
	if resp == nil {
		t.Error("heartbeat should return non-nil response")
	}
}

func TestHandleProduce(t *testing.T) {
	b := newTestBroker(t)

	// Build a minimal ProduceRequest using the encoder
	enc := kafkaprotocol.NewEncoder()
	enc.WriteString("")      // TransactionalId
	enc.WriteInt16(1)        // Acks
	enc.WriteInt32(5000)     // Timeout
	enc.WriteInt32(1)        // NumTopics
	enc.WriteString("test")  // Topic
	enc.WriteInt32(1)        // NumPartitions
	enc.WriteInt32(0)        // Partition

	// Build a minimal RecordBatch
	batchEnc := kafkaprotocol.NewEncoder()
	// RecordBatch header (61 bytes total after the batch size field)
	batchEnc.WriteInt64(0)   // BaseOffset
	batchEnc.WriteInt32(0)   // Length (placeholder)
	batchEnc.WriteInt32(0)   // PartitionLeaderEpoch
	batchEnc.WriteInt8(2)    // Magic
	batchEnc.WriteInt32(0)   // CRC
	batchEnc.WriteInt16(0)   // Attributes
	batchEnc.WriteInt32(0)   // LastOffsetDelta
	batchEnc.WriteInt64(0)   // FirstTimestamp
	batchEnc.WriteInt64(0)   // MaxTimestamp
	batchEnc.WriteInt64(-1)  // ProducerID
	batchEnc.WriteInt16(-1)  // ProducerEpoch
	batchEnc.WriteInt32(-1)  // BaseSequence
	batchEnc.WriteInt32(1)   // Records count

	// Single record with value "hello"
	recEnc := kafkaprotocol.NewEncoder()
	recEnc.WriteInt8(0) // length placeholder (varint, 1 byte)
	recEnc.WriteInt8(0) // Attr
	recEnc.WriteInt8(0) // TS delta
	recEnc.WriteInt8(0) // Offset delta
	recEnc.WriteInt8(0) // Key len = 0
	// Value len as varint: 5 → zigzag 10
	recEnc.WriteInt8(10)
	recEnc.WriteBytes([]byte("hello"))
	recEnc.WriteInt8(0) // Headers = 0
	batchEnc.WriteBytes(recEnc.Bytes())

	batchData := batchEnc.Bytes()
	enc.WriteInt32(int32(len(batchData))) // BatchSize
	enc.WriteBytes(batchData)

	resp, err := b.handleProduce(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp == nil {
		t.Fatal("handleProduce should return response")
	}
}

func TestHandleFetch(t *testing.T) {
	b := newTestBroker(t)

	// First produce a message
	tp, _ := b.getOrCreateTopic("test-topic")
	tp.Append([]byte("key"), []byte("hello"))

	// Build a minimal FetchRequest
	enc := kafkaprotocol.NewEncoder()
	enc.WriteInt32(-1)       // ReplicaID
	enc.WriteInt32(100)      // MaxWait
	enc.WriteInt32(1)        // MinBytes
	enc.WriteInt32(1048576)  // MaxBytes
	enc.WriteInt8(0)         // IsolationLevel
	enc.WriteInt32(0)        // SessionID
	enc.WriteInt32(0)        // SessionEpoch
	enc.WriteInt32(1)        // NumTopics
	enc.WriteString("test-topic")
	enc.WriteInt32(1)        // NumPartitions
	enc.WriteInt32(0)        // Partition
	enc.WriteInt32(0)        // CurrentLeaderEpoch
	enc.WriteInt64(0)        // FetchOffset
	enc.WriteInt64(0)        // LogStartOffset
	enc.WriteInt32(1048576)  // PartitionMaxBytes

	resp, err := b.handleFetch(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if resp == nil {
		t.Fatal("handleFetch should return response")
	}
}

func TestHandleFetch_Empty(t *testing.T) {
	b := newTestBroker(t)

	enc := kafkaprotocol.NewEncoder()
	enc.WriteInt32(-1)
	enc.WriteInt32(100)
	enc.WriteInt32(1)
	enc.WriteInt32(1048576)
	enc.WriteInt8(0)
	enc.WriteInt32(0)
	enc.WriteInt32(0)
	enc.WriteInt32(1)
	enc.WriteString("nonexistent")
	enc.WriteInt32(1)
	enc.WriteInt32(0)
	enc.WriteInt32(0)
	enc.WriteInt64(0)
	enc.WriteInt64(0)
	enc.WriteInt32(1048576)

	resp, err := b.handleFetch(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleFetch empty: %v", err)
	}
	if resp == nil {
		t.Fatal("handleFetch should return response even for empty topic")
	}
}

func TestHandleListOffsets(t *testing.T) {
	b := newTestBroker(t)

	tp, _ := b.getOrCreateTopic("test-topic")
	tp.Append(nil, []byte("msg"))

	enc := kafkaprotocol.NewEncoder()
	enc.WriteInt32(-1)       // ReplicaID
	enc.WriteInt8(0)         // IsolationLevel
	enc.WriteInt32(1)        // NumTopics
	enc.WriteString("test-topic")
	enc.WriteInt32(1)        // NumPartitions
	enc.WriteInt32(0)        // Partition
	enc.WriteInt32(-1)       // CurrentLeaderEpoch
	enc.WriteInt64(-1)       // Timestamp = Latest

	resp, err := b.handleListOffsets(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleListOffsets: %v", err)
	}
	if resp == nil {
		t.Fatal("handleListOffsets should return response")
	}
}

func TestHandleOffsetCommit(t *testing.T) {
	b := newTestBroker(t)

	enc := kafkaprotocol.NewEncoder()
	enc.WriteString("my-group")  // GroupID
	enc.WriteInt32(1)            // GenerationID
	enc.WriteString("member-1")  // MemberID
	enc.WriteString("")          // GroupInstanceID
	enc.WriteInt32(1)            // NumTopics
	enc.WriteString("orders")    // Topic
	enc.WriteInt32(1)            // NumPartitions
	enc.WriteInt32(0)            // Partition
	enc.WriteInt64(42)           // Offset
	enc.WriteString("")          // Metadata
	enc.WriteInt32(-1)           // PartitionLeaderEpoch

	resp, err := b.handleOffsetCommit(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleOffsetCommit: %v", err)
	}
	if resp == nil {
		t.Fatal("handleOffsetCommit should return response")
	}
}

func TestHandleOffsetFetch(t *testing.T) {
	b := newTestBroker(t)

	// Commit an offset first
	b.commitConsumerOffset("my-group", "orders", 0, 100)

	enc := kafkaprotocol.NewEncoder()
	enc.WriteString("my-group")
	enc.WriteInt32(1)
	enc.WriteString("orders")
	enc.WriteInt32(1)
	enc.WriteInt32(0)

	resp, err := b.handleOffsetFetch(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleOffsetFetch: %v", err)
	}
	if resp == nil {
		t.Fatal("handleOffsetFetch should return response")
	}
}

func TestHandleJoinGroup(t *testing.T) {
	b := newTestBroker(t)

	enc := kafkaprotocol.NewEncoder()
	enc.WriteString("test-group")
	enc.WriteInt32(10000)
	enc.WriteInt32(10000)
	enc.WriteString("")
	enc.WriteString("")
	enc.WriteString("consumer")
	enc.WriteInt32(1)
	enc.WriteString("range")
	enc.WriteBytes(nil)

	resp, err := b.handleJoinGroup(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleJoinGroup: %v", err)
	}
	if resp == nil {
		t.Fatal("handleJoinGroup should return response")
	}
}

func TestHandleSyncGroup(t *testing.T) {
	b := newTestBroker(t)

	// Join first to register member
	joinEnc := kafkaprotocol.NewEncoder()
	joinEnc.WriteString("test-group")
	joinEnc.WriteInt32(10000)
	joinEnc.WriteInt32(10000)
	joinEnc.WriteString("")
	joinEnc.WriteString("")
	joinEnc.WriteString("consumer")
	joinEnc.WriteInt32(1)
	joinEnc.WriteString("range")
	joinEnc.WriteBytes(nil)

	joinResp, err := b.handleJoinGroup(1, joinEnc.Bytes())
	if err != nil {
		t.Fatalf("handleJoinGroup: %v", err)
	}

	// Parse memberID from join response (skip correlationID + throttle + errorCode + generation + protocol + leaderID)
	dec := kafkaprotocol.NewDecoder(joinResp[4:]) // skip correlationID
	dec.ReadInt32()                               // throttle
	dec.ReadInt16()                               // errorCode
	dec.ReadInt32()                               // generation
	dec.ReadString()                              // protocol
	dec.ReadString()                              // leaderID
	memberID, _ := dec.ReadString()

	// SyncGroup
	syncEnc := kafkaprotocol.NewEncoder()
	syncEnc.WriteString("test-group")
	syncEnc.WriteInt32(1)          // generation
	syncEnc.WriteString(memberID)
	syncEnc.WriteString("")        // groupInstanceID
	syncEnc.WriteInt32(1)          // numAssignments
	syncEnc.WriteString(memberID)
	syncEnc.WriteBytes([]byte{0x01}) // assignment

	resp, err := b.handleSyncGroup(2, syncEnc.Bytes())
	if err != nil {
		t.Fatalf("handleSyncGroup: %v", err)
	}
	if resp == nil {
		t.Fatal("handleSyncGroup should return response")
	}
}

func TestHandleLeaveGroup(t *testing.T) {
	b := newTestBroker(t)

	enc := kafkaprotocol.NewEncoder()
	enc.WriteString("test-group")
	enc.WriteString("member-1")

	resp, err := b.handleLeaveGroup(1, enc.Bytes())
	if err != nil {
		t.Fatalf("handleLeaveGroup: %v", err)
	}
	if resp == nil {
		t.Fatal("handleLeaveGroup should return response")
	}
}
