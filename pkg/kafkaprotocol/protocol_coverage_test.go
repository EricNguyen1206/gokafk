package kafkaprotocol

import (
	"bufio"
	"bytes"
	"testing"
)

// --- Encoder/Decoder round-trip tests ---

func TestEncoderDecoder_Int8(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt8(-42)

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("ReadInt8: %v", err)
	}
	if v != -42 {
		t.Errorf("want -42, got %d", v)
	}
}

func TestEncoderDecoder_Int64(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt64(123456789)

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64: %v", err)
	}
	if v != 123456789 {
		t.Errorf("want 123456789, got %d", v)
	}
}

func TestEncoderDecoder_VarInt(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt8(10) // zigzag encoded 5

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadVarInt()
	if err != nil {
		t.Fatalf("ReadVarInt: %v", err)
	}
	if v != 5 {
		t.Errorf("want 5, got %d", v)
	}
}

func TestEncoderDecoder_String(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("hello")

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	if v != "hello" {
		t.Errorf("want 'hello', got %q", v)
	}
}

func TestEncoderDecoder_Bytes(t *testing.T) {
	enc := NewEncoder()
	enc.WriteBytes([]byte{0xCA, 0xFE})

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	if !bytes.Equal(v, []byte{0xCA, 0xFE}) {
		t.Errorf("want [CA FE], got %x", v)
	}
}

func TestDecoder_Remaining(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(42)

	dec := NewDecoder(enc.Bytes())
	if dec.Remaining() != 4 {
		t.Errorf("remaining before read: want 4, got %d", dec.Remaining())
	}
	dec.ReadInt32()
	if dec.Remaining() != 0 {
		t.Errorf("remaining after read: want 0, got %d", dec.Remaining())
	}
}

func TestDecoder_EOF(t *testing.T) {
	dec := NewDecoder([]byte{})
	_, err := dec.ReadInt16()
	if err == nil {
		t.Error("should error on empty buffer")
	}

	dec = NewDecoder([]byte{0x00})
	_, err = dec.ReadInt16()
	if err == nil {
		t.Error("should error on insufficient data")
	}
}

// --- Handle functions ---

func TestHandleApiVersions_Response(t *testing.T) {
	resp := HandleApiVersions(42)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 42 {
		t.Errorf("correlationID: want 42, got %d", corrID)
	}
}

func TestHandleMetadata_Response(t *testing.T) {
	reqData := func() []byte {
		enc := NewEncoder()
		enc.WriteInt32(1)
		enc.WriteString("test-topic")
		return enc.Bytes()
	}()

	resp := HandleMetadata(7, reqData)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 7 {
		t.Errorf("correlationID: want 7, got %d", corrID)
	}
}

func TestHandleFindCoordinator_Response(t *testing.T) {
	resp := HandleFindCoordinator(10)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 10 {
		t.Errorf("correlationID: want 10, got %d", corrID)
	}
}

func TestHandleHeartbeat_Response(t *testing.T) {
	resp := HandleHeartbeat(99)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 99 {
		t.Errorf("correlationID: want 99, got %d", corrID)
	}
}

func TestHandleProduceResponse_CorrelationID(t *testing.T) {
	resp := HandleProduceResponse(55, "test", 0, 100)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 55 {
		t.Errorf("correlationID: want 55, got %d", corrID)
	}
}

func TestHandleFetchResponse_Empty(t *testing.T) {
	resp := HandleFetchResponse(33, "test", 0, nil, 0)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 33 {
		t.Errorf("correlationID: want 33, got %d", corrID)
	}
}

func TestHandleFetchResponse_WithData(t *testing.T) {
	resp := HandleFetchResponse(1, "test", 0, [][]byte{[]byte("hello")}, 0)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 1 {
		t.Errorf("correlationID: want 1, got %d", corrID)
	}
}

func TestHandleOffsetCommitResponse_CorrelationID(t *testing.T) {
	resp := HandleOffsetCommitResponse(77)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 77 {
		t.Errorf("correlationID: want 77, got %d", corrID)
	}
}

func TestHandleOffsetFetchResponse(t *testing.T) {
	entries := []OffsetFetchResponseEntry{
		{
			Topic: "orders",
			Partitions: []OffsetFetchPartitionEntry{
				{Partition: 0, Offset: 100},
				{Partition: 1, Offset: -1},
			},
		},
	}
	resp := HandleOffsetFetchResponse(88, entries)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 88 {
		t.Errorf("correlationID: want 88, got %d", corrID)
	}
}

func TestHandleListOffsetsResponse(t *testing.T) {
	entries := []ListOffsetsResponseEntry{
		{Topic: "orders", Partition: 0, Offset: 50, Timestamp: -1},
		{Topic: "orders", Partition: 1, Offset: 0, Timestamp: -2},
	}
	resp := HandleListOffsetsResponse(66, entries)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 66 {
		t.Errorf("correlationID: want 66, got %d", corrID)
	}
}

func TestParseLeaveGroupRequest_Roundtrip(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("group-1")
	enc.WriteString("member-1")

	req, err := ParseLeaveGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseLeaveGroupRequest: %v", err)
	}
	if req.GroupID != "group-1" {
		t.Errorf("GroupID: want 'group-1', got %q", req.GroupID)
	}
	if req.MemberID != "member-1" {
		t.Errorf("MemberID: want 'member-1', got %q", req.MemberID)
	}
}

func TestHandleLeaveGroupResponse_CorrelationID(t *testing.T) {
	resp := HandleLeaveGroupResponse(44, 0)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 44 {
		t.Errorf("correlationID: want 44, got %d", corrID)
	}
}

func TestParseFetchRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1)       // ReplicaID
	enc.WriteInt32(100)      // MaxWait
	enc.WriteInt32(1)        // MinBytes
	enc.WriteInt32(1048576)  // MaxBytes
	enc.WriteInt8(0)         // IsolationLevel
	enc.WriteInt32(0)        // SessionID
	enc.WriteInt32(0)        // SessionEpoch
	enc.WriteInt32(1)        // NumTopics
	enc.WriteString("test")
	enc.WriteInt32(1)        // NumPartitions
	enc.WriteInt32(0)        // Partition
	enc.WriteInt32(0)        // CurrentLeaderEpoch
	enc.WriteInt64(0)        // FetchOffset
	enc.WriteInt64(0)        // LogStartOffset
	enc.WriteInt32(1048576)  // PartitionMaxBytes

	reqs, err := ParseFetchRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseFetchRequest: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Topic != "test" {
		t.Errorf("Topic: want 'test', got %q", reqs[0].Topic)
	}
}

func TestParseOffsetCommitRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("my-group")
	enc.WriteInt32(1)            // GenerationID
	enc.WriteString("member-1") // MemberID
	enc.WriteString("")          // GroupInstanceID
	enc.WriteInt32(1)            // NumTopics
	enc.WriteString("orders")    // Topic
	enc.WriteInt32(1)            // NumPartitions
	enc.WriteInt32(0)            // Partition
	enc.WriteInt64(42)           // Offset
	enc.WriteString("")          // Metadata
	enc.WriteInt32(-1)           // PartitionLeaderEpoch

	groupID, reqs, err := ParseOffsetCommitRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseOffsetCommitRequest: %v", err)
	}
	if groupID != "my-group" {
		t.Errorf("groupID: want 'my-group', got %q", groupID)
	}
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Offset != 42 {
		t.Errorf("Offset: want 42, got %d", reqs[0].Offset)
	}
}

func TestParseOffsetFetchRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("my-group")
	enc.WriteInt32(1)
	enc.WriteString("orders")
	enc.WriteInt32(1)
	enc.WriteInt32(0)

	groupID, reqs, err := ParseOffsetFetchRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseOffsetFetchRequest: %v", err)
	}
	if groupID != "my-group" {
		t.Errorf("groupID: want 'my-group', got %q", groupID)
	}
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Topic != "orders" {
		t.Errorf("Topic: want 'orders', got %q", reqs[0].Topic)
	}
}

func TestParseListOffsetsRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1)     // ReplicaID
	enc.WriteInt8(0)       // IsolationLevel
	enc.WriteInt32(1)      // NumTopics
	enc.WriteString("test")
	enc.WriteInt32(1)      // NumPartitions
	enc.WriteInt32(0)      // Partition
	enc.WriteInt32(-1)     // CurrentLeaderEpoch
	enc.WriteInt64(-1)     // Timestamp = Latest

	reqs, err := ParseListOffsetsRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseListOffsetsRequest: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Timestamp != -1 {
		t.Errorf("Timestamp: want -1, got %d", reqs[0].Timestamp)
	}
}

func TestCodec_NewCodec(t *testing.T) {
	rw := bufio.NewReadWriter(bufio.NewReader(nil), bufio.NewWriter(nil))
	c := NewCodec(rw, nil)
	if c == nil {
		t.Error("NewCodec returned nil")
	}
}

func TestCodec_WriteResponse(t *testing.T) {
	var buf bytes.Buffer
	rw := bufio.NewReadWriter(bufio.NewReader(nil), bufio.NewWriter(&buf))
	c := NewCodec(rw, nil)

	payload := []byte{0x01, 0x02, 0x03}
	if err := c.WriteResponse(payload); err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}

	// Should write [length(4)][payload]
	written := buf.Bytes()
	if len(written) != 7 {
		t.Fatalf("want 7 bytes (4+3), got %d", len(written))
	}
	length := int(written[0])<<24 | int(written[1])<<16 | int(written[2])<<8 | int(written[3])
	if length != 3 {
		t.Errorf("payload length: want 3, got %d", length)
	}
}

func TestParseProduceRequest(t *testing.T) {
	// Build a minimal valid ProduceRequest with a RecordBatch
	enc := NewEncoder()
	enc.WriteString("")      // TransactionalId
	enc.WriteInt16(1)        // Acks
	enc.WriteInt32(5000)     // Timeout
	enc.WriteInt32(1)        // NumTopics
	enc.WriteString("test")  // Topic
	enc.WriteInt32(1)        // NumPartitions
	enc.WriteInt32(0)        // Partition

	// Build a minimal RecordBatch (61-byte header + records)
	batchEnc := NewEncoder()
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

	// Single record using proper varint encoding
	encVarInt := func(n int64) []byte {
		zz := uint64((n << 1) ^ (n >> 63))
		var buf []byte
		for {
			b := byte(zz & 0x7f)
			zz >>= 7
			if zz > 0 {
				buf = append(buf, b|0x80)
			} else {
				buf = append(buf, b)
				break
			}
		}
		return buf
	}

	recData := make([]byte, 0)
	recData = append(recData, encVarInt(0)...)   // length
	recData = append(recData, encVarInt(0)...)   // Attr
	recData = append(recData, encVarInt(0)...)   // TS delta
	recData = append(recData, encVarInt(0)...)   // Offset delta
	recData = append(recData, encVarInt(0)...)   // Key len = 0
	recData = append(recData, encVarInt(2)...)   // Value len zigzag: 2→4
	recData = append(recData, []byte("hi")...)
	recData = append(recData, encVarInt(0)...)   // Headers = 0

	// Append recData directly (no length prefix — it's inside a RecordBatch)
	batchEnc.data = append(batchEnc.data, recData...)

	batchData := batchEnc.Bytes()
	enc.WriteInt32(int32(len(batchData)))
	enc.WriteBytes(batchData)

	records, err := ParseProduceRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if records[0].Topic != "test" {
		t.Errorf("Topic: want 'test', got %q", records[0].Topic)
	}
	if string(records[0].Value) != "hi" {
		t.Errorf("Value: want 'hi', got %q", string(records[0].Value))
	}
}

func TestParseProduceRequest_Empty(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("")
	enc.WriteInt16(1)
	enc.WriteInt32(5000)
	enc.WriteInt32(0) // 0 topics

	records, err := ParseProduceRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("empty produce request: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("want 0 records, got %d", len(records))
	}
}

func TestParseSyncGroupRequest_Roundtrip(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("group-1")
	enc.WriteInt32(1)            // GenerationID
	enc.WriteString("member-1") // MemberID
	enc.WriteString("")          // GroupInstanceID
	enc.WriteInt32(1)            // NumAssignments
	enc.WriteString("member-1")
	enc.WriteBytes([]byte{0x01})

	req, err := ParseSyncGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseSyncGroupRequest: %v", err)
	}
	if req.GroupID != "group-1" {
		t.Errorf("GroupID: want 'group-1', got %q", req.GroupID)
	}
	if len(req.Assignments) != 1 {
		t.Fatalf("want 1 assignment, got %d", len(req.Assignments))
	}
	if req.Assignments[0].MemberID != "member-1" {
		t.Errorf("Assignment MemberID: want 'member-1', got %q", req.Assignments[0].MemberID)
	}
}

func TestHandleSyncGroupResponse_CorrelationID(t *testing.T) {
	resp := HandleSyncGroupResponse(22, 0, []byte{0xAB})
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 22 {
		t.Errorf("correlationID: want 22, got %d", corrID)
	}
}

func TestHandleJoinGroupResponse_MultiMember(t *testing.T) {
	members := []JoinGroupMember{
		{MemberID: "m1", Metadata: []byte{0x01}},
		{MemberID: "m2", Metadata: []byte{0x02}},
	}
	resp := HandleJoinGroupResponse(5, 0, 2, "range", "m1", "m1", members)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 5 {
		t.Errorf("correlationID: want 5, got %d", corrID)
	}
}

func TestDecoder_ReadString_Null(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt16(-1) // null string

	dec := NewDecoder(enc.Bytes())
	s, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString null: %v", err)
	}
	if s != "" {
		t.Errorf("null string should be empty, got %q", s)
	}
}

func TestDecoder_ReadBytes_Null(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1) // null bytes

	dec := NewDecoder(enc.Bytes())
	b, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes null: %v", err)
	}
	if b != nil {
		t.Errorf("null bytes should be nil, got %v", b)
	}
}
