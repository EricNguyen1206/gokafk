package proto

import "testing"

func TestHandleProduceResponse_CorrelationID(t *testing.T) {
	resp := HandleProduceResponse(55, "test", 0, 100)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 55 {
		t.Errorf("correlationID: want 55, got %d", corrID)
	}
}

func TestParseProduceRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("")     // TransactionalId
	enc.WriteInt16(1)       // Acks
	enc.WriteInt32(5000)    // Timeout
	enc.WriteInt32(1)       // NumTopics
	enc.WriteString("test") // Topic
	enc.WriteInt32(1)       // NumPartitions
	enc.WriteInt32(0)       // Partition

	// Build a minimal RecordBatch (61-byte header + records)
	batchEnc := NewEncoder()
	batchEnc.WriteInt64(0)  // BaseOffset
	batchEnc.WriteInt32(0)  // Length (placeholder)
	batchEnc.WriteInt32(0)  // PartitionLeaderEpoch
	batchEnc.WriteInt8(2)   // Magic
	batchEnc.WriteInt32(0)  // CRC
	batchEnc.WriteInt16(0)  // Attributes
	batchEnc.WriteInt32(0)  // LastOffsetDelta
	batchEnc.WriteInt64(0)  // FirstTimestamp
	batchEnc.WriteInt64(0)  // MaxTimestamp
	batchEnc.WriteInt64(-1) // ProducerID
	batchEnc.WriteInt16(-1) // ProducerEpoch
	batchEnc.WriteInt32(-1) // BaseSequence
	batchEnc.WriteInt32(1)  // Records count

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
	recData = append(recData, encVarInt(0)...) // length
	recData = append(recData, encVarInt(0)...) // Attr
	recData = append(recData, encVarInt(0)...) // TS delta
	recData = append(recData, encVarInt(0)...) // Offset delta
	recData = append(recData, encVarInt(0)...) // Key len = 0
	recData = append(recData, encVarInt(2)...) // Value len zigzag: 2→4
	recData = append(recData, []byte("hi")...)
	recData = append(recData, encVarInt(0)...) // Headers = 0

	batchEnc.data = append(batchEnc.data, recData...)

	batchData := batchEnc.Bytes()
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

func TestParseProduceRequest_BatchTooSmall(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("")     // TransactionalId
	enc.WriteInt16(1)       // Acks
	enc.WriteInt32(5000)    // Timeout
	enc.WriteInt32(1)       // NumTopics
	enc.WriteString("test") // Topic
	enc.WriteInt32(1)       // NumPartitions
	enc.WriteInt32(0)       // Partition
	enc.WriteInt32(5)       // BatchSize > 0 but too small
	enc.data = append(enc.data, 1, 2, 3, 4, 5)

	_, err := ParseProduceRequest(enc.Bytes())
	if err == nil {
		t.Error("expected error for batch too small")
	}
}

func TestParseProduceRequest_WithKeyAndHeaders(t *testing.T) {
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

	enc := NewEncoder()
	enc.WriteString("")     // TransactionalId
	enc.WriteInt16(1)       // Acks
	enc.WriteInt32(5000)    // Timeout
	enc.WriteInt32(1)       // NumTopics
	enc.WriteString("test") // Topic
	enc.WriteInt32(1)       // NumPartitions
	enc.WriteInt32(0)       // Partition

	batchEnc := NewEncoder()
	batchEnc.WriteInt64(0)  // BaseOffset
	batchEnc.WriteInt32(0)  // Length
	batchEnc.WriteInt32(0)  // PartitionLeaderEpoch
	batchEnc.WriteInt8(2)   // Magic
	batchEnc.WriteInt32(0)  // CRC
	batchEnc.WriteInt16(0)  // Attributes
	batchEnc.WriteInt32(0)  // LastOffsetDelta
	batchEnc.WriteInt64(0)  // FirstTimestamp
	batchEnc.WriteInt64(0)  // MaxTimestamp
	batchEnc.WriteInt64(-1) // ProducerID
	batchEnc.WriteInt16(-1) // ProducerEpoch
	batchEnc.WriteInt32(-1) // BaseSequence
	batchEnc.WriteInt32(1)  // Records count

	recData := make([]byte, 0)
	recData = append(recData, encVarInt(0)...)  // length
	recData = append(recData, encVarInt(0)...)  // Attr
	recData = append(recData, encVarInt(0)...)  // TS delta
	recData = append(recData, encVarInt(0)...)  // Offset delta
	recData = append(recData, encVarInt(2)...)  // Key len = 2 (zigzag encoded)
	recData = append(recData, []byte("ke")...)  // Key data
	recData = append(recData, encVarInt(2)...)  // Value len = 2 (zigzag encoded)
	recData = append(recData, []byte("ab")...)  // Value data
	recData = append(recData, encVarInt(1)...)  // 1 header
	recData = append(recData, encVarInt(3)...)  // header key len = 3
	recData = append(recData, []byte("foo")...) // header key
	recData = append(recData, encVarInt(3)...)  // header value len = 3
	recData = append(recData, []byte("bar")...) // header value

	batchEnc.data = append(batchEnc.data, recData...)
	enc.WriteBytes(batchEnc.Bytes())

	records, err := ParseProduceRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if string(records[0].Key) != "ke" {
		t.Errorf("Key: want 'ke', got %q", string(records[0].Key))
	}
	if string(records[0].Value) != "ab" {
		t.Errorf("Value: want 'ab', got %q", string(records[0].Value))
	}
}

func TestParseProduceRequest_ZeroBatchSize(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("")     // TransactionalId
	enc.WriteInt16(1)       // Acks
	enc.WriteInt32(5000)    // Timeout
	enc.WriteInt32(1)       // NumTopics
	enc.WriteString("test") // Topic
	enc.WriteInt32(1)       // NumPartitions
	enc.WriteInt32(0)       // Partition
	enc.WriteInt32(0)       // BatchSize = 0 (should skip)

	records, err := ParseProduceRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseProduceRequest zero batch: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("want 0 records with zero batch size, got %d", len(records))
	}
}
