package proto

import "testing"

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

func TestParseFetchRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1)      // ReplicaID
	enc.WriteInt32(100)     // MaxWait
	enc.WriteInt32(1)       // MinBytes
	enc.WriteInt32(1048576) // MaxBytes
	enc.WriteInt8(0)        // IsolationLevel
	enc.WriteInt32(0)       // SessionID
	enc.WriteInt32(0)       // SessionEpoch
	enc.WriteInt32(1)       // NumTopics
	enc.WriteString("test")
	enc.WriteInt32(1)       // NumPartitions
	enc.WriteInt32(0)       // Partition
	enc.WriteInt32(0)       // CurrentLeaderEpoch
	enc.WriteInt64(0)       // FetchOffset
	enc.WriteInt64(0)       // LogStartOffset
	enc.WriteInt32(1048576) // PartitionMaxBytes

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
