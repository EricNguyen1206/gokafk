package proto

import "testing"

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

func TestParseListOffsetsRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1) // ReplicaID
	enc.WriteInt8(0)   // IsolationLevel
	enc.WriteInt32(1)  // NumTopics
	enc.WriteString("test")
	enc.WriteInt32(1)  // NumPartitions
	enc.WriteInt32(0)  // Partition
	enc.WriteInt32(-1) // CurrentLeaderEpoch
	enc.WriteInt64(-1) // Timestamp = Latest

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
