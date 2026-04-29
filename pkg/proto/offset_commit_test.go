package proto

import "testing"

func TestHandleOffsetCommitResponse_CorrelationID(t *testing.T) {
	resp := HandleOffsetCommitResponse(77)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 77 {
		t.Errorf("correlationID: want 77, got %d", corrID)
	}
}

func TestParseOffsetCommitRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("my-group")
	enc.WriteInt32(1)           // GenerationID
	enc.WriteString("member-1") // MemberID
	enc.WriteString("")         // GroupInstanceID
	enc.WriteInt32(1)           // NumTopics
	enc.WriteString("orders")   // Topic
	enc.WriteInt32(1)           // NumPartitions
	enc.WriteInt32(0)           // Partition
	enc.WriteInt64(42)          // Offset
	enc.WriteString("")         // Metadata
	enc.WriteInt32(-1)          // PartitionLeaderEpoch

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
