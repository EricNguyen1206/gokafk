package kafkaprotocol

import "testing"

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
