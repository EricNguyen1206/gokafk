package proto

import "testing"

func TestHandleHeartbeat_Response(t *testing.T) {
	resp := HandleHeartbeat(99)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 99 {
		t.Errorf("correlationID: want 99, got %d", corrID)
	}
}
