package proto

import "testing"

func TestHandleApiVersions_Response(t *testing.T) {
	resp := HandleApiVersions(42)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 42 {
		t.Errorf("correlationID: want 42, got %d", corrID)
	}
}
