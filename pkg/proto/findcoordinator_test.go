package proto

import "testing"

func TestHandleFindCoordinator_Response(t *testing.T) {
	resp := HandleFindCoordinator(10)
	dec := NewDecoder(resp)
	corrID, _ := dec.ReadInt32()
	if corrID != 10 {
		t.Errorf("correlationID: want 10, got %d", corrID)
	}
}
