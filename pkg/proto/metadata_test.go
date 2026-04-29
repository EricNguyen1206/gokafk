package proto

import "testing"

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
