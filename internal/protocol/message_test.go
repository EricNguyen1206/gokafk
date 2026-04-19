package protocol

import (
	"testing"
)

func TestProducerRegisterMessage_RoundTrip(t *testing.T) {
	orig := ProducerRegisterMessage{
		Port:    8080,
		TopicID: 42,
	}

	data := orig.Marshal()
	var got ProducerRegisterMessage

	if err := got.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got != orig {
		t.Errorf("Round-trip mismatch: expect %+v got %+v", orig, got)

	}
}

func TestProducerRegisterMessage_UnmarshalTooShort(t *testing.T) {
	data := []byte{1, 2, 3} // too short
	var got ProducerRegisterMessage

	err := got.Unmarshal(data)
	if err == nil {
		t.Errorf("Unmarshal: expect error, got nil")
	}
	if err.Error() != "data too short" {
		t.Errorf("Unmarshal: expect 'data too short', got %q", err.Error())
	}
}

func TestConsumerRegisterMessage_RoundTrip(t *testing.T) {
	orig := ConsumerRegisterMessage{
		Port:    8080,
		GroupID: 42,
		TopicID: 100,
	}

	data := orig.Marshal()
	var got ConsumerRegisterMessage

	if err := got.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got != orig {
		t.Errorf("Round-trip mismatch: expect %+v got %+v", orig, got)
	}
}

func TestConsumerRegisterMessage_UnmarshalTooShort(t *testing.T) {
	data := []byte{1, 2, 3} // too short
	var got ConsumerRegisterMessage

	err := got.Unmarshal(data)
	if err == nil {
		t.Errorf("Unmarshal: expect error, got nil")
	}
	if err.Error() != "data too short" {
		t.Errorf("Unmarshal: expect 'data too short', got %q", err.Error())
	}
}
