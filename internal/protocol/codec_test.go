package protocol

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
)

// Helper
// helper to create a write-codec that writes to a buffer
func newTestWriteCodec(buf *bytes.Buffer) (*Codec, *bytes.Buffer) {
	return NewCodec(bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(buf))), buf
}

// helper to create a read-codec that reads from a buffer
func newTestReadCodec(buf *bytes.Buffer) *Codec {
	rw := bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(io.Discard))
	return NewCodec(rw)
}

func TestCodecRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()

	msg := &Message{
		Type:    TypeEcho,
		CorrID:  123,
		Payload: []byte("hello"),
	}

	// Write
	wc, _ := newTestWriteCodec(&buf)
	if err := wc.WriteMessage(ctx, msg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// Read
	rc := newTestReadCodec(&buf)

	got, err := rc.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if !reflect.DeepEqual(msg, got) {
		t.Errorf("Round-trip mismatch: expect %+v got %+v", msg, got)
	}
}

func TestCodecEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()

	msg := &Message{
		Type:    TypePRegResp,
		CorrID:  123,
		Payload: []byte{},
	}

	// Write
	wc, _ := newTestWriteCodec(&buf)
	if err := wc.WriteMessage(ctx, msg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// Read
	rc := newTestReadCodec(&buf)

	got, err := rc.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if !reflect.DeepEqual(msg, got) {
		t.Errorf("Round-trip mismatch: expect %+v got %+v", msg, got)
	}
}

func TestCodecCRC32Corruption(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()

	msg := &Message{
		Type:    TypeEcho,
		CorrID:  123,
		Payload: []byte("hello"),
	}

	// Write
	wc, _ := newTestWriteCodec(&buf)
	if err := wc.WriteMessage(ctx, msg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// Corrupt CRC32
	corrupted := buf.Bytes()
	corrupted[9] ^= 0xFF // flip one byte of CRC32

	// Read
	rc := newTestReadCodec(bytes.NewBuffer(corrupted))

	_, err := rc.ReadMessage(ctx)
	if err == nil {
		t.Fatalf("Expected CRC32 error, got nil")
	}
	if !strings.Contains(err.Error(), "CRC32 mismatch") {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestCodecMultipleMessages(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()

	msgs := []*Message{
		{Type: TypeEcho, CorrID: 1, Payload: []byte("one")},
		{Type: TypeProduce, CorrID: 2, Payload: []byte("two")},
		{Type: TypeFetch, CorrID: 3, Payload: []byte("three")},
	}

	// Write all messages
	wc, _ := newTestWriteCodec(&buf)
	for _, msg := range msgs {
		if err := wc.WriteMessage(ctx, msg); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
	}

	// Read all messages
	rc := newTestReadCodec(&buf)
	for i, want := range msgs {
		got, err := rc.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("ReadMessage #%d: %v", i, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("Message #%d mismatch: expect %+v got %+v", i, want, got)
		}
	}

	// Should be EOF after last message
	_, err := rc.ReadMessage(ctx)
	if err != io.EOF {
		t.Errorf("Expected EOF after last message, got %v", err)
	}
}
