package kafkaprotocol

import (
	"bufio"
	"bytes"
	"testing"
)

func TestCodec_NewCodec(t *testing.T) {
	rw := bufio.NewReadWriter(bufio.NewReader(nil), bufio.NewWriter(nil))
	c := NewCodec(rw, nil)
	if c == nil {
		t.Error("NewCodec returned nil")
	}
}

func TestCodec_WriteResponse(t *testing.T) {
	var buf bytes.Buffer
	rw := bufio.NewReadWriter(bufio.NewReader(nil), bufio.NewWriter(&buf))
	c := NewCodec(rw, nil)

	payload := []byte{0x01, 0x02, 0x03}
	if err := c.WriteResponse(payload); err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}

	written := buf.Bytes()
	if len(written) != 7 {
		t.Fatalf("want 7 bytes (4+3), got %d", len(written))
	}
	length := int(written[0])<<24 | int(written[1])<<16 | int(written[2])<<8 | int(written[3])
	if length != 3 {
		t.Errorf("payload length: want 3, got %d", length)
	}
}
