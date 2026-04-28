package kafkaprotocol

import (
	"bytes"
	"testing"
)

func TestEncoderDecoder_Int8(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt8(-42)

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("ReadInt8: %v", err)
	}
	if v != -42 {
		t.Errorf("want -42, got %d", v)
	}
}

func TestEncoderDecoder_Int64(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt64(123456789)

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64: %v", err)
	}
	if v != 123456789 {
		t.Errorf("want 123456789, got %d", v)
	}
}

func TestEncoderDecoder_VarInt(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt8(10) // zigzag encoded 5

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadVarInt()
	if err != nil {
		t.Fatalf("ReadVarInt: %v", err)
	}
	if v != 5 {
		t.Errorf("want 5, got %d", v)
	}
}

func TestEncoderDecoder_String(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("hello")

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	if v != "hello" {
		t.Errorf("want 'hello', got %q", v)
	}
}

func TestEncoderDecoder_Bytes(t *testing.T) {
	enc := NewEncoder()
	enc.WriteBytes([]byte{0xCA, 0xFE})

	dec := NewDecoder(enc.Bytes())
	v, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	if !bytes.Equal(v, []byte{0xCA, 0xFE}) {
		t.Errorf("want [CA FE], got %x", v)
	}
}

func TestDecoder_Remaining(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(42)

	dec := NewDecoder(enc.Bytes())
	if dec.Remaining() != 4 {
		t.Errorf("remaining before read: want 4, got %d", dec.Remaining())
	}
	dec.ReadInt32()
	if dec.Remaining() != 0 {
		t.Errorf("remaining after read: want 0, got %d", dec.Remaining())
	}
}

func TestDecoder_EOF(t *testing.T) {
	dec := NewDecoder([]byte{})
	_, err := dec.ReadInt16()
	if err == nil {
		t.Error("should error on empty buffer")
	}

	dec = NewDecoder([]byte{0x00})
	_, err = dec.ReadInt16()
	if err == nil {
		t.Error("should error on insufficient data")
	}
}

func TestDecoder_ReadString_Null(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt16(-1) // null string

	dec := NewDecoder(enc.Bytes())
	s, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString null: %v", err)
	}
	if s != "" {
		t.Errorf("null string should be empty, got %q", s)
	}
}

func TestDecoder_ReadBytes_Null(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(-1) // null bytes

	dec := NewDecoder(enc.Bytes())
	b, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes null: %v", err)
	}
	if b != nil {
		t.Errorf("null bytes should be nil, got %v", b)
	}
}

func TestWriteBytes_Nil(t *testing.T) {
	enc := NewEncoder()
	enc.WriteBytes(nil)

	dec := NewDecoder(enc.Bytes())
	b, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes after WriteBytes(nil): %v", err)
	}
	if b != nil {
		t.Errorf("nil bytes should round-trip as nil, got %v", b)
	}
}

func TestWriteString_TooLong(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic for string > 32767 bytes")
		}
	}()
	enc := NewEncoder()
	enc.WriteString(string(make([]byte, 32768)))
}

func TestDecoder_ReadString_Truncated(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt16(10) // claims 10-byte string
	enc.WriteInt8(0)   // only 1 byte available

	dec := NewDecoder(enc.Bytes())
	_, err := dec.ReadString()
	if err == nil {
		t.Error("should error on truncated string data")
	}
}

func TestDecoder_ReadBytes_Truncated(t *testing.T) {
	enc := NewEncoder()
	enc.WriteInt32(10) // claims 10-byte array
	enc.WriteInt8(0)   // only 1 byte available

	dec := NewDecoder(enc.Bytes())
	_, err := dec.ReadBytes()
	if err == nil {
		t.Error("should error on truncated bytes data")
	}
}
