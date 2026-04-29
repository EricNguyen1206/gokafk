package proto

import (
	"encoding/binary"
	"io"
)

// Decoder provides methods to read Kafka primitives from a buffer
type Decoder struct {
	data []byte
	pos  int
}

// NewDecoder creates a new decoder for the given data
func NewDecoder(data []byte) *Decoder {
	return &Decoder{data: data}
}

// Remaining bytes in the buffer
func (d *Decoder) Remaining() int {
	return len(d.data) - d.pos
}

// read next byte as int8
func (d *Decoder) ReadInt8() (int8, error) {
	if d.pos+1 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int8(d.data[d.pos])
	d.pos++
	return val, nil
}

// read next 2 bytes as int16
func (d *Decoder) ReadInt16() (int16, error) {
	if d.pos+2 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int16(binary.BigEndian.Uint16(d.data[d.pos : d.pos+2]))
	d.pos += 2
	return val, nil
}

// read next 4 bytes as int32
func (d *Decoder) ReadInt32() (int32, error) {
	if d.pos+4 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int32(binary.BigEndian.Uint32(d.data[d.pos : d.pos+4]))
	d.pos += 4
	return val, nil
}

// read next 8 bytes as int64
func (d *Decoder) ReadInt64() (int64, error) {
	if d.pos+8 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int64(binary.BigEndian.Uint64(d.data[d.pos : d.pos+8]))
	d.pos += 8
	return val, nil
}

// ReadString reads a string prefixed with int16 length
func (d *Decoder) ReadString() (string, error) {
	strLen, err := d.ReadInt16()
	if err != nil {
		return "", err
	}
	if strLen == -1 {
		return "", nil // null string
	}
	if d.pos+int(strLen) > len(d.data) {
		return "", io.ErrUnexpectedEOF
	}
	val := string(d.data[d.pos : d.pos+int(strLen)])
	d.pos += int(strLen)
	return val, nil
}

// ReadBytes reads a byte array prefixed with int32 length
func (d *Decoder) ReadBytes() ([]byte, error) {
	ln, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}
	if ln == -1 {
		return nil, nil // null bytes
	}
	if d.pos+int(ln) > len(d.data) {
		return nil, io.ErrUnexpectedEOF
	}
	val := d.data[d.pos : d.pos+int(ln)]
	d.pos += int(ln)
	return val, nil
}

// ReadVarInt reads a zig-zag encoded LEB128 integer (Kafka uses this inside RecordBatches)
func (d *Decoder) ReadVarInt() (int64, error) {
	var value uint64
	var shift uint
	for {
		if d.pos >= len(d.data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := d.data[d.pos]
		d.pos++
		value |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	// Decode zigzag
	res := (value >> 1) ^ uint64(-(value & 1))
	return int64(res), nil
}

// Encoder provides methods to write Kafka primitives to a buffer
type Encoder struct {
	data []byte
}

func NewEncoder() *Encoder {
	return &Encoder{data: make([]byte, 0, 1024)}
}

func (e *Encoder) Bytes() []byte {
	return e.data
}

func (e *Encoder) WriteInt8(v int8) {
	e.data = append(e.data, byte(v))
}

func (e *Encoder) WriteInt16(v int16) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(v))
	e.data = append(e.data, buf...)
}

func (e *Encoder) WriteInt32(v int32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	e.data = append(e.data, buf...)
}

func (e *Encoder) WriteInt64(v int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	e.data = append(e.data, buf...)
}

// WriteString writes a string prefixed with int16 length
func (e *Encoder) WriteString(v string) {
	if len(v) > 32767 {
		panic("string too long")
	}
	e.WriteInt16(int16(len(v)))
	e.data = append(e.data, []byte(v)...)
}

// WriteBytes writes a byte array prefixed with int32 length
func (e *Encoder) WriteBytes(v []byte) {
	if v == nil {
		e.WriteInt32(-1)
		return
	}
	e.WriteInt32(int32(len(v)))
	e.data = append(e.data, v...)
}
