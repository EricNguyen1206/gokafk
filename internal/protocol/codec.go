package protocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

/*
- Codec = Encoder + Decoder
- Codec reads and writes framed messages on a buffered stream.
- Message = Header + Payload
- Codec follow wire format (13 bytes header of kafka standard)
- Header: [4 bytes length (BE)] [1 byte type] [4 bytes corrID (BE)] [4 bytes CRC32 (BE)] [N bytes payload]
*/
type Codec struct {
	rw *bufio.ReadWriter
}

// NewCodec wraps a buffered read-writer for message framing.
func NewCodec(rw *bufio.ReadWriter) *Codec {
	return &Codec{rw: rw}
}

// WriteMessage serializes and writes a framed message to the stream.
func (c *Codec) WriteMessage(ctx context.Context, msg *Message) error {
	// Handle graceful shutdown in broker
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	payloadLen := uint32(MinHeaderSize + len(msg.Payload))

	// Message length (4 bytes)
	lenBuf := make([]byte, LengthFieldSize)
	binary.BigEndian.PutUint32(lenBuf, payloadLen)

	// Message type (1 byte)
	typeBuf := []byte{msg.Type}

	// Message correlation ID (4 bytes)
	corrIDBuf := make([]byte, CorrIDFieldSize)
	binary.BigEndian.PutUint32(corrIDBuf, msg.CorrID)

	// CRC32 - Message Integrity check (4 bytes)
	crc := crc32.ChecksumIEEE(msg.Payload)
	crcBuf := make([]byte, CRC32FieldSize)
	binary.BigEndian.PutUint32(crcBuf, crc)

	// Write header to stream
	_, err := c.rw.Write(lenBuf)
	if err != nil {
		return err
	}
	_, err = c.rw.Write(typeBuf)
	if err != nil {
		return err
	}
	_, err = c.rw.Write(corrIDBuf)
	if err != nil {
		return err
	}
	_, err = c.rw.Write(crcBuf)
	if err != nil {
		return err
	}

	// Write payload to stream
	_, err = c.rw.Write(msg.Payload)
	if err != nil {
		return err
	}

	// Flush
	return c.rw.Flush()
}

// ReadMessage reads a framed message from the stream, verifying CRC32.
func (c *Codec) ReadMessage(ctx context.Context) (*Message, error) {
	// Check ctx.Done()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// Read length (4 bytes) via io.ReadFull
	lenBuf := make([]byte, LengthFieldSize)
	if _, err := io.ReadFull(c.rw, lenBuf); err != nil {
		return nil, fmt.Errorf("codec read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	// Validate length >= MinHeaderSize
	if length < MinHeaderSize {
		return nil, fmt.Errorf("codec: message too short %d < %d", length, MinHeaderSize)
	}
	// Read remaining bytes via io.ReadFull
	data := make([]byte, length)
	if _, err := io.ReadFull(c.rw, data); err != nil {
		return nil, fmt.Errorf("codec read body: %w", err)
	}

	// Parse: type, corrID, CRC32, payload
	msgType := data[0]
	corrID := binary.BigEndian.Uint32(data[1:5])
	crc := binary.BigEndian.Uint32(data[5:9])
	payload := data[9:]

	// Verify CRC32 matches — return error if mismatch
	calculatedCRC := crc32.ChecksumIEEE(payload)
	if calculatedCRC != crc {
		return nil, fmt.Errorf("codec: CRC32 mismatch (received %d, calculated %d)", crc, calculatedCRC)
	}

	// Return Message
	return &Message{
		Type:    msgType,
		CorrID:  corrID,
		Payload: payload,
	}, nil
}
