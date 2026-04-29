package proto

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// MAX_REQUEST_SIZE is the upper bound for a single Kafka request frame (100 MB).
const MAX_REQUEST_SIZE = 100 * 1024 * 1024

type KafkaCodec struct {
	rw   *bufio.ReadWriter
	conn net.Conn
}

func NewCodec(rw *bufio.ReadWriter, conn net.Conn) *KafkaCodec {
	return &KafkaCodec{rw: rw, conn: conn}
}

type RequestHeader struct {
	Size          int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

func (c *KafkaCodec) ReadRequest(ctx context.Context) (*RequestHeader, []byte, error) {
	// 1. Read 4 bytes length — use short read deadlines so we can check ctx
	lenBuf := make([]byte, 4)
	if err := c.readFullCtx(ctx, lenBuf); err != nil {
		return nil, nil, err
	}
	size := int32(binary.BigEndian.Uint32(lenBuf))

	if size <= 0 || size > MAX_REQUEST_SIZE {
		return nil, nil, fmt.Errorf("invalid request size: %d", size)
	}

	// 2. Read 'size' bytes
	data := make([]byte, size)
	if err := c.readFullCtx(ctx, data); err != nil {
		return nil, nil, err
	}

	decoder := NewDecoder(data)
	apiKey, err := decoder.ReadInt16()
	if err != nil {
		return nil, nil, err
	}

	apiVersion, err := decoder.ReadInt16()
	if err != nil {
		return nil, nil, err
	}

	correlationID, err := decoder.ReadInt32()
	if err != nil {
		return nil, nil, err
	}

	clientID, err := decoder.ReadString()
	if err != nil {
		// some older versions might not have clientID properly, ignore error for now
		clientID = ""
	}

	header := &RequestHeader{
		Size:          size,
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}

	// Return the unparsed remaining data
	return header, decoder.data[decoder.pos:], nil
}

// readFullCtx reads exactly len(buf) bytes while respecting context cancellation.
// Sets short read deadlines so the loop can check ctx.Done() between attempts.
func (c *KafkaCodec) readFullCtx(ctx context.Context, buf []byte) error {
	read := 0
	for read < len(buf) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Set a short deadline so we don't block forever
		c.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := c.rw.Read(buf[read:])
		read += n

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // deadline expired, loop back to check ctx
			}
			return err
		}
	}
	// Clear deadline for subsequent writes
	c.conn.SetReadDeadline(time.Time{})
	return nil
}

// WriteResponse wraps a payload with the total size and writes it
func (c *KafkaCodec) WriteResponse(payload []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	if _, err := c.rw.Write(lenBuf); err != nil {
		return err
	}
	if _, err := c.rw.Write(payload); err != nil {
		return err
	}
	return c.rw.Flush()
}
