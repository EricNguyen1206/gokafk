package kafkaprotocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
)

type KafkaCodec struct {
	rw *bufio.ReadWriter
}

func NewCodec(rw *bufio.ReadWriter) *KafkaCodec {
	return &KafkaCodec{rw: rw}
}

type RequestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

func (c *KafkaCodec) ReadRequest(ctx context.Context) (*RequestHeader, []byte, error) {
	// 1. Read 4 bytes length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.rw, lenBuf); err != nil {
		return nil, nil, err
	}
	size := int32(binary.BigEndian.Uint32(lenBuf))

	if size <= 0 || size > 100*1024*1024 { // Sanity check 100MB
		return nil, nil, fmt.Errorf("invalid request size: %d", size)
	}

	// 2. Read 'size' bytes
	data := make([]byte, size)
	if _, err := io.ReadFull(c.rw, data); err != nil {
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

	correlationId, err := decoder.ReadInt32()
	if err != nil {
		return nil, nil, err
	}

	clientId, err := decoder.ReadString()
	if err != nil {
		// some older versions might not have clientId properly, ignore error for now
		clientId = ""
	}

	header := &RequestHeader{
		Size:          size,
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationId: correlationId,
		ClientId:      clientId,
	}

	// Return the unparsed remaining data
	return header, decoder.data[decoder.pos:], nil
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
