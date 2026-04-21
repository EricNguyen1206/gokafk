package protocol

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"
)

func BenchmarkCodec_WriteMessage(b *testing.B) {
	msg := &Message{
		Type:    TypeProduce,
		CorrID:  1,
		Payload: bytes.Repeat([]byte("x"), 1024),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		rw := bufio.NewReadWriter(
			bufio.NewReader(bytes.NewReader(nil)),
			bufio.NewWriter(&buf),
		)
		codec := NewCodec(rw)
		codec.WriteMessage(context.Background(), msg)
	}
}

func BenchmarkCodec_RoundTrip(b *testing.B) {
	msg := &Message{
		Type:    TypeProduce,
		CorrID:  1,
		Payload: bytes.Repeat([]byte("x"), 1024),
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		wRW := bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&buf))
		wCodec := NewCodec(wRW)
		wCodec.WriteMessage(ctx, msg)

		rRW := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(io.Discard))
		rCodec := NewCodec(rRW)
		rCodec.ReadMessage(ctx)
	}
}
