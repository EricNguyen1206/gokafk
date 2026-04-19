package storage

import (
	"fmt"
	"testing"
)

func BenchmarkSegment_Append(b *testing.B) {
	dir := b.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		b.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	data := []byte("benchmark message payload 1234567890")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := seg.Append(data); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

func BenchmarkSegment_Read(b *testing.B) {
	dir := b.TempDir()
	seg, err := NewSegment(dir, 1)
	if err != nil {
		b.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	// Pre-populate
	n := 10000
	for i := 0; i < n; i++ {
		seg.Append([]byte(fmt.Sprintf("message-%d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i % n)
		if _, err := seg.Read(offset); err != nil {
			b.Fatalf("Read: %v", err)
		}
	}
}
