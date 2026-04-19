package broker

import (
	"testing"
)

func TestNewPartition(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartition(0, "test-topic", dir)
	if err != nil {
		t.Fatalf("NewPartition: %v", err)
	}
	defer p.Close()

	if p.ID() != 0 {
		t.Errorf("ID: want 0, got %d", p.ID())
	}
}

func TestPartition_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartition(0, "test-topic", dir)
	if err != nil {
		t.Fatalf("NewPartition: %v", err)
	}
	defer p.Close()

	offset, err := p.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset: want 0, got %d", offset)
	}

	data, err := p.Read(0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Read: want %q, got %q", "hello", string(data))
	}
}
