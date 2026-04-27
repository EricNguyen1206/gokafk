package broker

import (
	"net"
	"testing"
	"time"
)

// coverageMockConn implements net.Conn for tests in this file
type coverageMockConn struct{ addr string }

func (m *coverageMockConn) Read(b []byte) (n int, err error)  { return 0, nil }
func (m *coverageMockConn) Write(b []byte) (n int, err error) { return 0, nil }
func (m *coverageMockConn) Close() error                      { return nil }
func (m *coverageMockConn) LocalAddr() net.Addr {
	return coverageMockAddr(m.addr)
}
func (m *coverageMockConn) RemoteAddr() net.Addr {
	return coverageMockAddr(m.addr)
}
func (m *coverageMockConn) SetDeadline(t time.Time) error      { return nil }
func (m *coverageMockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *coverageMockConn) SetWriteDeadline(t time.Time) error { return nil }

type coverageMockAddr string

func (m coverageMockAddr) Network() string { return "tcp" }
func (m coverageMockAddr) String() string  { return string(m) }

func newCoverageMockConn(id string) *coverageMockConn {
	return &coverageMockConn{addr: id}
}

func TestConsumerGroup_MemberCount(t *testing.T) {
	cg := NewConsumerGroup("g")

	if cg.MemberCount() != 0 {
		t.Errorf("initial count: want 0, got %d", cg.MemberCount())
	}

	cg.AddMember(newCoverageMockConn("c-1"))
	if cg.MemberCount() != 1 {
		t.Errorf("after add: want 1, got %d", cg.MemberCount())
	}
}

func TestConsumerGroup_GetPartitionOffset(t *testing.T) {
	cg := NewConsumerGroup("g")

	off := cg.GetPartitionOffset(0)
	if off != 0 {
		t.Errorf("unset offset: want 0, got %d", off)
	}

	cg.CommitPartitionOffset(0, 42)
	if cg.GetPartitionOffset(0) != 42 {
		t.Errorf("committed offset: want 42, got %d", cg.GetPartitionOffset(0))
	}
}

func TestConsumerGroup_CommitPartitionOffset_OnlyForward(t *testing.T) {
	cg := NewConsumerGroup("g")

	cg.CommitPartitionOffset(0, 100)
	cg.CommitPartitionOffset(0, 50)

	if cg.GetPartitionOffset(0) != 100 {
		t.Errorf("should not move backward: want 100, got %d", cg.GetPartitionOffset(0))
	}
}

func TestConsumerGroup_SetRecoveredOffset(t *testing.T) {
	cg := NewConsumerGroup("g")

	cg.CommitPartitionOffset(0, 100)
	cg.SetRecoveredOffset(0, 50)

	if cg.GetPartitionOffset(0) != 50 {
		t.Errorf("SetRecoveredOffset should overwrite: want 50, got %d", cg.GetPartitionOffset(0))
	}
}

func TestConsumerGroup_RemoveMember(t *testing.T) {
	cg := NewConsumerGroup("g")
	id := cg.AddMember(newCoverageMockConn("c-1"))
	cg.AddMember(newCoverageMockConn("c-2"))

	cg.RemoveMember(id)
	if cg.MemberCount() != 1 {
		t.Errorf("after remove: want 1, got %d", cg.MemberCount())
	}
}
