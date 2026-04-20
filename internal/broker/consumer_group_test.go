package broker

import (
	"fmt"
	"net"
	"testing"
)

// mockConn implements net.Conn for testing
type mockConn struct {
	net.Conn
	addr string
}

type mockAddr struct{ id string }

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return m.id }

func (m *mockConn) RemoteAddr() net.Addr {
	return &mockAddr{id: m.addr}
}

func newMockConn(id string) *mockConn {
	return &mockConn{addr: id}
}

func TestConsumerGroup_Rebalance_SingleConsumer(t *testing.T) {
	cg := NewConsumerGroup("group-1")
	conn := newMockConn("consumer-1")
	memberID := cg.AddMember(conn)

	cg.Rebalance(3) // 3 partitions, 1 consumer

	assignments := cg.GetAssignments(memberID)
	if len(assignments) != 3 {
		t.Errorf("single consumer should get all 3 partitions, got %d", len(assignments))
	}
}

func TestConsumerGroup_Rebalance_EvenSplit(t *testing.T) {
	cg := NewConsumerGroup("group-1")

	members := make([]string, 3)
	for i := 0; i < 3; i++ {
		members[i] = cg.AddMember(newMockConn(fmt.Sprintf("c-%d", i)))
	}

	cg.Rebalance(6) // 6 partitions, 3 consumers → 2 each

	for _, memberID := range members {
		assignments := cg.GetAssignments(memberID)
		if len(assignments) != 2 {
			t.Errorf("member %s: want 2 partitions, got %d: %v", memberID, len(assignments), assignments)
		}
	}
}

func TestConsumerGroup_Rebalance_Remainder(t *testing.T) {
	cg := NewConsumerGroup("group-1")

	m1 := cg.AddMember(newMockConn("c-0"))
	m2 := cg.AddMember(newMockConn("c-1"))

	cg.Rebalance(5) // 5 partitions, 2 consumers → 3 + 2

	a1 := cg.GetAssignments(m1)
	a2 := cg.GetAssignments(m2)

	total := len(a1) + len(a2)
	if total != 5 {
		t.Errorf("total assigned: want 5, got %d", total)
	}

	// First consumer should get the remainder
	if len(a1) != 3 && len(a2) != 3 {
		t.Errorf("one consumer should get 3 partitions for remainder")
	}
}

func TestConsumerGroup_Rebalance_AfterLeave(t *testing.T) {
	cg := NewConsumerGroup("group-1")

	m1 := cg.AddMember(newMockConn("c-0"))
	m2 := cg.AddMember(newMockConn("c-1"))

	cg.Rebalance(4) // 4 partitions, 2 consumers → 2 each

	// Remove one consumer
	cg.RemoveMember(m2)
	cg.Rebalance(4) // 4 partitions, 1 consumer → 4 all

	a1 := cg.GetAssignments(m1)
	if len(a1) != 4 {
		t.Errorf("after leave, remaining consumer should get all 4, got %d", len(a1))
	}
}

func TestConsumerGroup_PerPartitionOffset(t *testing.T) {
	cg := NewConsumerGroup("group-1")

	// Get and advance offset for partition 0
	o1 := cg.GetAndAdvancePartitionOffset(0)
	o2 := cg.GetAndAdvancePartitionOffset(0)
	o3 := cg.GetAndAdvancePartitionOffset(1)

	if o1 != 0 {
		t.Errorf("first offset for p0: want 0, got %d", o1)
	}
	if o2 != 1 {
		t.Errorf("second offset for p0: want 1, got %d", o2)
	}
	if o3 != 0 {
		t.Errorf("first offset for p1: want 0, got %d", o3)
	}
}
