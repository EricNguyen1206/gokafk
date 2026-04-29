package broker

import (
	"testing"

	"gokafk/pkg/proto"
)

func TestGroupMetadata_Join_SingleMember(t *testing.T) {
	g := NewGroupMetadata("test-group")

	protocols := []proto.JoinGroupProtocol{
		{Name: "range", Metadata: []byte{0x01}},
	}

	genID, protoName, leaderID, memberID, members := g.Join("", "client-1", protocols)

	if genID != 1 {
		t.Errorf("generationID = %d, want 1", genID)
	}
	if protoName != "range" {
		t.Errorf("protocolName = %q, want %q", protoName, "range")
	}
	// Single member should be leader
	if leaderID != memberID {
		t.Errorf("leaderID = %q, memberID = %q — single member should be leader", leaderID, memberID)
	}
	if len(members) != 1 {
		t.Fatalf("members count = %d, want 1", len(members))
	}
	if members[0].MemberID != memberID {
		t.Errorf("member[0].MemberID = %q, want %q", members[0].MemberID, memberID)
	}
}

func TestGroupMetadata_Join_MultipleMembersLeaderElection(t *testing.T) {
	g := NewGroupMetadata("test-group")

	protocols := []proto.JoinGroupProtocol{
		{Name: "range", Metadata: nil},
	}

	// First member joins — becomes leader
	_, _, leaderID1, memberID1, _ := g.Join("", "client-1", protocols)
	if leaderID1 != memberID1 {
		t.Error("first member should be leader")
	}

	// Second member joins
	genID2, _, leaderID2, memberID2, members := g.Join("", "client-2", protocols)
	if genID2 != 2 {
		t.Errorf("generationID = %d, want 2 (new generation on join)", genID2)
	}
	if memberID2 == memberID1 {
		t.Error("second member should have different ID")
	}
	_ = leaderID2 // leader could be either, just verify both are in members
	if len(members) != 2 {
		t.Errorf("members count = %d, want 2", len(members))
	}
}

func TestGroupMetadata_SyncLeader(t *testing.T) {
	g := NewGroupMetadata("test-group")

	protocols := []proto.JoinGroupProtocol{
		{Name: "range", Metadata: nil},
	}

	_, _, _, memberID, _ := g.Join("", "client-1", protocols)

	// Leader sends SyncGroup with its own assignment
	assignments := []proto.SyncGroupAssignment{
		{MemberID: memberID, Assignment: []byte{0xCA, 0xFE}},
	}

	result, err := g.Sync(memberID, assignments)
	if err != nil {
		t.Fatalf("Sync() error: %v", err)
	}
	if len(result) != 2 || result[0] != 0xCA || result[1] != 0xFE {
		t.Errorf("assignment = %v, want [0xCA 0xFE]", result)
	}
}

func TestGroupMetadata_Leave(t *testing.T) {
	g := NewGroupMetadata("test-group")

	protocols := []proto.JoinGroupProtocol{
		{Name: "range", Metadata: nil},
	}

	_, _, _, memberID, _ := g.Join("", "client-1", protocols)

	g.Leave(memberID)

	g.mu.Lock()
	count := len(g.members)
	g.mu.Unlock()

	if count != 0 {
		t.Errorf("members after leave = %d, want 0", count)
	}
}

func TestGroupMetadata_Leave_LeaderReelection(t *testing.T) {
	g := NewGroupMetadata("test-group")

	protocols := []proto.JoinGroupProtocol{
		{Name: "range", Metadata: nil},
	}

	_, _, _, memberID1, _ := g.Join("", "client-1", protocols)
	_, _, _, memberID2, _ := g.Join("", "client-2", protocols)

	// Remove the leader (first member)
	g.mu.Lock()
	currentLeader := g.leaderID
	g.mu.Unlock()

	g.Leave(currentLeader)

	g.mu.Lock()
	newLeader := g.leaderID
	remaining := len(g.members)
	g.mu.Unlock()

	if remaining != 1 {
		t.Errorf("remaining members = %d, want 1", remaining)
	}
	// New leader should be the other member
	if newLeader != memberID1 && newLeader != memberID2 {
		t.Errorf("new leader %q not one of the members", newLeader)
	}
	if newLeader == currentLeader {
		t.Errorf("new leader should be different from removed leader")
	}
}
