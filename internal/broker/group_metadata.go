package broker

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gokafk/pkg/kafkaprotocol"
)

// GroupMetadata manages the group coordination protocol (JoinGroup/SyncGroup/LeaveGroup).
// This is separate from ConsumerGroup which handles offset tracking.
type GroupMetadata struct {
	mu           sync.Mutex
	groupID      string
	generationID int32
	protocolName string
	leaderID     string
	members      map[string]*MemberMetadata
	assignments  map[string][]byte // memberID → raw assignment bytes from leader
	assignReady  chan struct{}     // closed when leader submits assignments via SyncGroup
}

// MemberMetadata represents a member in a coordinated group.
type MemberMetadata struct {
	memberID  string
	clientID  string
	protocols []kafkaprotocol.JoinGroupProtocol
}

// NewGroupMetadata creates a new coordinated group.
func NewGroupMetadata(groupID string) *GroupMetadata {
	return &GroupMetadata{
		groupID:     groupID,
		members:     make(map[string]*MemberMetadata),
		assignments: make(map[string][]byte),
		assignReady: make(chan struct{}),
	}
}

// Join handles a JoinGroup request. Registers the member, elects leader, increments generation.
// Returns: generationID, protocolName, leaderID, memberID, members list
func (g *GroupMetadata) Join(memberID, clientID string, protocols []kafkaprotocol.JoinGroupProtocol) (int32, string, string, string, []kafkaprotocol.JoinGroupMember) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Generate member ID if empty
	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", clientID, time.Now().UnixNano())
	}

	// Register member
	g.members[memberID] = &MemberMetadata{
		memberID:  memberID,
		clientID:  clientID,
		protocols: protocols,
	}

	// First member becomes leader
	if g.leaderID == "" || len(g.members) == 1 {
		g.leaderID = memberID
	}

	// Select protocol (use first protocol from the first member — simplified)
	if len(protocols) > 0 {
		g.protocolName = protocols[0].Name
	}

	// New generation
	g.generationID++

	// Reset assignments for new generation
	g.assignments = make(map[string][]byte)
	g.assignReady = make(chan struct{})

	slog.Info("group join",
		"group", g.groupID,
		"member", memberID,
		"leader", g.leaderID,
		"generation", g.generationID,
		"members", len(g.members),
	)

	// Build member list (only leader gets full list with metadata in real Kafka,
	// but for simplicity we send it to all — KafkaJS handles both cases)
	members := make([]kafkaprotocol.JoinGroupMember, 0, len(g.members))
	for _, m := range g.members {
		var metadata []byte
		if len(m.protocols) > 0 {
			metadata = m.protocols[0].Metadata
		}
		members = append(members, kafkaprotocol.JoinGroupMember{
			MemberID:        m.memberID,
			GroupInstanceID: "",
			Metadata:        metadata,
		})
	}

	return g.generationID, g.protocolName, g.leaderID, memberID, members
}

// Sync handles a SyncGroup request.
// Leader provides assignments for all members; followers wait for assignments.
// Returns: assignment bytes for the requesting member.
func (g *GroupMetadata) Sync(memberID string, assignments []kafkaprotocol.SyncGroupAssignment) ([]byte, error) {
	g.mu.Lock()

	isLeader := memberID == g.leaderID

	if isLeader && len(assignments) > 0 {
		// Leader provides assignments for all members
		for _, a := range assignments {
			g.assignments[a.MemberID] = a.Assignment
		}
		// Signal followers that assignments are ready
		close(g.assignReady)

		slog.Info("group sync (leader)",
			"group", g.groupID,
			"member", memberID,
			"assignments", len(assignments),
		)

		// Return leader's own assignment
		result := g.assignments[memberID]
		g.mu.Unlock()
		return result, nil
	}

	// Follower: wait for leader to submit assignments
	ch := g.assignReady
	g.mu.Unlock()

	select {
	case <-ch:
		// Assignments ready
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("sync group timeout waiting for leader assignments")
	}

	g.mu.Lock()
	result := g.assignments[memberID]
	g.mu.Unlock()

	slog.Info("group sync (follower)",
		"group", g.groupID,
		"member", memberID,
		"hasAssignment", result != nil,
	)

	return result, nil
}

// Leave removes a member from the group.
func (g *GroupMetadata) Leave(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.members, memberID)

	// If leader left, elect new leader
	if g.leaderID == memberID {
		g.leaderID = ""
		for id := range g.members {
			g.leaderID = id
			break
		}
	}

	slog.Info("group leave",
		"group", g.groupID,
		"member", memberID,
		"remaining", len(g.members),
	)
}

// --- Broker-level handler methods ---

func (b *Broker) getOrCreateGroupMetadata(groupID string) *GroupMetadata {
	b.mu.Lock()
	defer b.mu.Unlock()

	g, ok := b.groups[groupID]
	if !ok {
		g = NewGroupMetadata(groupID)
		b.groups[groupID] = g
	}
	return g
}

func (b *Broker) handleJoinGroup(correlationId int32, data []byte) ([]byte, error) {
	req, err := kafkaprotocol.ParseJoinGroupRequest(data)
	if err != nil {
		slog.Error("parse join group failed", "err", err)
		return kafkaprotocol.HandleJoinGroupResponse(correlationId, 76, 0, "", "", "", nil), nil // 76 = GROUP_AUTHORIZATION_FAILED
	}

	g := b.getOrCreateGroupMetadata(req.GroupID)
	generationID, protocolName, leaderID, memberID, members := g.Join(req.MemberID, "", req.Protocols)

	return kafkaprotocol.HandleJoinGroupResponse(
		correlationId,
		0, // no error
		generationID,
		protocolName,
		leaderID,
		memberID,
		members,
	), nil
}

func (b *Broker) handleSyncGroup(correlationId int32, data []byte) ([]byte, error) {
	req, err := kafkaprotocol.ParseSyncGroupRequest(data)
	if err != nil {
		slog.Error("parse sync group failed", "err", err)
		return kafkaprotocol.HandleSyncGroupResponse(correlationId, 76, nil), nil
	}

	g := b.getOrCreateGroupMetadata(req.GroupID)
	assignment, err := g.Sync(req.MemberID, req.Assignments)
	if err != nil {
		slog.Error("sync group failed", "err", err)
		return kafkaprotocol.HandleSyncGroupResponse(correlationId, 25, nil), nil // 25 = REBALANCE_IN_PROGRESS
	}

	return kafkaprotocol.HandleSyncGroupResponse(correlationId, 0, assignment), nil
}

func (b *Broker) handleLeaveGroup(correlationId int32, data []byte) ([]byte, error) {
	req, err := kafkaprotocol.ParseLeaveGroupRequest(data)
	if err != nil {
		slog.Error("parse leave group failed", "err", err)
		return kafkaprotocol.HandleLeaveGroupResponse(correlationId, 76), nil
	}

	g := b.getOrCreateGroupMetadata(req.GroupID)
	g.Leave(req.MemberID)

	return kafkaprotocol.HandleLeaveGroupResponse(correlationId, 0), nil
}
