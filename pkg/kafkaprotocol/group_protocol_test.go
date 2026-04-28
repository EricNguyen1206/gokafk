package kafkaprotocol

import (
	"testing"
)

func TestParseJoinGroupRequest(t *testing.T) {
	// Build a JoinGroup request
	enc := NewEncoder()
	enc.WriteString("test-group")    // GroupId
	enc.WriteInt32(30000)            // SessionTimeoutMs
	enc.WriteInt32(60000)            // RebalanceTimeoutMs
	enc.WriteString("")              // MemberId (empty = new member)
	enc.WriteString("")              // GroupInstanceId (null)
	enc.WriteString("consumer")      // ProtocolType

	// Protocols array (1 protocol)
	enc.WriteInt32(1)
	enc.WriteString("range")                   // Protocol Name
	enc.WriteBytes([]byte{0x00, 0x01, 0x02})   // Protocol Metadata

	req, err := ParseJoinGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseJoinGroupRequest() error: %v", err)
	}

	if req.GroupID != "test-group" {
		t.Errorf("GroupID = %q, want %q", req.GroupID, "test-group")
	}
	if req.SessionTimeoutMs != 30000 {
		t.Errorf("SessionTimeoutMs = %d, want 30000", req.SessionTimeoutMs)
	}
	if req.RebalanceTimeoutMs != 60000 {
		t.Errorf("RebalanceTimeoutMs = %d, want 60000", req.RebalanceTimeoutMs)
	}
	if req.MemberID != "" {
		t.Errorf("MemberID = %q, want empty", req.MemberID)
	}
	if req.ProtocolType != "consumer" {
		t.Errorf("ProtocolType = %q, want %q", req.ProtocolType, "consumer")
	}
	if len(req.Protocols) != 1 {
		t.Fatalf("Protocols count = %d, want 1", len(req.Protocols))
	}
	if req.Protocols[0].Name != "range" {
		t.Errorf("Protocol.Name = %q, want %q", req.Protocols[0].Name, "range")
	}
}

func TestJoinGroupResponseRoundTrip(t *testing.T) {
	members := []JoinGroupMember{
		{MemberID: "member-1", GroupInstanceID: "", Metadata: []byte{0x01}},
		{MemberID: "member-2", GroupInstanceID: "", Metadata: []byte{0x02}},
	}

	resp := HandleJoinGroupResponse(42, 0, 1, "range", "member-1", "member-1", members)

	dec := NewDecoder(resp)

	// CorrelationId
	corrId, _ := dec.ReadInt32()
	if corrId != 42 {
		t.Errorf("CorrelationId = %d, want 42", corrId)
	}

	// ThrottleTimeMs
	throttle, _ := dec.ReadInt32()
	if throttle != 0 {
		t.Errorf("ThrottleTimeMs = %d, want 0", throttle)
	}

	// ErrorCode
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("ErrorCode = %d, want 0", errCode)
	}

	// GenerationId
	genId, _ := dec.ReadInt32()
	if genId != 1 {
		t.Errorf("GenerationId = %d, want 1", genId)
	}

	// ProtocolName
	proto, _ := dec.ReadString()
	if proto != "range" {
		t.Errorf("ProtocolName = %q, want %q", proto, "range")
	}

	// LeaderId
	leader, _ := dec.ReadString()
	if leader != "member-1" {
		t.Errorf("LeaderId = %q, want %q", leader, "member-1")
	}

	// MemberId
	member, _ := dec.ReadString()
	if member != "member-1" {
		t.Errorf("MemberId = %q, want %q", member, "member-1")
	}

	// Members array
	numMembers, _ := dec.ReadInt32()
	if numMembers != 2 {
		t.Errorf("Members count = %d, want 2", numMembers)
	}
}

func TestParseSyncGroupRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("test-group") // GroupId
	enc.WriteInt32(1)             // GenerationId
	enc.WriteString("member-1")   // MemberId
	enc.WriteString("")            // GroupInstanceId

	// Assignments array (1 assignment)
	enc.WriteInt32(1)
	enc.WriteString("member-1")                      // MemberId
	enc.WriteBytes([]byte{0x00, 0x00, 0x01, 0x02})   // Assignment bytes

	req, err := ParseSyncGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseSyncGroupRequest() error: %v", err)
	}

	if req.GroupID != "test-group" {
		t.Errorf("GroupID = %q, want %q", req.GroupID, "test-group")
	}
	if req.GenerationID != 1 {
		t.Errorf("GenerationID = %d, want 1", req.GenerationID)
	}
	if req.MemberID != "member-1" {
		t.Errorf("MemberID = %q, want %q", req.MemberID, "member-1")
	}
	if len(req.Assignments) != 1 {
		t.Fatalf("Assignments count = %d, want 1", len(req.Assignments))
	}
	if req.Assignments[0].MemberID != "member-1" {
		t.Errorf("Assignment.MemberID = %q, want %q", req.Assignments[0].MemberID, "member-1")
	}
}

func TestSyncGroupResponseRoundTrip(t *testing.T) {
	assignment := []byte{0x00, 0x01, 0x02, 0x03}
	resp := HandleSyncGroupResponse(99, 0, assignment)

	dec := NewDecoder(resp)
	corrId, _ := dec.ReadInt32()
	if corrId != 99 {
		t.Errorf("CorrelationId = %d, want 99", corrId)
	}
	throttle, _ := dec.ReadInt32()
	if throttle != 0 {
		t.Errorf("ThrottleTimeMs = %d, want 0", throttle)
	}
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("ErrorCode = %d, want 0", errCode)
	}
	data, _ := dec.ReadBytes()
	if len(data) != 4 || data[0] != 0x00 {
		t.Errorf("Assignment = %v, want [0 1 2 3]", data)
	}
}

func TestParseLeaveGroupRequest(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("test-group") // GroupId
	enc.WriteString("member-1")   // MemberId

	req, err := ParseLeaveGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseLeaveGroupRequest() error: %v", err)
	}

	if req.GroupID != "test-group" {
		t.Errorf("GroupID = %q, want %q", req.GroupID, "test-group")
	}
	if req.MemberID != "member-1" {
		t.Errorf("MemberID = %q, want %q", req.MemberID, "member-1")
	}
}

func TestLeaveGroupResponseRoundTrip(t *testing.T) {
	resp := HandleLeaveGroupResponse(77, 0)

	dec := NewDecoder(resp)
	corrId, _ := dec.ReadInt32()
	if corrId != 77 {
		t.Errorf("CorrelationId = %d, want 77", corrId)
	}
	throttle, _ := dec.ReadInt32()
	if throttle != 0 {
		t.Errorf("ThrottleTimeMs = %d, want 0", throttle)
	}
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("ErrorCode = %d, want 0", errCode)
	}
}

func TestParseJoinGroupRequest_Truncated(t *testing.T) {
	_, err := ParseJoinGroupRequest([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	enc := NewEncoder()
	enc.WriteString("group")
	// Missing remaining fields
	_, err = ParseJoinGroupRequest(enc.Bytes())
	if err == nil {
		t.Error("expected error for truncated data after GroupID")
	}
}

func TestParseJoinGroupRequest_MultipleProtocols(t *testing.T) {
	enc := NewEncoder()
	enc.WriteString("group")
	enc.WriteInt32(30000)
	enc.WriteInt32(60000)
	enc.WriteString("member-1")
	enc.WriteString("")
	enc.WriteString("consumer")
	enc.WriteInt32(2)
	enc.WriteString("range")
	enc.WriteBytes([]byte{0x01})
	enc.WriteString("roundrobin")
	enc.WriteBytes([]byte{0x02})

	req, err := ParseJoinGroupRequest(enc.Bytes())
	if err != nil {
		t.Fatalf("ParseJoinGroupRequest: %v", err)
	}
	if len(req.Protocols) != 2 {
		t.Errorf("Protocols: want 2, got %d", len(req.Protocols))
	}
	if req.Protocols[1].Name != "roundrobin" {
		t.Errorf("Protocol[1].Name: want 'roundrobin', got %q", req.Protocols[1].Name)
	}
}

func TestParseSyncGroupRequest_Truncated(t *testing.T) {
	_, err := ParseSyncGroupRequest([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	enc := NewEncoder()
	enc.WriteString("group")
	// Missing remaining fields
	_, err = ParseSyncGroupRequest(enc.Bytes())
	if err == nil {
		t.Error("expected error for truncated data after GroupID")
	}
}

func TestParseLeaveGroupRequest_Truncated(t *testing.T) {
	_, err := ParseLeaveGroupRequest([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	enc := NewEncoder()
	enc.WriteString("group")
	// Missing MemberID
	_, err = ParseLeaveGroupRequest(enc.Bytes())
	if err == nil {
		t.Error("expected error for truncated data after GroupID")
	}
}
