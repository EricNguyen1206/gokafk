package kafkaprotocol

// SyncGroupRequest represents a parsed SyncGroup request (v0-v3, non-flexible).
type SyncGroupRequest struct {
	GroupID         string
	GenerationID   int32
	MemberID        string
	GroupInstanceID string // nullable, v3+
	Assignments     []SyncGroupAssignment
}

// SyncGroupAssignment represents a single member assignment in SyncGroup request.
type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// ParseSyncGroupRequest parses a SyncGroup request body (v3, non-flexible).
// Fields: GroupId, GenerationId, MemberId, GroupInstanceId, Assignments[]
func ParseSyncGroupRequest(data []byte) (*SyncGroupRequest, error) {
	dec := NewDecoder(data)

	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	generationID, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	// GroupInstanceId — nullable string (v3+)
	groupInstanceID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	// Assignments array
	numAssignments, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	assignments := make([]SyncGroupAssignment, 0, numAssignments)
	for i := 0; i < int(numAssignments); i++ {
		mID, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		assignment, err := dec.ReadBytes()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, SyncGroupAssignment{
			MemberID:   mID,
			Assignment: assignment,
		})
	}

	return &SyncGroupRequest{
		GroupID:         groupID,
		GenerationID:   generationID,
		MemberID:        memberID,
		GroupInstanceID: groupInstanceID,
		Assignments:     assignments,
	}, nil
}

// HandleSyncGroupResponse builds a SyncGroup response (v3, non-flexible).
// Fields: CorrelationId, ThrottleTimeMs, ErrorCode, Assignment
func HandleSyncGroupResponse(correlationId int32, errorCode int16, assignment []byte) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationId)

	enc.WriteInt32(0)         // throttle_time_ms
	enc.WriteInt16(errorCode) // ErrorCode

	// Member assignment bytes
	enc.WriteBytes(assignment)

	return enc.Bytes()
}
