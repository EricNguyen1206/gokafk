package proto

// LeaveGroupRequest represents a parsed LeaveGroup request (v0-v2, non-flexible).
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// ParseLeaveGroupRequest parses a LeaveGroup request body (v2, non-flexible).
// Fields: GroupId, MemberId
func ParseLeaveGroupRequest(data []byte) (*LeaveGroupRequest, error) {
	dec := NewDecoder(data)

	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	return &LeaveGroupRequest{
		GroupID:  groupID,
		MemberID: memberID,
	}, nil
}

// HandleLeaveGroupResponse builds a LeaveGroup response (v2, non-flexible).
// Fields: CorrelationId, ThrottleTimeMs, ErrorCode
func HandleLeaveGroupResponse(correlationID int32, errorCode int16) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)

	enc.WriteInt32(0)         // throttle_time_ms
	enc.WriteInt16(errorCode) // ErrorCode
	enc.WriteInt32(0)         // Members array length (v3+)

	return enc.Bytes()
}
