package kafkaprotocol

// JoinGroupRequest represents a parsed JoinGroup request (v0-v5, non-flexible).
type JoinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	RebalanceTimeoutMs int32
	MemberID         string
	GroupInstanceID  string // nullable
	ProtocolType     string
	Protocols        []JoinGroupProtocol
}

// JoinGroupProtocol represents a single protocol entry in JoinGroup request.
type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupMember represents a member entry in JoinGroup response.
type JoinGroupMember struct {
	MemberID        string
	GroupInstanceID string
	Metadata        []byte
}

// ParseJoinGroupRequest parses a JoinGroup request body (v5, non-flexible).
// Fields: GroupId, SessionTimeoutMs, RebalanceTimeoutMs, MemberId, GroupInstanceId, ProtocolType, Protocols[]
func ParseJoinGroupRequest(data []byte) (*JoinGroupRequest, error) {
	dec := NewDecoder(data)

	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	sessionTimeout, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	rebalanceTimeout, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	// GroupInstanceId — nullable string (int16 = -1 means null)
	groupInstanceID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	protocolType, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	// Protocols array
	numProtocols, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	protocols := make([]JoinGroupProtocol, 0, numProtocols)
	for i := 0; i < int(numProtocols); i++ {
		name, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		metadata, err := dec.ReadBytes()
		if err != nil {
			return nil, err
		}
		protocols = append(protocols, JoinGroupProtocol{
			Name:     name,
			Metadata: metadata,
		})
	}

	return &JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   sessionTimeout,
		RebalanceTimeoutMs: rebalanceTimeout,
		MemberID:           memberID,
		GroupInstanceID:    groupInstanceID,
		ProtocolType:       protocolType,
		Protocols:          protocols,
	}, nil
}

// HandleJoinGroupResponse builds a JoinGroup response (v5, non-flexible).
// Fields: CorrelationId, ThrottleTimeMs, ErrorCode, GenerationId, ProtocolName, LeaderId, MemberId, Members[]
func HandleJoinGroupResponse(correlationId int32, errorCode int16, generationId int32, protocolName string, leaderID string, memberID string, members []JoinGroupMember) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationId)

	enc.WriteInt32(0)         // throttle_time_ms
	enc.WriteInt16(errorCode) // ErrorCode
	enc.WriteInt32(generationId)

	// Protocol Name
	enc.WriteString(protocolName)

	// Leader ID
	enc.WriteString(leaderID)

	// Member ID
	enc.WriteString(memberID)

	// Members array
	enc.WriteInt32(int32(len(members)))
	for _, m := range members {
		enc.WriteString(m.MemberID)
		// Group instance ID (nullable — write empty string for null)
		enc.WriteString(m.GroupInstanceID)
		// Metadata
		enc.WriteBytes(m.Metadata)
	}

	return enc.Bytes()
}
