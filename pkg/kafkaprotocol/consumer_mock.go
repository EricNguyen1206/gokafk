package kafkaprotocol

// HandleJoinGroup mocks a successful JoinGroup response where the member becomes the leader.
func HandleJoinGroup(corrId int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(corrId)

	enc.WriteInt32(0) // throttle_time_ms
	enc.WriteInt16(0) // ErrorCode = 0
	enc.WriteInt32(1) // Generation ID

	// Protocol Name
	enc.WriteString("range")

	// Leader ID (mock)
	enc.WriteString("member-1")

	// Member ID (mock)
	enc.WriteString("member-1")

	// Members array (only 1 member)
	enc.WriteInt32(1)

	// Member 0
	enc.WriteString("member-1")
	// Group instance ID
	enc.WriteString("") // null
	// Metadata
	enc.WriteBytes([]byte{0x00, 0x01}) // dummy metadata

	// Tag buffer
	enc.WriteInt8(0)

	return enc.Bytes()
}

// HandleSyncGroup mocks a successful SyncGroup response with Partition 0 assigned.
func HandleSyncGroup(corrId int32, topic string) []byte {
	enc := NewEncoder()
	enc.WriteInt32(corrId)

	enc.WriteInt32(0) // throttle_time_ms
	enc.WriteInt16(0) // ErrorCode = 0

	// Member Assignment bytes
	asn := NewEncoder()
	asn.WriteInt16(0) // Version
	asn.WriteInt32(1) // 1 topic

	// Topic Assignment 0
	topicName := topic
	if topicName == "" {
		topicName = "test-topic"
	}
	asn.WriteString(topicName)
	asn.WriteInt32(1) // 1 partition
	asn.WriteInt32(0) // Partition 0

	// UserData
	asn.WriteBytes(nil)

	enc.WriteBytes(asn.Bytes())

	// Tag buffer
	enc.WriteInt8(0)

	return enc.Bytes()
}

// HandleOffsetFetch mocks a successful OffsetFetch response.
func HandleOffsetFetch(corrId int32, topic string) []byte {
	enc := NewEncoder()
	enc.WriteInt32(corrId)
	enc.WriteInt32(0) // throttle

	enc.WriteInt32(1) // 1 topic
	topicName := topic
	if topicName == "" {
		topicName = "test-topic"
	}
	enc.WriteString(topicName)
	enc.WriteInt32(1) // 1 partition
	enc.WriteInt32(0) // Request Partition 0
	enc.WriteInt64(0) // Offset 0
	enc.WriteInt32(-1) // Leader Epoch
	enc.WriteString("") // Metadata
	enc.WriteInt16(0) // ErrorCode

	enc.WriteInt16(0) // Top level ErrorCode

	return enc.Bytes()
}
