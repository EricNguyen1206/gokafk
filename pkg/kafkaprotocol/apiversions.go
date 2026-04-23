package kafkaprotocol

// HandleApiVersions returns a static/mocked ApiVersion response stating we support basic APIs
func HandleApiVersions(correlationId int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationId)

	enc.WriteInt16(0) // ErrorCode = 0 (Success)

	// API Keys supported
	apis := []struct {
		Key int16
		Min int16
		Max int16
	}{
		{ApiKeyProduce, 0, 8},
		{ApiKeyFetch, 0, 11},
		{ApiKeyListOffsets, 0, 5},
		{ApiKeyMetadata, 0, 9},
		{ApiKeyOffsetCommit, 0, 8},
		{ApiKeyOffsetFetch, 0, 5},
		{ApiKeyFindCoordinator, 0, 3},
		{ApiKeyJoinGroup, 0, 7},
		{ApiKeyHeartbeat, 0, 4},
		{ApiKeyLeaveGroup, 0, 4},
		{ApiKeySyncGroup, 0, 5},
		{ApiKeyApiVersions, 0, 3},
	}

	enc.WriteInt32(int32(len(apis))) // Array length
	for _, a := range apis {
		enc.WriteInt16(a.Key)
		enc.WriteInt16(a.Min)
		enc.WriteInt16(a.Max)
	}

	enc.WriteInt32(0) // throttle_time_ms
	// Tag buffer for v3+ (empty)
	enc.WriteInt8(0) // empty tagged fields

	return enc.Bytes()
}
