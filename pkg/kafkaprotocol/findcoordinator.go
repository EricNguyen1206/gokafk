package kafkaprotocol

// HandleFindCoordinator mocks the FindCoordinatorResponse directing the client to Node 0.
func HandleFindCoordinator(correlationID int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)

	enc.WriteInt32(0)   // throttle_time_ms
	enc.WriteInt16(0)   // ErrorCode = 0 (Success)
	enc.WriteString("") // ErrorMessage = null

	enc.WriteInt32(0)            // Node ID
	enc.WriteString("localhost") // Host
	enc.WriteInt32(10000)        // Port

	// Tag buffer
	enc.WriteInt8(0)

	return enc.Bytes()
}
