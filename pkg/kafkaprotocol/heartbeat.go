package kafkaprotocol

// HandleHeartbeat builds a HeartbeatResponse (v0+).
// Stateless — just echoes correlationID with no error.
func HandleHeartbeat(correlationID int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)
	enc.WriteInt32(0) // throttle_time_ms
	enc.WriteInt16(0) // error_code
	return enc.Bytes()
}
