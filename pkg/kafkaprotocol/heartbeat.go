package kafkaprotocol

// HandleHeartbeat builds a HeartbeatResponse (v0+).
// Stateless — just echoes correlationId with no error.
func HandleHeartbeat(correlationId int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationId)
	enc.WriteInt32(0) // throttle_time_ms
	enc.WriteInt16(0) // error_code
	return enc.Bytes()
}
