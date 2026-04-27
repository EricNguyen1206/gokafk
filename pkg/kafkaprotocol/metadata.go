package kafkaprotocol

// HandleMetadata parses a minimal MetadataRequest and returns a single mocked partition pointing to localhost:10000.
func HandleMetadata(correlationID int32, reqData []byte) []byte {
	// Parse requested topics
	dec := NewDecoder(reqData)
	numTopics, _ := dec.ReadInt32()
	var topics []string
	for i := 0; i < int(numTopics); i++ {
		t, _ := dec.ReadString()
		topics = append(topics, t)
	}

	enc := NewEncoder()
	enc.WriteInt32(correlationID)

	enc.WriteInt32(0) // throttle_time_ms

	// Brokers Array (1 broker)
	enc.WriteInt32(1)
	enc.WriteInt32(0)            // Node ID
	enc.WriteString("localhost") // Host
	enc.WriteInt32(10000)        // Port
	enc.WriteString("")          // Rack

	// Cluster ID
	enc.WriteString("gokafk-cluster")
	// Controller ID
	enc.WriteInt32(0)

	// Topics Array
	if len(topics) == 0 {
		topics = []string{"test-topic"} // Default fallback
	}
	enc.WriteInt32(int32(len(topics)))
	for _, t := range topics {
		enc.WriteInt16(0) // ErrorCode = 0
		enc.WriteString(t)
		enc.WriteInt8(0) // IsInternal = false

		// Partitions Array (1 partition)
		enc.WriteInt32(1)
		enc.WriteInt16(0) // ErrorCode = 0
		enc.WriteInt32(0) // Partition Index = 0
		enc.WriteInt32(0) // Leader = Node 0

		// Replicas (Node 0)
		enc.WriteInt32(1)
		enc.WriteInt32(0)

		// Isr (Node 0)
		enc.WriteInt32(1)
		enc.WriteInt32(0)

		// Offline Replicas (Empty)
		enc.WriteInt32(0)
	}

	// Tag buffer
	enc.WriteInt8(0)

	return enc.Bytes()
}
