package proto

// ListOffsetsRequestInfo holds parsed info from a single partition in a ListOffsets request.
type ListOffsetsRequestInfo struct {
	Topic     string
	Partition int32
	Timestamp int64 // -1 = Latest, -2 = Earliest, >= 0 = real timestamp query
}

// ParseListOffsetsRequest parses a ListOffsets request (v1+).
// Wire format (after header):
//
//	ReplicaID        int32
//	IsolationLevel   int8   (v2+)
//	Topics Array:
//	  TopicName      string
//	  Partitions Array:
//	    PartitionIndex       int32
//	    CurrentLeaderEpoch   int32 (v4+, we skip)
//	    Timestamp            int64
func ParseListOffsetsRequest(reqData []byte) ([]ListOffsetsRequestInfo, error) {
	dec := NewDecoder(reqData)

	// ReplicaID
	_, _ = dec.ReadInt32()
	// IsolationLevel (v2+)
	_, _ = dec.ReadInt8()

	numTopics, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	var results []ListOffsetsRequestInfo
	for i := 0; i < int(numTopics); i++ {
		topic, _ := dec.ReadString()
		numParts, _ := dec.ReadInt32()

		for p := 0; p < int(numParts); p++ {
			partition, _ := dec.ReadInt32()
			// CurrentLeaderEpoch (v4+)
			_, _ = dec.ReadInt32()
			// Timestamp
			timestamp, _ := dec.ReadInt64()

			results = append(results, ListOffsetsRequestInfo{
				Topic:     topic,
				Partition: partition,
				Timestamp: timestamp,
			})
		}
	}
	return results, nil
}

// ListOffsetsResponseEntry holds the response data for a single partition.
type ListOffsetsResponseEntry struct {
	Topic     string
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// HandleListOffsetsResponse encodes a ListOffsets response (v1+).
// Wire format:
//
//	throttle_time_ms   int32
//	Topics Array:
//	  TopicName        string
//	  Partitions Array:
//	    PartitionIndex int32
//	    ErrorCode      int16
//	    Timestamp      int64
//	    Offset         int64
//	    LeaderEpoch    int32 (v4+)
func HandleListOffsetsResponse(correlationID int32, entries []ListOffsetsResponseEntry) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)
	enc.WriteInt32(0) // throttle_time_ms

	// Group entries by topic
	type partEntry struct {
		Partition int32
		ErrorCode int16
		Timestamp int64
		Offset    int64
	}
	topicMap := make(map[string][]partEntry)
	var topicOrder []string
	for _, e := range entries {
		if _, exists := topicMap[e.Topic]; !exists {
			topicOrder = append(topicOrder, e.Topic)
		}
		topicMap[e.Topic] = append(topicMap[e.Topic], partEntry{
			Partition: e.Partition,
			ErrorCode: e.ErrorCode,
			Timestamp: e.Timestamp,
			Offset:    e.Offset,
		})
	}

	enc.WriteInt32(int32(len(topicOrder)))
	for _, topic := range topicOrder {
		enc.WriteString(topic)
		parts := topicMap[topic]
		enc.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			enc.WriteInt32(p.Partition) // PartitionIndex
			enc.WriteInt16(p.ErrorCode) // ErrorCode
			enc.WriteInt64(p.Timestamp) // Timestamp
			enc.WriteInt64(p.Offset)    // Offset
			enc.WriteInt32(-1)          // LeaderEpoch (v4+, -1 = unknown)
		}
	}

	return enc.Bytes()
}
