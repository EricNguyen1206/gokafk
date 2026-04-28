package kafkaprotocol

type OffsetFetchRequestInfo struct {
	Topic      string
	Partitions []int32
}

func ParseOffsetFetchRequest(reqData []byte) (string, []OffsetFetchRequestInfo, error) {
	dec := NewDecoder(reqData)

	// GroupID
	groupID, _ := dec.ReadString()

	// Topics Array
	topics32, err := dec.ReadInt32()
	if err != nil {
		return "", nil, err
	}
	topics := int(topics32)

	var fetchReqs []OffsetFetchRequestInfo

	for i := 0; i < topics; i++ {
		// TopicName
		topicName, _ := dec.ReadString()

		// Partitions Array
		partitions32, _ := dec.ReadInt32()
		partitions := int(partitions32)
		var parts []int32

		for j := 0; j < partitions; j++ {
			// Partition
			partition, _ := dec.ReadInt32()
			parts = append(parts, partition)
		}

		fetchReqs = append(fetchReqs, OffsetFetchRequestInfo{
			Topic:      topicName,
			Partitions: parts,
		})
	}

	return groupID, fetchReqs, nil
}

// OffsetFetchPartitionEntry holds the response data for a single partition in an OffsetFetch response.
type OffsetFetchPartitionEntry struct {
	Partition int32
	Offset    int64  // -1 = no committed offset
	Metadata  string
	ErrorCode int16
}

// OffsetFetchResponseEntry holds the response data for a single topic in an OffsetFetch response.
type OffsetFetchResponseEntry struct {
	Topic      string
	Partitions []OffsetFetchPartitionEntry
}

// HandleOffsetFetchResponse encodes an OffsetFetch response with multi-topic/multi-partition support.
// Wire format (v0-v4):
//
//	CorrelationId      int32
//	throttle_time_ms   int32
//	Topics Array:
//	  TopicName        string
//	  Partitions Array:
//	    PartitionIndex int32
//	    CommittedOffset int64
//	    Metadata       string
//	    ErrorCode      int16
//	ErrorCode          int16 (top-level)
func HandleOffsetFetchResponse(correlationID int32, entries []OffsetFetchResponseEntry) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)
	enc.WriteInt32(0) // throttle_time_ms

	enc.WriteInt32(int32(len(entries)))
	for _, entry := range entries {
		enc.WriteString(entry.Topic)
		enc.WriteInt32(int32(len(entry.Partitions)))
		for _, p := range entry.Partitions {
			enc.WriteInt32(p.Partition) // PartitionIndex
			enc.WriteInt64(p.Offset)    // CommittedOffset
			if p.Metadata == "" {
				enc.WriteInt16(-1) // Null metadata
			} else {
				enc.WriteString(p.Metadata)
			}
			enc.WriteInt16(p.ErrorCode) // ErrorCode
		}
	}

	enc.WriteInt16(0) // Top level ErrorCode

	return enc.Bytes()
}
