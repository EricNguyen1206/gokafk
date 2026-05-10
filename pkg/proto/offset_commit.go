package proto

type OffsetCommitRequestInfo struct {
	Topic                string
	Partition            int32
	Offset               int64
	Metadata             string
	PartitionLeaderEpoch int32
}

func ParseOffsetCommitRequest(reqData []byte) (string, []OffsetCommitRequestInfo, error) {
	dec := NewDecoder(reqData)

	// GroupID
	groupID, _ := dec.ReadString()

	// GenerationID
	_, _ = dec.ReadInt32()

	// MemberID
	_, _ = dec.ReadString()

	// RetentionTime (added in V2)
	_, _ = dec.ReadInt64()

	// Topics Array
	topics32, err := dec.ReadInt32()
	if err != nil {
		return "", nil, err
	}
	topics := int(topics32)

	var offsetCommitReqs []OffsetCommitRequestInfo

	for i := 0; i < topics; i++ {
		// TopicName
		topicName, _ := dec.ReadString()

		// Partitions Array
		partitions32, _ := dec.ReadInt32()
		partitions := int(partitions32)

		for j := 0; j < partitions; j++ {
			// Partition
			partition, _ := dec.ReadInt32()

			// Offset
			offset, _ := dec.ReadInt64()

			// Metadata
			metadata, _ := dec.ReadString()

			offsetCommitReqs = append(offsetCommitReqs, OffsetCommitRequestInfo{
				Topic:     topicName,
				Partition: partition,
				Offset:    offset,
				Metadata:  metadata,
			})
		}
	}

	return groupID, offsetCommitReqs, nil
}

func HandleOffsetCommitResponse(correlationID int32, reqs []OffsetCommitRequestInfo) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID)

	// throttle_time_ms
	enc.WriteInt32(0)

	// Group topics by name
	topicMap := make(map[string][]int32)
	for _, req := range reqs {
		topicMap[req.Topic] = append(topicMap[req.Topic], req.Partition)
	}

	// Topics array
	enc.WriteInt32(int32(len(topicMap)))
	for topicName, partitions := range topicMap {
		enc.WriteString(topicName)
		enc.WriteInt32(int32(len(partitions)))
		for _, p := range partitions {
			enc.WriteInt32(p)
			enc.WriteInt16(0) // ErrorCode
		}
	}

	return enc.Bytes()
}
