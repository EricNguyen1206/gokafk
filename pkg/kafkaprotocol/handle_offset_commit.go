package kafkaprotocol

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

	// GroupInstanceID
	_, _ = dec.ReadString()

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

			// PartitionLeaderEpoch (added in V13)
			partitionLeaderEpoch, _ := dec.ReadInt32()

			offsetCommitReqs = append(offsetCommitReqs, OffsetCommitRequestInfo{
				Topic:                topicName,
				Partition:            partition,
				Offset:               offset,
				Metadata:             metadata,
				PartitionLeaderEpoch: partitionLeaderEpoch,
			})
		}
	}

	return groupID, offsetCommitReqs, nil
}

func HandleOffsetCommitResponse(correlationId int32) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationId)

	// throttle_time_ms
	enc.WriteInt32(0)

	// Topics array
	enc.WriteInt32(1)
	enc.WriteString("test-topic")
	enc.WriteInt32(1)
	enc.WriteInt32(0)
	enc.WriteInt16(0) // ErrorCode

	return enc.Bytes()
}
