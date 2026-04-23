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
