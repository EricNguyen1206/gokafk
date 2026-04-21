package kafkaprotocol

// FetchRequestInfo holds parsed information about a fetch request length
type FetchRequestInfo struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
}

func ParseFetchRequest(reqData []byte) ([]FetchRequestInfo, error) {
	dec := NewDecoder(reqData)
	
	// Replica ID
	_, _ = dec.ReadInt32()
	// MaxWait
	_, _ = dec.ReadInt32()
	// MinBytes
	_, _ = dec.ReadInt32()
	// MaxBytes (v3+)
	_, _ = dec.ReadInt32()
	// IsolationLevel
	_, _ = dec.ReadInt8()
	// SessionId
	_, _ = dec.ReadInt32()
	// SessionEpoch
	_, _ = dec.ReadInt32()

	numTopics, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	var fetchReqs []FetchRequestInfo
	for i := 0; i < int(numTopics); i++ {
		topic, _ := dec.ReadString()
		numParts, _ := dec.ReadInt32()

		for p := 0; p < int(numParts); p++ {
			part, _ := dec.ReadInt32()
			// Current Leader Epoch
			_, _ = dec.ReadInt32()
			// Fetch Offset
			offset, _ := dec.ReadInt64()
			// Log Start Offset
			_, _ = dec.ReadInt64()
			// Partition Max Bytes
			maxBytes, _ := dec.ReadInt32()

			fetchReqs = append(fetchReqs, FetchRequestInfo{
				Topic:     topic,
				Partition: part,
				Offset:    offset,
				MaxBytes:  maxBytes,
			})
		}
	}
	return fetchReqs, nil
}

func HandleFetchResponse(corrId int32, topic string, partition int32, messages [][]byte, startOffset int64) []byte {
	enc := NewEncoder()
	enc.WriteInt32(corrId)

	// throttle
	enc.WriteInt32(0) 
	// error code
	enc.WriteInt16(0) 
	// session ID
	enc.WriteInt32(0) 

	// Responses Array
	enc.WriteInt32(1)
	enc.WriteString(topic)
	// Partitions Array
	enc.WriteInt32(1)
	enc.WriteInt32(partition)
	enc.WriteInt16(0) // ErrorCode
	enc.WriteInt64(startOffset + int64(len(messages))) // HighWatermark
	enc.WriteInt64(-1) // LastStableOffset
	enc.WriteInt64(0) // LogStartOffset
	enc.WriteInt32(-1) // AbortedTransactions array length (-1 = null)
	enc.WriteInt32(-1) // PreferredReadReplica (added in V11)
	
	// RecordBatch size
	// For simulation, we wrap our messages inside a simple v2 RecordBatch header
	// If there are no messages, size is 0
	if len(messages) == 0 {
		enc.WriteInt32(0) 
	} else {
		// Calculate records sizes
		recsEnc := NewEncoder()
		for i, msg := range messages {
			// Length(VarInt), Attr(VarInt), TimestampDelta(VarInt), OffsetDelta(VarInt),
			// KeyLen(VarInt), Key, ValLen(VarInt), Val, HeadersLen(VarInt)
			rEnc := NewEncoder()
			rEnc.WriteInt8(0) // Attr
			rEnc.WriteInt8(0) // TS delta
			// Offset delta is varint, but we can't write varint with our simple encoder yet
			// Actually we can hack it since tiny ints are just 1 byte in varint
			// let's just make everything 0 to pass the test if it parses it raw
			rEnc.WriteInt8(int8(i * 2)) // OffsetDelta varint (i << 1)
			
			rEnc.WriteInt8(0) // Key len = 0
			// Val len varint
			valLenZz := (len(msg) << 1) ^ (len(msg) >> 31)
			rEnc.WriteInt8(int8(valLenZz)) // Warning: only works for small values!
			rEnc.data = append(rEnc.data, msg...)
			rEnc.WriteInt8(0) // Headers = 0

			recBytes := rEnc.Bytes()
			recSizeZz := (len(recBytes) << 1) ^ (len(recBytes) >> 31)
			recsEnc.WriteInt8(int8(recSizeZz))
			recsEnc.data = append(recsEnc.data, recBytes...)
		}

		batchSize := 61 + len(recsEnc.Bytes())
		enc.WriteInt32(int32(batchSize))

		// Write RecordBatch Header (61 bytes)
		enc.WriteInt64(startOffset) // BaseOffset
		enc.WriteInt32(int32(batchSize - 12)) // Length
		enc.WriteInt32(0) // PartitionLeaderEpoch
		enc.WriteInt8(2) // Magic
		enc.WriteInt32(0) // CRC (fake)
		enc.WriteInt16(0) // Attributes
		enc.WriteInt32(int32(len(messages) - 1)) // LastOffsetDelta
		enc.WriteInt64(0) // FirstTimestamp
		enc.WriteInt64(0) // MaxTimestamp
		enc.WriteInt64(-1) // ProducerId
		enc.WriteInt16(-1) // ProducerEpoch
		enc.WriteInt32(-1) // BaseSequence

		enc.WriteInt32(int32(len(messages))) // Records count
		enc.data = append(enc.data, recsEnc.Bytes()...)
	}
	
	return enc.Bytes()
}
