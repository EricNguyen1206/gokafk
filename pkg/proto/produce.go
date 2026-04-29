package proto

import "fmt"

type ProduceRecord struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
}

func ParseProduceRequest(reqData []byte) ([]ProduceRecord, error) {
	dec := NewDecoder(reqData)

	// TransactionalId (v3+)
	_, _ = dec.ReadString()
	// Acks
	_, _ = dec.ReadInt16()
	// Timeout
	_, _ = dec.ReadInt32()

	numTopics, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}

	var records []ProduceRecord

	for i := 0; i < int(numTopics); i++ {
		topic, _ := dec.ReadString()
		numParts, _ := dec.ReadInt32()

		for p := 0; p < int(numParts); p++ {
			part, _ := dec.ReadInt32()

			// RecordBatch Size
			batchSize, _ := dec.ReadInt32()
			if batchSize <= 0 {
				continue
			}

			// TODO: Implement proper RecordBatch v2 parsing.
			// Current implementation is a naive "hack" for the very first version:
			// - Skips 57 bytes of RecordBatch header (BaseOffset through BaseSequence).
			// - Then reads NumRecords (4 bytes) separately below.
			// - Assumes one record per batch for testing purposes.
			// RecordBatch v2 header: BaseOffset(8) + BatchLength(4) + PartitionLeaderEpoch(4) +
			//   Magic(1) + CRC(4) + Attributes(2) + LastOffsetDelta(4) + FirstTimestamp(8) +
			//   MaxTimestamp(8) + ProducerId(8) + ProducerEpoch(2) + BaseSequence(4) = 57 bytes
			// Then NumRecords(4) is read by ReadInt32 below.
			if dec.Remaining() < 61 { // 57 header + 4 for NumRecords
				return nil, fmt.Errorf("batch too small")
			}
			dec.pos += 57

			// Records Array
			numRecs, _ := dec.ReadInt32()
			for r := 0; r < int(numRecs); r++ {
				// Record: Length(VarInt), Attributes(VarInt), TimestampDelta(VarInt), OffsetDelta(VarInt),
				// KeyLength(VarInt), Key, ValueLength(VarInt), Value, HeadersArray(VarInt).
				// We'll have to parse VarInt.

				_, _ = dec.ReadVarInt() // length
				_, _ = dec.ReadVarInt() // attr
				_, _ = dec.ReadVarInt() // ts delta
				_, _ = dec.ReadVarInt() // offset delta

				keyLen, _ := dec.ReadVarInt()
				var key []byte
				if keyLen > 0 {
					key = dec.data[dec.pos : dec.pos+int(keyLen)]
					dec.pos += int(keyLen)
				}

				valLen, _ := dec.ReadVarInt()
				var val []byte
				if valLen > 0 {
					val = dec.data[dec.pos : dec.pos+int(valLen)]
					dec.pos += int(valLen)
				}

				records = append(records, ProduceRecord{
					Topic:     topic,
					Partition: part,
					Key:       key,
					Value:     val,
				})

				// Headers array Length
				numHeaders, _ := dec.ReadVarInt()
				for h := 0; h < int(numHeaders); h++ {
					kLen, _ := dec.ReadVarInt()
					dec.pos += int(kLen)
					vLen, _ := dec.ReadVarInt()
					dec.pos += int(vLen)
				}
			}
		}
	}

	return records, nil
}

// Return encoded response message for ProduceResponse in binary
// Message format: Correlation ID + Topic + Partition + ErrorCode + BaseOffset + LogAppendTimeMs + LogStartOffset + RecordErrors + ErrorMessage + ThrottleTime
func HandleProduceResponse(correlationID int32, topic string, partition int32, offset int64) []byte {
	enc := NewEncoder()
	enc.WriteInt32(correlationID) // Correlation ID

	// Responses Array (topics)
	enc.WriteInt32(1)
	enc.WriteString(topic)

	// Partitions Array
	enc.WriteInt32(1)         // Length of Patitions Array = 1
	enc.WriteInt32(partition) // Partition
	enc.WriteInt16(0)         // ErrorCode
	enc.WriteInt64(offset)    // BaseOffset
	enc.WriteInt64(-1)        // LogAppendTimeMs
	enc.WriteInt64(0)         // LogStartOffset

	// RecordErrors array (empty)
	enc.WriteInt32(0)

	// ErrorMessage (null string)
	enc.WriteInt16(-1)

	// Throttle time
	enc.WriteInt32(0)

	return enc.Bytes()
}
