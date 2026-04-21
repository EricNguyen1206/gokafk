package kafkaprotocol

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

			// We will do a very naive parse of the RecordBatch, assuming one record per batch for the sake of the test.
			// Kafka RecordBatch format (v2): BaseOffset(8), Length(4), PartitionLeaderEpoch(4), Magic(1), CRC(4),
			// Attributes(2), LastOffsetDelta(4), FirstTimestamp(8), MaxTimestamp(8), ProducerId(8), ProducerEpoch(2), BaseSequence(4),
			// RecordsArrayLength(4).
			
			// To keep our hand-rolled parser simple for the tutorial, we will hack it:
			// Just pretend the test sends simple V0 Message Sets or we skip 61 bytes of RecordBatch header.
			// Actually KafkaJS will send modern RecordBatches (Magic=2).
			// Header size before the records array is 61 bytes.
			if dec.Remaining() < 61 {
				return nil, fmt.Errorf("batch too small")
			}
			dec.pos += 61 

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

func HandleProduceResponse(corrId int32, topic string, partition int32, offset int64) []byte {
	enc := NewEncoder()
	enc.WriteInt32(corrId)

	// Responses Array (topics)
	enc.WriteInt32(1)
	enc.WriteString(topic)

	// Partitions Array
	enc.WriteInt32(1)
	enc.WriteInt32(partition)
	enc.WriteInt16(0) // ErrorCode
	enc.WriteInt64(offset) // BaseOffset
	enc.WriteInt64(-1) // LogAppendTimeMs
	enc.WriteInt64(0) // LogStartOffset

	// RecordErrors array (empty)
	enc.WriteInt32(0)

	// ErrorMessage (null string)
	enc.WriteInt16(-1)

	// Throttle time
	enc.WriteInt32(0)

	return enc.Bytes()
}
