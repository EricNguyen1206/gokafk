package broker

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

const ConsumerOffsetsTopic = "__consumer_offsets"

// recoverConsumerOffsets reads the internal topic and populates the in-memory offsets map
func (b *Broker) recoverConsumerOffsets() error {
	tp, err := b.getOrCreateTopic(ConsumerOffsetsTopic)
	if err != nil {
		return err
	}

	for i := 0; i < tp.NumPartitions(); i++ {
		var offset int64 = 0
		for {
			data, err := tp.ReadFromPartition(i, offset)
			if err != nil {
				// Reached end of partition
				break
			}

			// Parse data: [KeyLen(4)][KeyBytes][ValueLen(4)][ValueBytes]
			if len(data) >= 4 {
				keyLen := int(binary.BigEndian.Uint32(data[0:4]))
				if len(data) >= 4+keyLen+4 {
					key := string(data[4 : 4+keyLen])
					valLen := int(binary.BigEndian.Uint32(data[4+keyLen : 8+keyLen]))
					if len(data) >= 8+keyLen+valLen {
						val := string(data[8+keyLen : 8+keyLen+valLen])

						// Apply to memory
						// Key format: groupID:topic:partition
						parts := strings.Split(key, ":")
						if len(parts) == 3 {
							groupID := parts[0]
							topicName := parts[1]
							partition, _ := strconv.Atoi(parts[2])
							offsetVal, _ := strconv.ParseInt(val, 10, 64)

							b.applyOffsetRecovery(groupID, topicName, partition, offsetVal)
						}
					}
				}
			}

			offset++
		}
	}

	slog.Info("recovered consumer offsets", "topic", ConsumerOffsetsTopic)
	return nil
}

func (b *Broker) applyOffsetRecovery(groupID, topicName string, partition int, offset int64) {
	tp, err := b.getOrCreateTopic(topicName)
	if err != nil {
		return
	}
	cg := tp.GetOrCreateConsumerGroup(groupID)
	cg.SetRecoveredOffset(partition, offset)
}

func (b *Broker) commitConsumerOffset(groupID, topicName string, partition int, offset int64) error {
	// Apply to memory
	tp, err := b.getOrCreateTopic(topicName)
	if err != nil {
		return err
	}
	cg := tp.GetOrCreateConsumerGroup(groupID)
	cg.CommitPartitionOffset(partition, offset)

	// Persist to __consumer_offsets
	internalTp, err := b.getOrCreateTopic(ConsumerOffsetsTopic)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s:%d", groupID, topicName, partition)
	val := fmt.Sprintf("%d", offset)

	data := make([]byte, 8+len(key)+len(val))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(key)))
	copy(data[4:], key)
	binary.BigEndian.PutUint32(data[4+len(key):8+len(key)], uint32(len(val)))
	copy(data[8+len(key):], val)

	_, _, err = internalTp.Append([]byte(key), data) // Using key-based routing for consumer offsets
	return err
}
