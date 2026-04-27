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

	var recovered int
	for i := 0; i < tp.NumPartitions(); i++ {
		for offset := int64(0); ; offset++ {
			data, err := tp.ReadFromPartition(i, offset)
			if err != nil {
				break // end of partition
			}

			key, val, err := parseOffsetRecord(data)
			if err != nil {
				slog.Warn("skip malformed offset record", "partition", i, "offset", offset, "err", err)
				continue
			}

			groupID, topicName, partition, offsetVal, err := parseOffsetKey(key, val)
			if err != nil {
				slog.Warn("skip invalid offset key", "key", key, "err", err)
				continue
			}

			b.applyOffsetRecovery(groupID, topicName, partition, offsetVal)
			recovered++
		}
	}

	slog.Info("recovered consumer offsets", "topic", ConsumerOffsetsTopic, "count", recovered)
	return nil
}

// parseOffsetRecord decodes the binary format: [KeyLen(4)][KeyBytes][ValLen(4)][ValBytes]
func parseOffsetRecord(data []byte) (key, val string, err error) {
	if len(data) < 4 {
		return "", "", fmt.Errorf("data too short for key length: %d bytes", len(data))
	}

	keyLen := int(binary.BigEndian.Uint32(data[0:4]))
	keyEnd := 4 + keyLen

	if len(data) < keyEnd+4 {
		return "", "", fmt.Errorf("data too short for value length: need %d, have %d", keyEnd+4, len(data))
	}

	key = string(data[4:keyEnd])
	valLen := int(binary.BigEndian.Uint32(data[keyEnd : keyEnd+4]))
	valEnd := keyEnd + 4 + valLen

	if len(data) < valEnd {
		return "", "", fmt.Errorf("data too short for value: need %d, have %d", valEnd, len(data))
	}

	val = string(data[keyEnd+4 : valEnd])
	return key, val, nil
}

// parseOffsetKey splits "groupID:topic:partition" key and parses the offset value.
func parseOffsetKey(key, val string) (groupID, topicName string, partition int, offset int64, err error) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 {
		return "", "", 0, 0, fmt.Errorf("expected 3 parts, got %d", len(parts))
	}

	partition, err = strconv.Atoi(parts[2])
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("invalid partition %q: %w", parts[2], err)
	}

	offset, err = strconv.ParseInt(val, 10, 64)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("invalid offset %q: %w", val, err)
	}

	return parts[0], parts[1], partition, offset, nil
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
