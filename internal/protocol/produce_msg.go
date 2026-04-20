package protocol

import (
	"encoding/binary"
	"errors"
)

type ProduceMessage struct {
	Topic string
	Key   []byte // nil or empty = no key (round-robin)
	Value []byte
}

// Marshal serializes the message to bytes
func (m *ProduceMessage) Marshal() []byte {
	topicLen := uint16(len(m.Topic))
	keyLen := uint16(len(m.Key))

	// total = 2 (topicLen) + N (topic) + 2 (keyLen) + M (key) + K (value)
	bufSize := 2 + int(topicLen) + 2 + int(keyLen) + len(m.Value)
	buf := make([]byte, bufSize)

	// Write Topic
	binary.BigEndian.PutUint16(buf[0:2], topicLen)
	copy(buf[2:2+topicLen], m.Topic)

	// Write Key
	binary.BigEndian.PutUint16(buf[2+topicLen:4+topicLen], keyLen)
	if keyLen > 0 {
		copy(buf[4+topicLen:4+topicLen+keyLen], m.Key)
	}

	// Write Value
	copy(buf[4+topicLen+keyLen:], m.Value)

	return buf
}

func (m *ProduceMessage) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("produce message data too short")
	}

	// Read Topic
	topicLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(topicLen)+2 {
		return errors.New("data too short for topic and key length")
	}
	m.Topic = string(data[2 : 2+topicLen])

	// Read Key
	offset := 2 + int(topicLen)
	keyLen := binary.BigEndian.Uint16(data[offset : offset+2])

	if len(data) < offset+2+int(keyLen) {
		return errors.New("data too short for key content")
	}

	if keyLen > 0 {
		m.Key = make([]byte, keyLen)
		copy(m.Key, data[offset+2:offset+2+int(keyLen)])
	} else {
		m.Key = nil
	}

	// Read Value
	valueOffset := offset + 2 + int(keyLen)
	m.Value = make([]byte, len(data)-valueOffset)
	copy(m.Value, data[valueOffset:])

	return nil
}
