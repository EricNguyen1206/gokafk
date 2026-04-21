package protocol

import (
	"encoding/binary"
	"errors"
)

// ProducerRegisterMessage is sent by a producer to register with the broker.
type ProducerRegisterMessage struct {
	Port  uint16
	Topic string
}

// Marshal serializes the message to 4 bytes based
func (m *ProducerRegisterMessage) Marshal() []byte {
	topicLen := uint16(len(m.Topic))

	// total = 2 (topicLen) + N (topic) + 2 (portLen) + M (port)
	bufSize := 2 + int(topicLen) + 2 + int(m.Port)
	buf := make([]byte, bufSize)

	// Write Topic
	binary.BigEndian.PutUint16(buf[0:2], topicLen)
	copy(buf[2:2+topicLen], m.Topic)

	// Write Port
	binary.BigEndian.PutUint16(buf[2+topicLen:4+topicLen], m.Port)
	return buf
}

// Unmarshal deserializes the message from 4 bytes
func (m *ProducerRegisterMessage) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("data too short")
	}
	m.Port = binary.BigEndian.Uint16(data[0:2])
	m.Topic = string(data[2:4])
	return nil
}
