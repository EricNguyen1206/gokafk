package protocol

import (
	"encoding/binary"
	"errors"
)

// ProducerRegisterMessage is sent by a producer to register with the broker.
type ProducerRegisterMessage struct {
	Port    uint16
	TopicID uint16
}

// Marshal serializes the message to 4 bytes based
func (m *ProducerRegisterMessage) Marshal() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:2], m.Port)
	binary.BigEndian.PutUint16(buf[2:4], m.TopicID)
	return buf
}

// Unmarshal deserializes the message from 4 bytes
func (m *ProducerRegisterMessage) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("data too short")
	}
	m.Port = binary.BigEndian.Uint16(data[0:2])
	m.TopicID = binary.BigEndian.Uint16(data[2:4])
	return nil
}
