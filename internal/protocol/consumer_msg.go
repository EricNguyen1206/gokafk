package protocol

import (
	"encoding/binary"
	"errors"
)

// ConsumerRegisterMessage is sent by a consumer to register with a consumer group.
type ConsumerRegisterMessage struct {
	Port    uint16
	GroupID uint16
	TopicID uint16
}

// Marshal serializes the message to 6 bytes based on big-endian encoding
func (m *ConsumerRegisterMessage) Marshal() []byte {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], m.Port)
	binary.BigEndian.PutUint16(buf[2:4], m.GroupID)
	binary.BigEndian.PutUint16(buf[4:6], m.TopicID)
	return buf
}

// Unmarshal deserializes the message from 6 bytes based on big-endian encoding
func (m *ConsumerRegisterMessage) Unmarshal(data []byte) error {
	if len(data) < 6 {
		return errors.New("data too short")
	}
	m.Port = binary.BigEndian.Uint16(data[0:2])
	m.GroupID = binary.BigEndian.Uint16(data[2:4])
	m.TopicID = binary.BigEndian.Uint16(data[4:6])
	return nil
}
