package protocol

import (
	"encoding/binary"
	"errors"
)

// ConsumerRegisterMessage is sent by a consumer to register with a consumer group.
type ConsumerRegisterMessage struct {
	Port  uint16
	Group string
	Topic string
}

// Marshal serializes the message to 6 bytes based on big-endian encoding
func (m *ConsumerRegisterMessage) Marshal() []byte {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], m.Port)
	copy(buf[2:4], m.Group)
	copy(buf[4:6], m.Topic)
	return buf
}

// Unmarshal deserializes the message from 6 bytes based on big-endian encoding
func (m *ConsumerRegisterMessage) Unmarshal(data []byte) error {
	if len(data) < 6 {
		return errors.New("data too short")
	}
	m.Port = binary.BigEndian.Uint16(data[0:2])
	m.Group = string(data[2:4])
	m.Topic = string(data[4:6])
	return nil
}
