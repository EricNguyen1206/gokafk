package protocol

import (
	"encoding/binary"
	"errors"
)

type ProduceMessage struct {
	TopicID uint16
	Key     []byte // nil or empty = no key (round-robin)
	Value   []byte
}

func (m *ProduceMessage) Marshal() []byte {
	keyLen := uint16(len(m.Key))
	buf := make([]byte, 4+keyLen+uint16(len(m.Value)))
	binary.BigEndian.PutUint16(buf[0:2], m.TopicID)
	binary.BigEndian.PutUint16(buf[2:4], keyLen)

	if keyLen > 0 {
		copy(buf[4:4+keyLen], m.Key)
	}
	copy(buf[4+keyLen:], m.Value)

	return buf
}

func (m *ProduceMessage) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("produce message data too short")
	}
	m.TopicID = binary.BigEndian.Uint16(data[0:2])
	keyLen := binary.BigEndian.Uint16(data[2:4])

	if len(data) < 4+int(keyLen) {
		return errors.New("produce message key truncated")
	}

	if keyLen > 0 {
		m.Key = make([]byte, keyLen)
		copy(m.Key, data[4:4+keyLen])
	} else {
		m.Key = nil
	}

	m.Value = make([]byte, len(data)-(4+int(keyLen)))
	copy(m.Value, data[4+keyLen:])

	return nil
}
