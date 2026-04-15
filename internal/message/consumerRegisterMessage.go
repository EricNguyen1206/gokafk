package message

type ConsumerRegisterMessage struct {
	Port    uint16
	GroupID uint16
	TopicID uint16
}

func (m *ConsumerRegisterMessage) FromByte(streamMessage []byte) {
	// First 2 bytes: port
	// Next 2 bytes: groupID
	// Last 2 bytes: topicID
	m.Port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.GroupID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
	m.TopicID = uint16(streamMessage[4])<<8 + uint16(streamMessage[5])
}

func (m *ConsumerRegisterMessage) ToByte() []byte {
	var data [6]byte
	// First 2 bytes: port
	data[0] = byte(m.Port >> 8)
	data[1] = byte(m.Port % 256)

	// Next 2 bytes: groupID
	data[2] = byte(m.GroupID >> 8)
	data[3] = byte(m.GroupID % 256)

	// Last 2 bytes: topicID
	data[4] = byte(m.TopicID >> 8)
	data[5] = byte(m.TopicID % 256)
	return data[0:6]
}
