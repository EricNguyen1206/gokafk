package message

type ProducerRegisterMessage struct {
	Port    uint16
	TopicID uint16
}

func (m *ProducerRegisterMessage) FromByte(streamMessage []byte) {
	// First 2 bytes: port
	// Next 2 bytes: topicID
	m.Port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.TopicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
}

func (m *ProducerRegisterMessage) ToByte() []byte {
	var data [4]byte
	// First 2 bytes: port
	// Next 2 bytes: topicID

	data[0] = byte(m.Port >> 8)
	data[1] = byte(m.Port % 256)

	data[2] = byte(m.TopicID >> 8)
	data[3] = byte(m.TopicID % 256)
	return data[0:4]
}
