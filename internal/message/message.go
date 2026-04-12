package message

import (
	"bufio"
	"fmt"
)

const BrokerPort = 10000

const (
	ECHO  = 1
	P_REG = 2
	C_REQ = 3
	PCM   = 4

	// responses
	R_ECHO  = 101
	R_P_REG = 102
	R_C_REQ = 103
	R_PCM   = 104
)

type Message struct {
	ECHO  *string
	P_REG *ProducerRegisterMessage
	PCM   []byte //*ProducerConsumerMessage

	// responses
	R_ECHO  *string
	R_P_REG *byte // port
	R_PCM   *byte
}

type ProducerRegisterMessage struct {
	Port    uint16
	TopicID uint16
}

type ConsumerRegisterMessage struct {
	Port    uint16
	GroupID uint16
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

// Message Format:
func ReadFromStream(rw *bufio.ReadWriter) ([]byte, error) {
	header, err := rw.ReadByte()
	if err != nil {
		return nil, err
	}

	data, err := rw.Peek(int(header))
	if err != nil {
		return nil, err
	}

	_, err = rw.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func ReadMessageFromStream(rw *bufio.ReadWriter) (*Message, error) {
	data, err := ReadFromStream(rw)
	if err != nil {
		return nil, err
	}
	return ParseMessage(data), nil
}

func writeToStreamWithType(rw *bufio.ReadWriter, msgType byte, data string) error {
	// Write length
	err := rw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Write type
	err = rw.WriteByte(msgType)
	if err != nil {
		return err
	}
	// Write data
	_, err = rw.WriteString(data)
	if err != nil {
		return err
	}
	return rw.Flush()
}

func WriteMessageToStream(rw *bufio.ReadWriter, msg Message) error {
	if msg.ECHO != nil {
		return writeToStreamWithType(rw, ECHO, *msg.ECHO)
	}
	if msg.R_ECHO != nil {
		return writeToStreamWithType(rw, R_ECHO, *msg.R_ECHO)
	}
	if msg.P_REG != nil {
		return writeToStreamWithType(rw, P_REG, string(msg.P_REG.ToByte()))
	}
	if msg.R_P_REG != nil {
		return writeToStreamWithType(rw, R_P_REG, string(*msg.R_P_REG))
	}
	if msg.PCM != nil {
		if err := writeToStreamWithType(rw, PCM, string(msg.PCM)); err != nil {
			return err
		}
	}
	if msg.R_PCM != nil {
		data := fmt.Sprintf("%d", *msg.R_PCM)
		if err := writeToStreamWithType(rw, R_PCM, data); err != nil {
			return err
		}
	}
	return nil
}

func WriteEchoToStream(rw *bufio.ReadWriter, data string) error {
	err := rw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	err = rw.WriteByte(ECHO)
	if err != nil {
		return err
	}
	_, err = rw.WriteString(data)
	if err != nil {
		return err
	}
	return rw.Flush()
}

func ParseMessage(stream_msg []byte) *Message {
	if len(stream_msg) == 0 {
		return nil
	}
	switch stream_msg[0] {
	case ECHO:
		s := string(stream_msg[1:])
		return &Message{ECHO: &s}
	case P_REG:
		p := ProducerRegisterMessage{}
		p.FromByte(stream_msg[1:])
		return &Message{P_REG: &p}
	case R_ECHO:
		s := string(stream_msg[1:])
		return &Message{R_ECHO: &s}
	case R_P_REG:
		if len(stream_msg) < 2 {
			return nil
		}
		p := stream_msg[1]
		return &Message{R_P_REG: &p}
	case PCM:
		data := make([]byte, len(stream_msg[1:]))
		copy(data, stream_msg[1:])
		return &Message{PCM: data}
	case R_PCM:
		if len(stream_msg) < 2 {
			return nil
		}
		p := stream_msg[1]
		return &Message{R_PCM: &p}
	default:
		return nil
	}
}
