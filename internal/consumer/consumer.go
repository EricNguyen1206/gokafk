package consumer

import (
	"bufio"
	"fmt"
	"net"

	"gokafk/internal/message"
)

type CGroup struct {
	GroupID   uint16
	Offset    uint
	Consumers []ConsumerConnection
}

type ConsumerConnection struct {
	Conn net.Conn
	RW   *bufio.ReadWriter
}

func NewConsumerConnection(addr string) (*ConsumerConnection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return &ConsumerConnection{Conn: conn, RW: rw}, nil
}

func (c *ConsumerConnection) Send(msg string) error {
	return message.WriteEchoToStream(c.RW, msg)
}

func (c *ConsumerConnection) Receive() (string, error) {
	header, err := c.RW.ReadByte()
	if err != nil {
		return "", err
	}
	if header == 0 {
		return "", fmt.Errorf("empty response")
	}
	data, err := c.RW.Peek(int(header))
	if err != nil {
		return "", err
	}
	_, err = c.RW.Discard(int(header))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *ConsumerConnection) Close() error {
	return c.Conn.Close()
}

type Consumer struct {
	Port int16
}

func (c *Consumer) startConsumerServer() error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", c.Port))
	if err != nil {
		return err
	}
	defer conn.Close()

	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		// Read message from broker
		pcm, err := message.ReadMessageFromStream(streamRW)
		if err != nil {
			break
		}
		fmt.Printf("Received PCM: %s\n", string(pcm.PCM))

		// Send response back
		err = message.WriteMessageToStream(streamRW, message.Message{R_PCM: pcm.R_PCM})
		if err != nil {
			break
		}
	}

	return nil
}
