package consumer

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"time"

	"gokafk/internal/message"
)

// Consumer Group
type CGroup struct {
	GroupID       uint16
	CurrentOffset int
	Consumers     []ConsumerConnection
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

// Start connect to Broker and continuously pull messages from Topic
func Start(port uint16, topicID uint16, groupID uint16) error {
	// 1. CONNECT TCP TO BROKER (same as Producer)
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", message.BrokerPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// 2. SEND C_REG: Register Consumer to Broker
	regMsg := message.ConsumerRegisterMessage{
		Port:    port,
		GroupID: groupID,
		TopicID: topicID,
	}
	err = message.WriteMessageToStream(streamRW, message.Message{C_REG: &regMsg})
	if err != nil {
		return fmt.Errorf("consumer register failed: %w", err)
	}

	// 3. READ REGISTER RESPONSE FROM BROKER
	resp, err := message.ReadMessageFromStream(streamRW)
	if err != nil {
		return fmt.Errorf("read register response failed: %w", err)
	}
	if resp == nil || resp.R_C_REG == nil {
		return fmt.Errorf("unexpected register response from broker")
	}
	slog.Info("Consumer registered successfully", "topicID", topicID, "groupID", groupID)

	// 4. PULL LOOP: Continously send C_REG to get next message
	for {
		// Send pull request (use C_REG again, Broker will know this consumer has registered)
		err = message.WriteMessageToStream(streamRW, message.Message{C_REG: &regMsg})
		if err != nil {
			return fmt.Errorf("pull request failed: %w", err)
		}

		// READ PULL RESPONSE
		pullResp, err := message.ReadMessageFromStream(streamRW)
		if err != nil {
			slog.Info("No more messages, waiting...", "offset", regMsg.TopicID)
			time.Sleep(1 * time.Second)
			continue
		}

		// Print message to screen
		if pullResp != nil && pullResp.R_C_REG != nil {
			slog.Info("Consumed message", "data", string(pullResp.R_C_REG))
		}
	}
}
