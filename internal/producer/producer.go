package producer

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"gokafk/internal/message"
)

type Producer struct {
	Port    uint16
	TopicID uint16
}

func (p *Producer) StartProducerServer() error {
	// Connect to the Broker (not to self)
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", message.BrokerPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// Step 1: Register this producer with the broker
	if err := p.registerProducerToBroker(streamRW); err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	slog.Info("Producer registered successfully", "topicID", p.TopicID)

	// Step 2: Read from stdin and send PCM messages to broker
	rd := bufio.NewReader(os.Stdin)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		// Send PCM message to broker
		err = message.WriteMessageToStream(streamRW, message.Message{PCM: []byte(line)})
		if err != nil {
			return fmt.Errorf("send PCM failed: %w", err)
		}

		// Read response from broker
		resp, err := message.ReadMessageFromStream(streamRW)
		if err != nil {
			return fmt.Errorf("read response failed: %w", err)
		}
		if resp != nil && resp.R_PCM != nil {
			slog.Info("Response PCM", "message", *resp.R_PCM)
		}
	}

	return nil
}

func (p *Producer) registerProducerToBroker(rw *bufio.ReadWriter) error {
	regMsg := message.ProducerRegisterMessage{
		Port:    p.Port,
		TopicID: p.TopicID,
	}

	// Send P_REG message to the broker
	err := message.WriteMessageToStream(rw, message.Message{P_REG: &regMsg})
	if err != nil {
		return err
	}

	// Wait for R_P_REG response from broker
	resp, err := message.ReadMessageFromStream(rw)
	if err != nil {
		return err
	}
	if resp == nil || resp.R_P_REG == nil {
		return fmt.Errorf("unexpected response from broker")
	}

	return nil
}
