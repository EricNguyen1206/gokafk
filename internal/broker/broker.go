package broker

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"

	"gokafk/internal/message"
)

type Broker struct {
	topics []Topic
}

func (b *Broker) init() {
	b.topics = make([]Topic, 0)
}

func (b *Broker) StartBrokerServer() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", message.BrokerPort))
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Error accepting connection", "error", err)
			continue
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		// read message
		rawMsg, err := message.ReadFromStream(rw)
		if err != nil {
			break
		}

		// process message
		if rawMsg != nil {
			msg := message.ParseMessage(rawMsg)
			if msg != nil {
				resp, err := b.processBrokerMessage(msg, conn, rw)
				if err != nil {
					break
				}
				if resp != nil {
					message.WriteMessageToStream(rw, *resp)
				}
			}
		}

	}
}

/*
*

	Process message:
	- Call inner message handler base on type
	- Return correct Message

*
*/
func (b *Broker) processBrokerMessage(msg *message.Message, conn net.Conn, rw *bufio.ReadWriter) (*message.Message, error) {
	if msg.ECHO != nil {
		resp, err := b.processEchoMessage(msg.ECHO)
		if err != nil {
			return nil, err
		}
		return &message.Message{R_ECHO: &resp}, nil
	}
	if msg.P_REG != nil {
		resp, err := b.processProducerRegisterMessage(*msg.P_REG, conn, rw)
		if err != nil {
			return nil, err
		}
		return &message.Message{R_P_REG: resp}, nil
	}
	if msg.PCM != nil {
		resp, err := b.processProducerPCM(msg.PCM, conn)
		if err != nil {
			return nil, err
		}
		return &message.Message{R_PCM: &resp}, nil
	}
	return nil, nil
}

func (b *Broker) processProducerPCM(pcm []byte, conn net.Conn) (byte, error) {
	// Find which topic this producer belongs to
	for i, tp := range b.topics {
		for _, pc := range tp.producers {
			if pc.Conn == conn {
				b.topics[i].store.Append(pcm)
				// b.topics[i].mq.debug()
				return 0, nil
			}
		}
	}
	return 1, fmt.Errorf("producer not registered to any topic")
}

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) processProducerRegisterMessage(pRegMessage message.ProducerRegisterMessage, conn net.Conn, rw *bufio.ReadWriter) (*byte, error) {
	var topicIdx int = -1
	for idx, tp := range b.topics {
		if tp.topicID == pRegMessage.TopicID {
			topicIdx = idx
			break
		}
	}
	if topicIdx == -1 {
		tp := Topic{}
		tp.init(pRegMessage.TopicID)
		b.topics = append(b.topics, tp)
		topicIdx = len(b.topics) - 1
	}

	// Save the current connection into the topic's producer list.
	// The handleConnection loop will continue reading PCM messages
	// from this same connection — no need for a separate goroutine.
	b.topics[topicIdx].producers = append(b.topics[topicIdx].producers, ProducerConnection{
		Conn: conn,
		RW:   rw,
	})

	slog.Info("Producer registered to topic", "topicID", pRegMessage.TopicID)
	slog.Info("Total producers", "count", len(b.topics[topicIdx].producers))

	var resp byte = 0
	return &resp, nil
}

func (b *Broker) startConsumerGroupConsumtion(cRegMessage message.ConsumerRegisterMessage) (*byte, error) {
	return nil, nil
}
