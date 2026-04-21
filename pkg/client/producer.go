package client

import (
	"bufio"
	"context"
	"fmt"
	"net"

	"gokafk/internal/config"
	"gokafk/pkg/protocol"
)

type Producer struct {
	cfg   *config.Config
	port  uint16
	conn  net.Conn
	codec *protocol.Codec
}

type Message struct {
	Key   []byte
	Value []byte
}

func (p *Producer) Connect(ctx context.Context, topic string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", p.cfg.BrokerPort))
	if err != nil {
		return err
	}
	p.conn = conn
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	p.codec = protocol.NewCodec(rw)

	regMsg := protocol.ProducerRegisterMessage{
		Port:  p.port,
		Topic: topic,
	}

	err = p.codec.WriteMessage(ctx, &protocol.Message{
		Type:    protocol.TypePReg,
		CorrID:  1,
		Payload: regMsg.Marshal(),
	})
	if err != nil {
		return fmt.Errorf("producer register failed: %w", err)
	}

	resp, err := p.codec.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("read register response failed: %w", err)
	}
	if resp.Type != protocol.TypePRegResp {
		return fmt.Errorf("unexpected response from broker: %d", resp.Type)
	}

	return nil
}

func (p *Producer) Send(ctx context.Context, topic string, msg Message) error {
	prodMsg := protocol.ProduceMessage{
		Topic: topic,
		Key:   msg.Key,
		Value: msg.Value,
	}

	err := p.codec.WriteMessage(ctx, &protocol.Message{
		Type:    protocol.TypeProduce,
		CorrID:  2,
		Payload: prodMsg.Marshal(),
	})
	if err != nil {
		return err
	}

	resp, err := p.codec.ReadMessage(ctx)
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeProduceResp {
		return fmt.Errorf("unexpected produce response: %d", resp.Type)
	}
	return nil
}

func (p *Producer) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
