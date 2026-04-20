package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	"gokafk/internal/config"
	"gokafk/internal/protocol"
)

type Consumer struct {
	cfg      *config.Config
	port     uint16
	group    string
	conn     net.Conn
	codec    *protocol.Codec
	memberID string
}

func (c *Consumer) Connect(ctx context.Context, topic string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", c.cfg.BrokerPort))
	if err != nil {
		return err
	}
	c.conn = conn
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	c.codec = protocol.NewCodec(rw)

	regMsg := protocol.ConsumerRegisterMessage{
		Port:  c.port,
		Group: c.group,
		Topic: topic,
	}

	err = c.codec.WriteMessage(ctx, &protocol.Message{
		Type:    protocol.TypeCReg,
		CorrID:  1,
		Payload: regMsg.Marshal(),
	})
	if err != nil {
		return fmt.Errorf("consumer register failed: %w", err)
	}

	resp, err := c.codec.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("read register response failed: %w", err)
	}
	if resp.Type != protocol.TypeCRegResp {
		return fmt.Errorf("unexpected register response from broker: %d", resp.Type)
	}

	c.memberID = string(resp.Payload)
	return nil
}

func (c *Consumer) Run(ctx context.Context, topic string, handler func(msg []byte)) error {
	var corrID uint32 = 2
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchReq := (&protocol.FetchRequest{
			Topic:    topic,
			Group:    c.group,
			MemberID: c.memberID,
		}).Marshal()

		err := c.codec.WriteMessage(ctx, &protocol.Message{
			Type:    protocol.TypeFetch,
			CorrID:  corrID,
			Payload: fetchReq,
		})
		if err != nil {
			return fmt.Errorf("pull request failed: %w", err)
		}
		corrID++

		pullResp, err := c.codec.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var fetchResp protocol.FetchResponse
		if err := fetchResp.Unmarshal(pullResp.Payload); err == nil {
			if fetchResp.PartitionID >= 0 && len(fetchResp.Data) > 0 {
				handler(fetchResp.Data)
			} else {
				time.Sleep(1 * time.Second)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Consumer) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
