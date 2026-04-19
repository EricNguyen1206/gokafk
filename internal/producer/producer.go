package producer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"gokafk/internal/config"
	"gokafk/internal/protocol"
)

func Start(ctx context.Context, cfg *config.Config, port, topicID uint16, key string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", cfg.BrokerPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	codec := protocol.NewCodec(rw)

	// Step 1: Register this producer with the broker
	regMsg := protocol.ProducerRegisterMessage{
		Port:    port,
		TopicID: topicID,
	}

	err = codec.WriteMessage(ctx, &protocol.Message{
		Type:    protocol.TypePReg,
		CorrID:  1,
		Payload: regMsg.Marshal(),
	})
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	// Wait for response
	resp, err := codec.ReadMessage(ctx)
	if err != nil || resp.Type != protocol.TypePRegResp {
		return fmt.Errorf("unexpected response from broker: %v", err)
	}
	slog.Info("Producer registered successfully", "topicID", topicID)

	// Step 2: Read from stdin and send Produce messages
	rd := bufio.NewReader(os.Stdin)
	var corrID uint32 = 2

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		// Send ProduceMessage
		prodMsg := protocol.ProduceMessage{
			TopicID: topicID,
			Value:   []byte(line),
		}
		if key != "" {
			prodMsg.Key = []byte(key)
		}

		err = codec.WriteMessage(ctx, &protocol.Message{
			Type:    protocol.TypeProduce,
			CorrID:  corrID,
			Payload: prodMsg.Marshal(),
		})
		if err != nil {
			return fmt.Errorf("send produce failed: %w", err)
		}

		prodResp, err := codec.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read response failed: %w", err)
		}
		if prodResp != nil && prodResp.Type == protocol.TypeProduceResp {
			slog.Debug("Message produced successfully")
		}
		corrID++
	}

	return nil
}
