package consumer

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"gokafk/internal/config"
	"gokafk/internal/protocol"
)

// Start connects to Broker and continuously pulls messages from Topic
func Start(ctx context.Context, cfg *config.Config, port, topicID, groupID uint16) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", cfg.BrokerPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	codec := protocol.NewCodec(rw)

	// SEND C_REG: Register Consumer to Broker
	regMsg := protocol.ConsumerRegisterMessage{
		Port:    port,
		GroupID: groupID,
		TopicID: topicID,
	}
	err = codec.WriteMessage(ctx, &protocol.Message{
		Type:    protocol.TypeCReg,
		CorrID:  1,
		Payload: regMsg.Marshal(),
	})
	if err != nil {
		return fmt.Errorf("consumer register failed: %w", err)
	}

	// READ REGISTER RESPONSE FROM BROKER
	resp, err := codec.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("read register response failed: %w", err)
	}
	if resp == nil || resp.Type != protocol.TypeCRegResp {
		return fmt.Errorf("unexpected register response from broker")
	}
	memberID := string(resp.Payload)
	slog.Info("Consumer registered successfully", "topicID", topicID, "groupID", groupID, "memberID", memberID)

	var corrID uint32 = 2

	// PULL LOOP
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchReq := (&protocol.FetchRequest{
			TopicID:  topicID,
			GroupID:  groupID,
			MemberID: memberID,
		}).Marshal()

		err = codec.WriteMessage(ctx, &protocol.Message{
			Type:    protocol.TypeFetch,
			CorrID:  corrID,
			Payload: fetchReq,
		})
		if err != nil {
			return fmt.Errorf("pull request failed: %w", err)
		}
		corrID++

		// READ PULL RESPONSE
		pullResp, err := codec.ReadMessage(ctx)
		if err != nil {
			slog.Debug("pull read error", "err", err)
			return err
		}

		var fetchResp protocol.FetchResponse
		if err := fetchResp.Unmarshal(pullResp.Payload); err == nil {
			if fetchResp.PartitionID >= 0 && len(fetchResp.Data) > 0 {
				slog.Info("consumed",
					"partition", fetchResp.PartitionID,
					"offset", fetchResp.Offset,
					"data", string(fetchResp.Data),
				)
			} else {
				// No messages
				time.Sleep(1 * time.Second)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}
