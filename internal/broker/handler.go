package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"gokafk/pkg/kafkaprotocol"
)

func (b *Broker) routeMessage(ctx context.Context, header *kafkaprotocol.RequestHeader, data []byte, conn net.Conn) ([]byte, error) {
	switch header.ApiKey {
	case kafkaprotocol.ApiKeyProduce: // 0
		return b.handleProduce(header.CorrelationId, data)

	case kafkaprotocol.ApiKeyFetch: // 1
		return b.handleFetch(header.CorrelationId, data)

	case kafkaprotocol.ApiKeyListOffsets: // 2
		// Return 0 for now
		return kafkaprotocol.HandleOffsetFetch(header.CorrelationId, "test-topic"), nil

	case kafkaprotocol.ApiKeyMetadata: // 3
		return kafkaprotocol.HandleMetadata(header.CorrelationId, data), nil

	case kafkaprotocol.ApiKeyOffsetFetch: // 9
		return kafkaprotocol.HandleOffsetFetch(header.CorrelationId, "test-topic"), nil

	case kafkaprotocol.ApiKeyFindCoordinator: // 10
		return kafkaprotocol.HandleFindCoordinator(header.CorrelationId), nil

	case kafkaprotocol.ApiKeyJoinGroup: // 11
		return kafkaprotocol.HandleJoinGroup(header.CorrelationId), nil

	case kafkaprotocol.ApiKeyHeartbeat: // 12
		// Heartbeat: just echo corrId with no error
		enc := kafkaprotocol.NewEncoder()
		enc.WriteInt32(header.CorrelationId)
		enc.WriteInt32(0) // throttle_time_ms
		enc.WriteInt16(0) // error_code
		return enc.Bytes(), nil

	case kafkaprotocol.ApiKeySyncGroup: // 14
		// hardcoded topic for tutorial
		return kafkaprotocol.HandleSyncGroup(header.CorrelationId, "test-topic"), nil

	case kafkaprotocol.ApiKeyApiVersions: // 18
		return kafkaprotocol.HandleApiVersions(header.CorrelationId), nil

	default:
		slog.Warn("unsupported api key", "key", header.ApiKey)
		// Return nil — don't crash the connection, just ignore
		return nil, nil
	}
}

func (b *Broker) handleProduce(corrId int32, data []byte) ([]byte, error) {
	slog.Debug("handleProduce", "bytes", len(data), "raw", data[:min(len(data), 40)])
	records, err := kafkaprotocol.ParseProduceRequest(data)
	if err != nil {
		slog.Error("parse produce failed", "err", err)
		// Don't crash connection; return error response
		return kafkaprotocol.HandleProduceResponse(corrId, "unknown", 0, -1), nil
	}

	if len(records) == 0 {
		return kafkaprotocol.HandleProduceResponse(corrId, "test-topic", 0, 0), nil
	}

	rec := records[0]
	tp, err := b.getOrCreateTopic(rec.Topic)
	if err != nil {
		slog.Error("get topic failed", "topic", rec.Topic, "err", err)
		return kafkaprotocol.HandleProduceResponse(corrId, rec.Topic, 0, -1), nil
	}

	_, offset, err := tp.Append(rec.Key, rec.Value)
	if err != nil {
		slog.Error("append failed", "err", err)
		return kafkaprotocol.HandleProduceResponse(corrId, rec.Topic, 0, -1), nil
	}
	slog.Info("produced", "topic", rec.Topic, "offset", offset, "val", string(rec.Value))

	return kafkaprotocol.HandleProduceResponse(corrId, rec.Topic, rec.Partition, offset), nil
}

func (b *Broker) handleFetch(corrId int32, data []byte) ([]byte, error) {
	fetchReqs, err := kafkaprotocol.ParseFetchRequest(data)
	if err != nil {
		return nil, fmt.Errorf("parse fetch: %w", err)
	}

	if len(fetchReqs) == 0 {
		return kafkaprotocol.HandleFetchResponse(corrId, "test-topic", 0, nil, 0), nil
	}

	req := fetchReqs[0]
	tp, err := b.getOrCreateTopic(req.Topic)
	if err != nil {
		// Topic doesn't exist yet
		return kafkaprotocol.HandleFetchResponse(corrId, req.Topic, req.Partition, nil, 0), nil
	}

	// read one message for simplification, starting from offset
	msgData, err := tp.ReadFromPartition(int(req.Partition), req.Offset)
	var msgs [][]byte
	if err == nil {
		msgs = append(msgs, msgData)
	}

	return kafkaprotocol.HandleFetchResponse(corrId, req.Topic, req.Partition, msgs, req.Offset), nil
}
