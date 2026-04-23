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
		return kafkaprotocol.HandleOffsetFetchResponse(header.CorrelationId, "test-topic", 0, 0), nil

	case kafkaprotocol.ApiKeyMetadata: // 3
		return kafkaprotocol.HandleMetadata(header.CorrelationId, data), nil

	case kafkaprotocol.ApiKeyOffsetCommit: // 8
		return b.handleOffsetCommit(header.CorrelationId, data)

	case kafkaprotocol.ApiKeyOffsetFetch: // 9
		return b.handleOffsetFetch(header.CorrelationId, data)

	case kafkaprotocol.ApiKeyFindCoordinator: // 10
		return kafkaprotocol.HandleFindCoordinator(header.CorrelationId), nil

	case kafkaprotocol.ApiKeyJoinGroup: // 11
		return kafkaprotocol.HandleJoinGroup(header.CorrelationId), nil

	case kafkaprotocol.ApiKeyHeartbeat: // 12
		// Heartbeat: just echo correlationId with no error
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

func (b *Broker) handleProduce(correlationId int32, data []byte) ([]byte, error) {
	slog.Debug("handleProduce", "bytes", len(data), "raw", data[:min(len(data), 40)])
	records, err := kafkaprotocol.ParseProduceRequest(data)
	if err != nil {
		slog.Error("parse produce failed", "err", err)
		// Not crash connection, just return error response
		return kafkaprotocol.HandleProduceResponse(correlationId, "unknown", 0, -1), nil
	}

	if len(records) == 0 {
		return kafkaprotocol.HandleProduceResponse(correlationId, "test-topic", 0, 0), nil
	}

	rec := records[0]
	tp, err := b.getOrCreateTopic(rec.Topic)
	if err != nil {
		slog.Error("get topic failed", "topic", rec.Topic, "err", err)
		return kafkaprotocol.HandleProduceResponse(correlationId, rec.Topic, 0, -1), nil
	}

	_, offset, err := tp.Append(rec.Key, rec.Value)
	if err != nil {
		slog.Error("append failed", "err", err)
		return kafkaprotocol.HandleProduceResponse(correlationId, rec.Topic, 0, -1), nil
	}
	slog.Info("produced", "topic", rec.Topic, "offset", offset, "val", string(rec.Value))

	return kafkaprotocol.HandleProduceResponse(correlationId, rec.Topic, rec.Partition, offset), nil
}

func (b *Broker) handleFetch(correlationId int32, data []byte) ([]byte, error) {
	fetchReqs, err := kafkaprotocol.ParseFetchRequest(data)
	if err != nil {
		return nil, fmt.Errorf("parse fetch: %w", err)
	}

	if len(fetchReqs) == 0 {
		return kafkaprotocol.HandleFetchResponse(correlationId, "test-topic", 0, nil, 0), nil
	}

	req := fetchReqs[0]
	tp, err := b.getOrCreateTopic(req.Topic)
	if err != nil {
		// Topic doesn't exist yet
		return kafkaprotocol.HandleFetchResponse(correlationId, req.Topic, req.Partition, nil, 0), nil
	}

	// read one message for simplification, starting from offset
	msgData, err := tp.ReadFromPartition(int(req.Partition), req.Offset)
	var msgs [][]byte
	if err == nil {
		msgs = append(msgs, msgData)
	}

	return kafkaprotocol.HandleFetchResponse(correlationId, req.Topic, req.Partition, msgs, req.Offset), nil
}

func (b *Broker) handleOffsetCommit(correlationId int32, data []byte) ([]byte, error) {
	groupID, reqs, err := kafkaprotocol.ParseOffsetCommitRequest(data)
	if err != nil {
		slog.Error("parse offset commit failed", "err", err)
		return nil, fmt.Errorf("parse offset commit: %w", err)
	}

	for _, req := range reqs {
		slog.Info("offset commit", "group", groupID, "topic", req.Topic, "partition", req.Partition, "offset", req.Offset)
		if err := b.commitConsumerOffset(groupID, req.Topic, int(req.Partition), req.Offset); err != nil {
			slog.Error("failed to commit offset", "err", err)
		}
	}

	return kafkaprotocol.HandleOffsetCommitResponse(correlationId), nil
}

func (b *Broker) handleOffsetFetch(correlationId int32, data []byte) ([]byte, error) {
	groupID, reqs, err := kafkaprotocol.ParseOffsetFetchRequest(data)
	if err != nil {
		slog.Error("parse offset fetch failed", "err", err)
		return nil, fmt.Errorf("parse offset fetch: %w", err)
	}

	if len(reqs) == 0 {
		return kafkaprotocol.HandleOffsetFetchResponse(correlationId, "test-topic", 0, 0), nil
	}

	req := reqs[0]
	topicName := req.Topic
	partition := int32(0)
	if len(req.Partitions) > 0 {
		partition = req.Partitions[0]
	}

	// Lookup offset in memory
	var offset int64 = 0
	b.mu.RLock()
	tp, ok := b.topics[topicName]
	b.mu.RUnlock()

	if ok {
		cg := tp.GetOrCreateConsumerGroup(groupID)
		offset = cg.GetPartitionOffset(int(partition))
	}

	slog.Info("offset fetch", "group", groupID, "topic", topicName, "partition", partition, "offset", offset)
	return kafkaprotocol.HandleOffsetFetchResponse(correlationId, topicName, partition, offset), nil
}
