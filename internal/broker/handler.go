package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"gokafk/pkg/kafkaprotocol"
)

func (b *Broker) routeMessage(ctx context.Context, header *kafkaprotocol.RequestHeader, data []byte, conn net.Conn) ([]byte, error) {
	switch header.APIKey {
	case kafkaprotocol.ApiKeyProduce: // 0
		return b.handleProduce(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyFetch: // 1
		return b.handleFetch(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyListOffsets: // 2
		return b.handleListOffsets(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyMetadata: // 3
		return kafkaprotocol.HandleMetadata(header.CorrelationID, data), nil

	case kafkaprotocol.ApiKeyOffsetCommit: // 8
		return b.handleOffsetCommit(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyOffsetFetch: // 9
		return b.handleOffsetFetch(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyFindCoordinator: // 10
		return kafkaprotocol.HandleFindCoordinator(header.CorrelationID), nil

	case kafkaprotocol.ApiKeyJoinGroup: // 11
		return b.handleJoinGroup(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyHeartbeat: // 12
		return kafkaprotocol.HandleHeartbeat(header.CorrelationID), nil

	case kafkaprotocol.ApiKeyLeaveGroup: // 13
		return b.handleLeaveGroup(header.CorrelationID, data)

	case kafkaprotocol.ApiKeySyncGroup: // 14
		return b.handleSyncGroup(header.CorrelationID, data)

	case kafkaprotocol.ApiKeyApiVersions: // 18
		return kafkaprotocol.HandleApiVersions(header.CorrelationID), nil

	default:
		slog.Warn("unsupported api key", "key", header.APIKey)
		// Return nil — don't crash the connection, just ignore
		return nil, nil
	}
}

func (b *Broker) handleProduce(correlationID int32, data []byte) ([]byte, error) {
	slog.Debug("handleProduce", "bytes", len(data), "raw", data[:min(len(data), 40)])
	records, err := kafkaprotocol.ParseProduceRequest(data)
	if err != nil {
		slog.Error("parse produce failed", "err", err)
		// Not crash connection, just return error response
		return kafkaprotocol.HandleProduceResponse(correlationID, "unknown", 0, -1), nil
	}

	if len(records) == 0 {
		return kafkaprotocol.HandleProduceResponse(correlationID, "test-topic", 0, 0), nil
	}
	// TODO: Support produce multiple records
	rec := records[0]
	tp, err := b.getOrCreateTopic(rec.Topic)
	if err != nil {
		slog.Error("get topic failed", "topic", rec.Topic, "err", err)
		return kafkaprotocol.HandleProduceResponse(correlationID, rec.Topic, 0, -1), nil
	}

	_, offset, err := tp.Append(rec.Key, rec.Value)
	if err != nil {
		slog.Error("append failed", "err", err)
		return kafkaprotocol.HandleProduceResponse(correlationID, rec.Topic, 0, -1), nil
	}
	slog.Info("produced", "topic", rec.Topic, "offset", offset, "val", string(rec.Value))

	return kafkaprotocol.HandleProduceResponse(correlationID, rec.Topic, rec.Partition, offset), nil
}

func (b *Broker) handleFetch(correlationID int32, data []byte) ([]byte, error) {
	fetchReqs, err := kafkaprotocol.ParseFetchRequest(data)
	if err != nil {
		return nil, fmt.Errorf("parse fetch: %w", err)
	}

	if len(fetchReqs) == 0 {
		return kafkaprotocol.HandleFetchResponse(correlationID, "test-topic", 0, nil, 0), nil
	}
	// TODO: Support fetch multiple records
	req := fetchReqs[0]
	tp, err := b.getOrCreateTopic(req.Topic)
	if err != nil {
		// Topic doesn't exist yet
		return kafkaprotocol.HandleFetchResponse(correlationID, req.Topic, req.Partition, nil, 0), nil
	}

	// read one message for simplification, starting from offset
	msgData, err := tp.ReadFromPartition(int(req.Partition), req.Offset)
	var msgs [][]byte
	if err == nil {
		msgs = append(msgs, msgData)
	}

	return kafkaprotocol.HandleFetchResponse(correlationID, req.Topic, req.Partition, msgs, req.Offset), nil
}

func (b *Broker) handleOffsetCommit(correlationID int32, data []byte) ([]byte, error) {
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

	return kafkaprotocol.HandleOffsetCommitResponse(correlationID), nil
}

func (b *Broker) handleOffsetFetch(correlationID int32, data []byte) ([]byte, error) {
	groupID, reqs, err := kafkaprotocol.ParseOffsetFetchRequest(data)
	if err != nil {
		slog.Error("parse offset fetch failed", "err", err)
		return nil, fmt.Errorf("parse offset fetch: %w", err)
	}

	var entries []kafkaprotocol.OffsetFetchResponseEntry
	for _, req := range reqs {
		var partEntries []kafkaprotocol.OffsetFetchPartitionEntry
		for _, partition := range req.Partitions {
			// Default: -1 = no committed offset (Kafka spec)
			var offset int64 = -1

			b.mu.RLock()
			tp, ok := b.topics[req.Topic]
			b.mu.RUnlock()

			if ok {
				cg := tp.GetOrCreateConsumerGroup(groupID)
				committed := cg.GetPartitionOffset(int(partition))
				if committed > 0 {
					offset = committed
				}
			}

			slog.Info("offset fetch", "group", groupID, "topic", req.Topic, "partition", partition, "offset", offset)
			partEntries = append(partEntries, kafkaprotocol.OffsetFetchPartitionEntry{
				Partition: partition,
				Offset:    offset,
			})
		}
		entries = append(entries, kafkaprotocol.OffsetFetchResponseEntry{
			Topic:      req.Topic,
			Partitions: partEntries,
		})
	}

	// If no topics were requested, return an empty response
	if len(entries) == 0 {
		entries = append(entries, kafkaprotocol.OffsetFetchResponseEntry{
			Topic: "test-topic",
			Partitions: []kafkaprotocol.OffsetFetchPartitionEntry{
				{Partition: 0, Offset: -1},
			},
		})
	}

	return kafkaprotocol.HandleOffsetFetchResponse(correlationID, entries), nil
}

func (b *Broker) handleListOffsets(correlationID int32, data []byte) ([]byte, error) {
	reqs, err := kafkaprotocol.ParseListOffsetsRequest(data)
	if err != nil {
		slog.Error("parse list offsets failed", "err", err)
		return nil, fmt.Errorf("parse list offsets: %w", err)
	}

	var entries []kafkaprotocol.ListOffsetsResponseEntry
	for _, req := range reqs {
		entry := kafkaprotocol.ListOffsetsResponseEntry{
			Topic:     req.Topic,
			Partition: req.Partition,
		}

		tp, tpErr := b.getOrCreateTopic(req.Topic)
		if tpErr != nil {
			// Topic not found — return error code 3 (UNKNOWN_TOPIC_OR_PARTITION)
			entry.ErrorCode = 3
			entry.Offset = -1
			entry.Timestamp = -1
			entries = append(entries, entry)
			continue
		}

		switch req.Timestamp {
		case -2: // Earliest
			entry.Offset = 0
			entry.Timestamp = -2
		case -1: // Latest
			entry.Offset = tp.PartitionOffset(int(req.Partition))
			entry.Timestamp = -1
		default: // Real timestamp query
			offset, findErr := tp.PartitionFindOffsetByTimestamp(int(req.Partition), req.Timestamp)
			if findErr != nil {
				entry.ErrorCode = 3
				entry.Offset = -1
				entry.Timestamp = -1
			} else if offset == -1 {
				// No message found at or after the requested timestamp
				entry.Offset = -1
				entry.Timestamp = -1
			} else {
				entry.Offset = offset
				// Return the actual timestamp of the found message
				ts, _ := tp.PartitionTimestampAt(int(req.Partition), offset)
				entry.Timestamp = ts
			}
		}

		slog.Info("list offsets", "topic", req.Topic, "partition", req.Partition, "timestamp", req.Timestamp, "offset", entry.Offset)
		entries = append(entries, entry)
	}

	return kafkaprotocol.HandleListOffsetsResponse(correlationID, entries), nil
}
