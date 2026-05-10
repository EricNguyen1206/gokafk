package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"gokafk/pkg/proto"
)

func (b *Broker) routeMessage(ctx context.Context, header *proto.RequestHeader, data []byte, conn net.Conn) ([]byte, error) {
	switch header.APIKey {
	case proto.ApiKeyProduce: // 0
		return b.handleProduce(header.CorrelationID, data)

	case proto.ApiKeyFetch: // 1
		return b.handleFetch(header.CorrelationID, data)

	case proto.ApiKeyListOffsets: // 2
		return b.handleListOffsets(header.CorrelationID, data)

	case proto.ApiKeyMetadata: // 3
		return proto.HandleMetadata(header.CorrelationID, data), nil

	case proto.ApiKeyOffsetCommit: // 8
		return b.handleOffsetCommit(header.CorrelationID, data)

	case proto.ApiKeyOffsetFetch: // 9
		return b.handleOffsetFetch(header.CorrelationID, data)

	case proto.ApiKeyFindCoordinator: // 10
		return proto.HandleFindCoordinator(header.CorrelationID), nil

	case proto.ApiKeyJoinGroup: // 11
		return b.handleJoinGroup(header.CorrelationID, data)

	case proto.ApiKeyHeartbeat: // 12
		return proto.HandleHeartbeat(header.CorrelationID), nil

	case proto.ApiKeyLeaveGroup: // 13
		return b.handleLeaveGroup(header.CorrelationID, data)

	case proto.ApiKeySyncGroup: // 14
		return b.handleSyncGroup(header.CorrelationID, data)

	case proto.ApiKeyApiVersions: // 18
		return proto.HandleApiVersions(header.CorrelationID), nil

	default:
		slog.Warn("unsupported api key", "key", header.APIKey)
		// Return nil — don't crash the connection, just ignore
		return nil, nil
	}
}

func (b *Broker) handleProduce(correlationID int32, data []byte) ([]byte, error) {
	records, err := proto.ParseProduceRequest(data)
	if err != nil {
		slog.Error("parse produce failed", "err", err)
		return proto.HandleProduceResponse(correlationID, "unknown", 0, -1), nil
	}

	if len(records) == 0 {
		return proto.HandleProduceResponse(correlationID, "test-topic", 0, 0), nil
	}

	var baseOffset int64 = -1
	var lastTopic string
	var lastPartition int32

	for i, rec := range records {
		tp, err := b.getOrCreateTopic(rec.Topic)
		if err != nil {
			slog.Error("get topic failed", "topic", rec.Topic, "err", err)
			continue
		}

		_, offset, err := tp.Append(rec.Key, rec.Value)
		if err != nil {
			slog.Error("append failed", "err", err)
			continue
		}

		if i == 0 {
			baseOffset = offset
			lastTopic = rec.Topic
			lastPartition = rec.Partition
		}
		ts, _ := tp.PartitionTimestampAt(int(rec.Partition), offset)
		slog.Info("produced", "topic", rec.Topic, "offset", offset, "ts", ts, "val", string(rec.Value))
	}

	return proto.HandleProduceResponse(correlationID, lastTopic, lastPartition, baseOffset), nil
}

func (b *Broker) handleFetch(correlationID int32, data []byte) ([]byte, error) {
	fetchReqs, err := proto.ParseFetchRequest(data)
	if err != nil {
		return nil, fmt.Errorf("parse fetch: %w", err)
	}

	if len(fetchReqs) == 0 {
		return proto.HandleFetchResponse(correlationID, "test-topic", 0, nil, 0), nil
	}
	// TODO: Support fetch multiple records
	req := fetchReqs[0]
	tp, err := b.getOrCreateTopic(req.Topic)
	if err != nil {
		// Topic doesn't exist yet
		return proto.HandleFetchResponse(correlationID, req.Topic, req.Partition, nil, 0), nil
	}

	// read one message for simplification, starting from offset
	msgData, err := tp.ReadFromPartition(int(req.Partition), req.Offset)
	var msgs [][]byte
	if err == nil {
		msgs = append(msgs, msgData)
	}

	return proto.HandleFetchResponse(correlationID, req.Topic, req.Partition, msgs, req.Offset), nil
}

func (b *Broker) handleOffsetCommit(correlationID int32, data []byte) ([]byte, error) {
	groupID, reqs, err := proto.ParseOffsetCommitRequest(data)
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

	return proto.HandleOffsetCommitResponse(correlationID, reqs), nil
}

func (b *Broker) handleOffsetFetch(correlationID int32, data []byte) ([]byte, error) {
	groupID, reqs, err := proto.ParseOffsetFetchRequest(data)
	if err != nil {
		slog.Error("parse offset fetch failed", "err", err)
		return nil, fmt.Errorf("parse offset fetch: %w", err)
	}

	var entries []proto.OffsetFetchResponseEntry
	for _, req := range reqs {
		var partEntries []proto.OffsetFetchPartitionEntry
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
			partEntries = append(partEntries, proto.OffsetFetchPartitionEntry{
				Partition: partition,
				Offset:    offset,
			})
		}
		entries = append(entries, proto.OffsetFetchResponseEntry{
			Topic:      req.Topic,
			Partitions: partEntries,
		})
	}

	// If no topics were requested, return an empty response
	if len(entries) == 0 {
		entries = append(entries, proto.OffsetFetchResponseEntry{
			Topic: "test-topic",
			Partitions: []proto.OffsetFetchPartitionEntry{
				{Partition: 0, Offset: -1},
			},
		})
	}

	return proto.HandleOffsetFetchResponse(correlationID, entries), nil
}

func (b *Broker) handleListOffsets(correlationID int32, data []byte) ([]byte, error) {
	reqs, err := proto.ParseListOffsetsRequest(data)
	if err != nil {
		slog.Error("parse list offsets failed", "err", err)
		return nil, fmt.Errorf("parse list offsets: %w", err)
	}

	var entries []proto.ListOffsetsResponseEntry
	for _, req := range reqs {
		entry := proto.ListOffsetsResponseEntry{
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

		slog.Info("list offsets", "topic", req.Topic, "partition", req.Partition, "reqTimestamp", req.Timestamp, "foundOffset", entry.Offset, "foundTimestamp", entry.Timestamp)
		entries = append(entries, entry)
	}

	return proto.HandleListOffsetsResponse(correlationID, entries), nil
}
