package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"gokafk/pkg/protocol"
)

func (b *Broker) routeMessage(ctx context.Context, msg *protocol.Message, conn net.Conn) (*protocol.Message, error) {
	switch msg.Type {
	case protocol.TypeEcho:
		return b.handleEcho(msg)
	case protocol.TypePReg:
		return b.handleProducerRegister(msg, conn)
	case protocol.TypeCReg:
		return b.handleConsumerRegister(msg, conn)
	case protocol.TypeProduce:
		return b.handleProduce(msg, conn)
	case protocol.TypeFetch:
		return b.handleFetch(msg, conn)
	default:
		return nil, fmt.Errorf("unknown message type: %d", msg.Type)
	}
}

func (b *Broker) handleEcho(msg *protocol.Message) (*protocol.Message, error) {
	resp := fmt.Sprintf("echo: %s", string(msg.Payload))
	return &protocol.Message{
		Type:    protocol.TypeEchoResp,
		CorrID:  msg.CorrID,
		Payload: []byte(resp),
	}, nil
}

func (b *Broker) handleProducerRegister(msg *protocol.Message, conn net.Conn) (*protocol.Message, error) {
	var regMsg protocol.ProducerRegisterMessage
	if err := regMsg.Unmarshal(msg.Payload); err != nil {
		return nil, fmt.Errorf("parse producer register: %w", err)
	}

	if _, err := b.getOrCreateTopic(regMsg.Topic); err != nil {
		return nil, fmt.Errorf("create topic %s: %w", regMsg.Topic, err)
	}

	b.mu.Lock()
	b.producers[conn] = regMsg.Topic
	b.mu.Unlock()

	slog.Info("producer registered", "topic", regMsg.Topic, "remote", conn.RemoteAddr())

	return &protocol.Message{
		Type:    protocol.TypePRegResp,
		CorrID:  msg.CorrID,
		Payload: []byte{0},
	}, nil
}

func (b *Broker) handleConsumerRegister(msg *protocol.Message, conn net.Conn) (*protocol.Message, error) {
	var regMsg protocol.ConsumerRegisterMessage
	if err := regMsg.Unmarshal(msg.Payload); err != nil {
		return nil, fmt.Errorf("parse consumer register: %w", err)
	}

	tp, err := b.getOrCreateTopic(regMsg.Topic)
	if err != nil {
		return nil, fmt.Errorf("create topic %s: %w", regMsg.Topic, err)
	}

	cg := tp.GetOrCreateConsumerGroup(regMsg.Group)
	memberID := cg.AddMember(conn)

	// Trigger rebalance
	cg.Rebalance(tp.NumPartitions())

	slog.Info("consumer registered + rebalanced",
		"topic", regMsg.Topic,
		"group", regMsg.Group,
		"memberID", memberID,
		"members", cg.MemberCount(),
		"assignments", cg.GetAssignments(memberID),
	)

	return &protocol.Message{
		Type:    protocol.TypeCRegResp,
		CorrID:  msg.CorrID,
		Payload: []byte(memberID),
	}, nil
}

func (b *Broker) handleProduce(msg *protocol.Message, conn net.Conn) (*protocol.Message, error) {
	var prodMsg protocol.ProduceMessage
	if err := prodMsg.Unmarshal(msg.Payload); err != nil {
		return nil, fmt.Errorf("parse produce: %w", err)
	}

	tp, err := b.getOrCreateTopic(prodMsg.Topic)
	if err != nil {
		return nil, fmt.Errorf("get topic %s: %w", prodMsg.Topic, err)
	}

	partID, offset, err := tp.Append(prodMsg.Key, prodMsg.Value)
	if err != nil {
		return nil, fmt.Errorf("produce to topic %s: %w", prodMsg.Topic, err)
	}

	slog.Debug("produced", "topic", prodMsg.Topic, "partition", partID, "offset", offset)

	return &protocol.Message{
		Type:    protocol.TypeProduceResp,
		CorrID:  msg.CorrID,
		Payload: []byte{0},
	}, nil
}

func (b *Broker) handleFetch(msg *protocol.Message, conn net.Conn) (*protocol.Message, error) {
	var fetchReq protocol.FetchRequest
	if err := fetchReq.Unmarshal(msg.Payload); err != nil {
		return nil, fmt.Errorf("parse fetch: %w", err)
	}

	b.mu.RLock()
	tp, ok := b.topics[fetchReq.Topic]
	b.mu.RUnlock()

	if !ok {
		slog.Error("topic not found in fetch", "topic", fetchReq.Topic)
		resp := protocol.FetchResponse{PartitionID: -1, Offset: -1}
		return &protocol.Message{
			Type:    protocol.TypeFetchResp,
			CorrID:  msg.CorrID,
			Payload: resp.Marshal(),
		}, nil
	}

	cg := tp.GetOrCreateConsumerGroup(fetchReq.Group)
	assignments := cg.GetAssignments(fetchReq.MemberID)

	if len(assignments) == 0 {
		slog.Error("no assignments for member", "member", fetchReq.MemberID, "group", fetchReq.Group)
		resp := protocol.FetchResponse{PartitionID: -1, Offset: -1}
		return &protocol.Message{
			Type:    protocol.TypeFetchResp,
			CorrID:  msg.CorrID,
			Payload: resp.Marshal(),
		}, nil
	}

	for _, partID := range assignments {
		offset := cg.GetPartitionOffset(partID)
		data, err := tp.ReadFromPartition(partID, offset)
		if err != nil {
			slog.Error("read error", "part", partID, "off", offset, "err", err)
			continue
		}

		// read success, advance offset
		cg.GetAndAdvancePartitionOffset(partID)

		resp := protocol.FetchResponse{
			PartitionID: int32(partID),
			Offset:      offset,
			Data:        data,
		}
		return &protocol.Message{
			Type:    protocol.TypeFetchResp,
			CorrID:  msg.CorrID,
			Payload: resp.Marshal(),
		}, nil
	}

	resp := protocol.FetchResponse{PartitionID: -1, Offset: -1}
	return &protocol.Message{
		Type:    protocol.TypeFetchResp,
		CorrID:  msg.CorrID,
		Payload: resp.Marshal(),
	}, nil
}
