package broker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"gokafk/internal/config"
	"gokafk/internal/protocol"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func startTestBroker(t *testing.T, ctx context.Context) *config.Config {
	t.Helper()
	cfg := &config.Config{
		BrokerPort:    freePort(t),
		DataDir:       t.TempDir(),
		NumPartitions: 3,
	}
	b := NewBroker(cfg)
	go func() {
		if err := b.Start(ctx); err != nil {
			t.Logf("broker stopped: %v", err)
		}
	}()
	// Wait for broker to start
	time.Sleep(100 * time.Millisecond)
	return cfg
}

func dialBroker(t *testing.T, cfg *config.Config) *protocol.Codec {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", cfg.BrokerPort))
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return protocol.NewCodec(rw)
}

func TestIntegration_ProduceAndFetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := startTestBroker(t, ctx)

	// --- Producer ---
	prodCodec := dialBroker(t, cfg)

	// Register producer
	regPayload := (&protocol.ProducerRegisterMessage{Port: 8000, Topic: "topic-1"}).Marshal()
	err := prodCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypePReg, CorrID: 1, Payload: regPayload})
	if err != nil {
		t.Fatalf("producer register: %v", err)
	}
	resp, err := prodCodec.ReadMessage(ctx)
	if err != nil || resp.Type != protocol.TypePRegResp {
		t.Fatalf("producer register resp: err=%v type=%d", err, resp.Type)
	}

	// Send messages with keys
	messages := []struct{ key, value string }{
		{"user-1", "msg-1"},
		{"user-2", "msg-2"},
		{"user-1", "msg-3"}, // same key as first — same partition
	}
	for i, m := range messages {
		prodMsg := protocol.ProduceMessage{Topic: "topic-1", Key: []byte(m.key), Value: []byte(m.value)}
		err := prodCodec.WriteMessage(ctx, &protocol.Message{
			Type: protocol.TypeProduce, CorrID: uint32(i + 2), Payload: prodMsg.Marshal(),
		})
		if err != nil {
			t.Fatalf("produce[%d]: %v", i, err)
		}
		prodResp, err := prodCodec.ReadMessage(ctx)
		if err != nil || prodResp.Type != protocol.TypeProduceResp {
			t.Fatalf("produce resp[%d]: err=%v type=%d", i, err, prodResp.Type)
		}
	}

	// --- Consumer ---
	consCodec := dialBroker(t, cfg)

	// Register consumer
	cRegPayload := (&protocol.ConsumerRegisterMessage{Port: 0, Group: "group-1", Topic: "topic-1"}).Marshal()
	err = consCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypeCReg, CorrID: 100, Payload: cRegPayload})
	if err != nil {
		t.Fatalf("consumer register: %v", err)
	}
	cResp, err := consCodec.ReadMessage(ctx)
	if err != nil || cResp.Type != protocol.TypeCRegResp {
		t.Fatalf("consumer register resp: err=%v type=%d", err, cResp.Type)
	}
	memberID := string(cResp.Payload)

	// Fetch messages
	fetched := 0
	for i := 0; i < 10; i++ {
		fetchReq := (&protocol.FetchRequest{Topic: "topic-1", Group: "group-1", MemberID: memberID}).Marshal()
		err := consCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypeFetch, CorrID: uint32(200 + i), Payload: fetchReq})
		if err != nil {
			t.Fatalf("fetch[%d]: %v", i, err)
		}
		fetchResp, err := consCodec.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("fetch resp[%d]: %v", i, err)
		}

		var fr protocol.FetchResponse
		if err := fr.Unmarshal(fetchResp.Payload); err != nil {
			t.Fatalf("parse fetch resp[%d]: %v", i, err)
		}
		if fr.PartitionID >= 0 && len(fr.Data) > 0 {
			fetched++
			t.Logf("fetched: partition=%d offset=%d data=%q", fr.PartitionID, fr.Offset, fr.Data)
		}
	}

	if fetched != 3 {
		t.Errorf("want 3 fetched messages, got %d", fetched)
	}
}

func TestIntegration_SameKeyGoesToSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := startTestBroker(t, ctx)
	prodCodec := dialBroker(t, cfg)

	// Register
	regPayload := (&protocol.ProducerRegisterMessage{Port: 8000, Topic: "topic-1"}).Marshal()
	prodCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypePReg, CorrID: 1, Payload: regPayload})
	prodCodec.ReadMessage(ctx) // consume response

	// Send 10 messages with same key
	key := "deterministic-key"
	for i := 0; i < 10; i++ {
		prodMsg := protocol.ProduceMessage{
			Topic: "topic-1",
			Key:     []byte(key),
			Value:   []byte(fmt.Sprintf("msg-%d", i)),
		}
		prodCodec.WriteMessage(ctx, &protocol.Message{
			Type: protocol.TypeProduce, CorrID: uint32(i + 2), Payload: prodMsg.Marshal(),
		})
		prodCodec.ReadMessage(ctx) // consume response
	}

	// All 10 should be in same partition → consumer gets them in order
	consCodec := dialBroker(t, cfg)
	cRegPayload := (&protocol.ConsumerRegisterMessage{Port: 0, Group: "group-1", Topic: "topic-1"}).Marshal()
	consCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypeCReg, CorrID: 100, Payload: cRegPayload})
	cResp, _ := consCodec.ReadMessage(ctx)
	memberID := string(cResp.Payload)

	var prevPartition int32 = -1
	for i := 0; i < 10; i++ {
		fetchReq := (&protocol.FetchRequest{Topic: "topic-1", Group: "group-1", MemberID: memberID}).Marshal()
		consCodec.WriteMessage(ctx, &protocol.Message{Type: protocol.TypeFetch, CorrID: uint32(200 + i), Payload: fetchReq})
		fetchResp, _ := consCodec.ReadMessage(ctx)

		var fr protocol.FetchResponse
		fr.Unmarshal(fetchResp.Payload)

		if fr.PartitionID < 0 {
			continue
		}

		if prevPartition >= 0 && fr.PartitionID != prevPartition {
			t.Errorf("same key routed to different partitions: %d and %d", prevPartition, fr.PartitionID)
		}
		prevPartition = fr.PartitionID
	}
}
