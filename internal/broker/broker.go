package broker

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"gokafk/internal/config"
	"gokafk/pkg/kafkaprotocol"
)

// Broker is the central TCP server that routes messages between producers and consumers.
type Broker struct {
	cfg       *config.Config
	mu        sync.RWMutex
	topics    map[string]*Topic // topicID → Topic
	producers map[net.Conn]string
	listener  net.Listener
	wg        sync.WaitGroup
}

// NewBroker creates a new broker with the given configuration.
func NewBroker(cfg *config.Config) *Broker {
	return &Broker{
		cfg:       cfg,
		topics:    make(map[string]*Topic),
		producers: make(map[net.Conn]string),
	}
}

// Start begins accepting TCP connections. Blocks until ctx is cancelled.
func (b *Broker) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", b.cfg.BrokerPort))
	if err != nil {
		return fmt.Errorf("broker listen: %w", err)
	}
	b.listener = ln

	// Shutdown goroutine
	go func() {
		<-ctx.Done()
		slog.Info("broker shutting down")
		ln.Close()
	}()

	slog.Info("broker started", "port", b.cfg.BrokerPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // context cancelled
			}
			slog.Error("accept error", "err", err)
			continue
		}

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.handleConnection(ctx, conn)
		}()
	}

	// Wait for all connections to drain
	b.wg.Wait()

	// Close all topics
	b.mu.RLock()
	for _, tp := range b.topics {
		tp.Close()
	}
	b.mu.RUnlock()

	slog.Info("broker stopped")
	return nil
}

func (b *Broker) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	defer b.cleanupConnection(conn)

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	codec := kafkaprotocol.NewCodec(rw)

	slog.Info("new connection", "remote", conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		header, data, err := codec.ReadRequest(ctx)
		if err != nil {
			// eof or read error
			return
		}

		resp, err := b.routeMessage(ctx, header, data, conn)
		if err != nil {
			slog.Error("message handling error", "err", err)
			return
		}

		if resp != nil {
			if err := codec.WriteResponse(resp); err != nil {
				slog.Error("response write error", "err", err)
				return
			}
		}
	}
}

func (b *Broker) getOrCreateTopic(topic string) (*Topic, error) {
	b.mu.RLock()
	tp, ok := b.topics[topic]
	b.mu.RUnlock()

	if ok {
		return tp, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double-check after acquiring write lock
	if tp, ok := b.topics[topic]; ok {
		return tp, nil
	}

	tp, err := NewTopic(topic, b.cfg.DataDir, b.cfg.NumPartitions)
	if err != nil {
		return nil, err
	}
	b.topics[topic] = tp
	return tp, nil
}

func (b *Broker) cleanupConnection(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.producers, conn)
	slog.Info("connection cleaned up", "remote", conn.RemoteAddr())
}
