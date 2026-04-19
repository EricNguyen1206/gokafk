package broker

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"gokafk/internal/config"
	"gokafk/internal/protocol"
)

// Broker is the central TCP server that routes messages between producers and consumers.
type Broker struct {
	cfg      *config.Config
	mu       sync.RWMutex
	topics   map[uint16]*Topic  // topicID → Topic
	producers map[net.Conn]uint16
	listener  net.Listener
	wg        sync.WaitGroup
}

// NewBroker creates a new broker with the given configuration.
func NewBroker(cfg *config.Config) *Broker {
	return &Broker{
		cfg:       cfg,
		topics:    make(map[uint16]*Topic),
		producers: make(map[net.Conn]uint16),
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
	codec := protocol.NewCodec(rw)

	slog.Info("new connection", "remote", conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := codec.ReadMessage(ctx)
		if err != nil {
			slog.Debug("connection read error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		resp, err := b.routeMessage(ctx, msg, conn)
		if err != nil {
			slog.Error("message handling error", "type", msg.Type, "err", err)
			return
		}

		if resp != nil {
			if err := codec.WriteMessage(ctx, resp); err != nil {
				slog.Error("response write error", "err", err)
				return
			}
		}
	}
}

func (b *Broker) getOrCreateTopic(topicID uint16) (*Topic, error) {
	b.mu.RLock()
	tp, ok := b.topics[topicID]
	b.mu.RUnlock()

	if ok {
		return tp, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double-check after acquiring write lock
	if tp, ok := b.topics[topicID]; ok {
		return tp, nil
	}

	tp, err := NewTopic(topicID, b.cfg.DataDir, b.cfg.NumPartitions)
	if err != nil {
		return nil, err
	}
	b.topics[topicID] = tp
	return tp, nil
}

func (b *Broker) cleanupConnection(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.producers, conn)
	slog.Info("connection cleaned up", "remote", conn.RemoteAddr())
}
