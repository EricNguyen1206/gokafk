package consumer

import (
	"context"
	"log/slog"

	"gokafk/internal/config"
	"gokafk/pkg/client"
)

// Start connects to Broker and continuously pulls messages from Topic
func Start(ctx context.Context, cfg *config.Config, port uint16, topic, group string) error {
	cli := client.NewClient(cfg)
	c := cli.Consumer(port, group)
	if err := c.Connect(ctx, topic); err != nil {
		return err
	}
	defer c.Close()

	slog.Info("Consumer registered successfully", "topic", topic, "group", group)

	return c.Run(ctx, topic, func(data []byte) {
		slog.Info("Consumed message", "data", string(data))
	})
}
