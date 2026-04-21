package producer

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"

	"gokafk/internal/config"
	"gokafk/pkg/client"
)

func Start(ctx context.Context, cfg *config.Config, port uint16, topic, key string) error {
	cli := client.NewClient(cfg)
	p := cli.Producer(port)
	if err := p.Connect(ctx, topic); err != nil {
		return err
	}
	defer p.Close()

	slog.Info("Producer registered successfully", "topic", topic)

	rd := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Print("> ")
		line, err := rd.ReadString('\n')
		if err != nil {
			return err
		}
		// Strip newline
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}

		var k []byte
		if key != "" {
			k = []byte(key)
		}

		err = p.Send(ctx, topic, client.Message{
			Key:   k,
			Value: []byte(line),
		})
		if err != nil {
			slog.Error("send failed", "err", err)
		} else {
			slog.Info("Produce OK")
		}
	}
}
