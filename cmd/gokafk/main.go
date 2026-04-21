package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gokafk/internal/broker"
	"gokafk/internal/config"
)

func main() {
	// Enable debug logging so we can trace protocol bytes
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	if len(os.Args) >= 2 && os.Args[1] != "server" {
		fmt.Fprintf(os.Stderr, "unknown command: %s. usage: gokafk [server]\n", os.Args[1])
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.DefaultConfig()

	if err := runServer(ctx, cfg); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func runServer(ctx context.Context, cfg *config.Config) error {
	b := broker.NewBroker(cfg)
	return b.Start(ctx)
}
