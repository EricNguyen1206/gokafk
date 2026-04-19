package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"gokafk/internal/broker"
	"gokafk/internal/config"
	"gokafk/internal/consumer"
	"gokafk/internal/producer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gokafk <server|producer|consumer>")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.DefaultConfig()

	switch os.Args[1] {
	case "server":
		if err := runServer(ctx, cfg); err != nil {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	case "producer":
		if err := runProducer(ctx, cfg); err != nil {
			slog.Error("producer error", "err", err)
			os.Exit(1)
		}
	case "consumer":
		if err := runConsumer(ctx, cfg); err != nil {
			slog.Error("consumer error", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runServer(ctx context.Context, cfg *config.Config) error {
	b := broker.NewBroker(cfg)
	return b.Start(ctx)
}

func runProducer(ctx context.Context, cfg *config.Config) error {
	if len(os.Args) < 4 {
		return fmt.Errorf("usage: gokafk producer <port> <topicID> [key]")
	}
	port, err := strconv.ParseUint(os.Args[2], 10, 16)
	if err != nil {
		return err
	}
	topicID, err := strconv.ParseUint(os.Args[3], 10, 16)
	if err != nil {
		return err
	}
	var key string
	if len(os.Args) >= 5 {
		key = os.Args[4]
	}
	slog.Info("Trying to start producer processes")
	return producer.Start(ctx, cfg, uint16(port), uint16(topicID), key)
}

func runConsumer(ctx context.Context, cfg *config.Config) error {
	if len(os.Args) < 5 {
		return fmt.Errorf("usage: gokafk consumer <port> <topicID> <groupID>")
	}
	port, err := strconv.ParseUint(os.Args[2], 10, 16)
	if err != nil {
		return err
	}
	topicID, err := strconv.ParseUint(os.Args[3], 10, 16)
	if err != nil {
		return err
	}
	groupID, err := strconv.ParseUint(os.Args[4], 10, 16)
	if err != nil {
		return err
	}
	slog.Info("Trying to start consumer processes")
	return consumer.Start(ctx, cfg, uint16(port), uint16(topicID), uint16(groupID))
}
