package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"gokafk/internal/broker"
	"gokafk/internal/consumer"
	"gokafk/internal/message"
	"gokafk/internal/producer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gokafk <server|producer|consumer>")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		runServer()
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runServer() {
	b := &broker.Broker{}
	slog.Info("Listening on port", "port", message.BrokerPort)
	if err := b.StartBrokerServer(); err != nil {
		slog.Error("Error", "error", err)
		os.Exit(1)
	}
}

func runProducer() {
	slog.Info("Trying to start producer processes")
	port, err := strconv.ParseInt(os.Args[2], 10, 16)
	if err != nil {
		panic(err)
	}
	topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
	if err != nil {
		panic(err)
	}
	producer := producer.Producer{
		Port:    uint16(port),
		TopicID: uint16(topicID),
	}
	producer.StartProducerServer()
}

func runConsumer() {
	port, err := strconv.ParseInt(os.Args[2], 10, 16)
	if err != nil {
		panic(err)
	}
	topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
	if err != nil {
		panic(err)
	}
	groupID, err := strconv.ParseInt(os.Args[4], 10, 16)
	if err != nil {
		panic(err)
	}
	consumer.Start(uint16(port), uint16(topicID), uint16(groupID))
}
