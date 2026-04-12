package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

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
		fmt.Println("Trying to start producer processes")
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
	case "consumer":
		runConsumer()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runServer() {
	b := &broker.Broker{}
	fmt.Printf("Listening on port %d\n", message.BrokerPort)
	if err := b.StartBrokerServer(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runConsumer() {
	c, err := consumer.NewConsumerConnection(fmt.Sprintf(":%d", message.BrokerPort))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	fmt.Printf("Connected to server at port %d\n", message.BrokerPort)
	rd := bufio.NewReader(os.Stdin)

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			break
		}

		msg := strings.TrimRight(line, "\n")
		fmt.Printf("Sent to server: %s\n", msg)

		if err := c.Send(msg); err != nil {
			fmt.Fprintf(os.Stderr, "Send error: %v\n", err)
			break
		}

		resp, err := c.Receive()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Receive error: %v\n", err)
			break
		}
		fmt.Printf("Receive message from server: %s\n", resp)
	}
}
