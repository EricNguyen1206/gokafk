package client

import (
	"gokafk/internal/config"
)

// Client provides high-level APIs to interact with Gokafk cluster.
type Client struct {
	cfg *config.Config
}

// NewClient creates a new Gokafk client.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		cfg: cfg,
	}
}

// Producer creates a new producer instance.
func (c *Client) Producer(port uint16) *Producer {
	return &Producer{
		cfg:  c.cfg,
		port: port,
	}
}

// Consumer creates a new consumer instance for a given consumer group.
func (c *Client) Consumer(port uint16, group string) *Consumer {
	return &Consumer{
		cfg:   c.cfg,
		port:  port,
		group: group,
	}
}
