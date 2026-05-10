package config

import (
	"os"
	"strconv"
)

const (
	DefaultBrokerPort    = 10000
	DefaultDataDir       = "./data/logs"
	DefaultNumPartitions = 1
)

type Config struct {
	BrokerPort    int
	DataDir       string
	NumPartitions int
}

func LoadConfig() *Config {
	cfg := &Config{
		BrokerPort:    DefaultBrokerPort,
		DataDir:       DefaultDataDir,
		NumPartitions: DefaultNumPartitions,
	}

	// Override config with environment variables if present
	if port := os.Getenv("GOKAFK_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.BrokerPort = p
		}
	}
	if dir := os.Getenv("GOKAFK_DATA_DIR"); dir != "" {
		cfg.DataDir = dir
	}
	if parts := os.Getenv("GOKAFK_PARTITIONS"); parts != "" {
		if p, err := strconv.Atoi(parts); err == nil {
			cfg.NumPartitions = p
		}
	}

	return cfg
}
