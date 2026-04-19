package config

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Error("DefaultConfig() returned nil")
	}
	if cfg.BrokerPort != DefaultBrokerPort {
		t.Errorf("DefaultConfig() BrokerPort = %d; want %d", cfg.BrokerPort, DefaultBrokerPort)
	}
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("DefaultConfig() DataDir = %q; want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.NumPartitions != DefaultNumPartitions {
		t.Errorf("DefaultConfig() NumPartitions = %d; want %d", cfg.NumPartitions, DefaultNumPartitions)
	}
}
