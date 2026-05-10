package config

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	cfg := LoadConfig()
	if cfg == nil {
		t.Error("LoadConfig() returned nil")
	}
	if cfg.BrokerPort != DefaultBrokerPort {
		t.Errorf("LoadConfig() BrokerPort = %d; want %d", cfg.BrokerPort, DefaultBrokerPort)
	}
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("LoadConfig() DataDir = %q; want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.NumPartitions != DefaultNumPartitions {
		t.Errorf("LoadConfig() NumPartitions = %d; want %d", cfg.NumPartitions, DefaultNumPartitions)
	}
}
