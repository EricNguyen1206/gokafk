package config

// TODO [Phase 1 - Task 1]: Centralized configuration package
// See: ~/docs/plans/2026-04-18-phase1-foundation-refactor.md — Task 1
//
// Implement:
// - DefaultBrokerPort    = 10000
// - DefaultDataDir       = "./data/logs"
// - DefaultNumPartitions = 3
// - Config struct with BrokerPort, DataDir, NumPartitions
// - DefaultConfig() constructor
// - Write tests in config_test.go

const (
	DefaultBrokerPort    = 10000
	DefaultDataDir       = "./data/logs"
	DefaultNumPartitions = 3
)

type Config struct {
	BrokerPort    int
	DataDir       string
	NumPartitions int
}

func DefaultConfig() *Config {
	// TODO: implement
	return &Config{
		BrokerPort:    DefaultBrokerPort,
		DataDir:       DefaultDataDir,
		NumPartitions: DefaultNumPartitions,
	}
}
