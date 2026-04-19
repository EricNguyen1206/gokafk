package config

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
	return &Config{
		BrokerPort:    DefaultBrokerPort,
		DataDir:       DefaultDataDir,
		NumPartitions: DefaultNumPartitions,
	}
}
