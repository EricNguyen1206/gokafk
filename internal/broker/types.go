package broker

import (
	"bufio"
	"net"
)

// ProducerConnection represents the broker's session with a producer
type ProducerConnection struct {
	Conn net.Conn
	RW   *bufio.ReadWriter
}
