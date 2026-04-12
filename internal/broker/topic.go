package broker

import (
	"gokafk/internal/consumer"
)

type Topic struct {
	topicID   uint16
	producers []ProducerConnection
	consumers []consumer.CGroup
	mq        Queue
}

func (t *Topic) init(topicID uint16) {
	t.topicID = topicID
	t.producers = make([]ProducerConnection, 0)
	t.consumers = make([]consumer.CGroup, 0)
	t.mq = Queue{}
}
