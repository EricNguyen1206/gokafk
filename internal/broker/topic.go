package broker

import (
	"gokafk/internal/consumer"
	"gokafk/internal/storage"
)

type Topic struct {
	topicID   uint16
	producers []ProducerConnection
	consumers map[uint16]*consumer.CGroup
	store     *storage.Segment
}

func (t *Topic) init(topicID uint16) {
	t.topicID = topicID
	t.producers = make([]ProducerConnection, 0)
	t.consumers = make(map[uint16]*consumer.CGroup)
	t.store, _ = storage.NewSegment("./data/logs", topicID)
}
