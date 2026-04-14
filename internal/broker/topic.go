package broker

import (
	"gokafk/internal/consumer"
	"gokafk/internal/storage"
)

type Topic struct {
	topicID   uint16
	producers []ProducerConnection
	consumers []consumer.CGroup
	store     *storage.Segment
}

func (t *Topic) init(topicID uint16) {
	t.topicID = topicID
	t.producers = make([]ProducerConnection, 0)
	t.consumers = make([]consumer.CGroup, 0)
	t.store, _ = storage.NewSegment("./data/logs", topicID)
}
