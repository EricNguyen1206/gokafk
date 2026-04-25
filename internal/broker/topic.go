package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"gokafk/internal/storage"
)

// Topic manages partitions and consumer groups for a single topic.
type Topic struct {
	mu         sync.RWMutex
	topic      string
	partitions []*Partition
	numParts   int
	rrCounter  uint64 // atomic counter for round-robin
	consumers  map[string]*ConsumerGroup
}

// NewTopic creates a new topic with the specified number of partitions.
func NewTopic(topicName string, dataDir string, numPartitions int) (*Topic, error) {
	partitions := make([]*Partition, numPartitions)

	for i := 0; i < numPartitions; i++ {
		p, err := NewPartition(i, topicName, dataDir)
		if err != nil {
			// Cleanup already-created partitions
			for j := 0; j < i; j++ {
				partitions[j].Close()
			}
			return nil, fmt.Errorf("new topic %s partition %d: %w", topicName, i, err)
		}
		partitions[i] = p
	}

	return &Topic{
		topic:      topicName,
		partitions: partitions,
		numParts:   numPartitions,
		consumers:  make(map[string]*ConsumerGroup),
	}, nil
}

// PartitionFor returns the partition index for the given key.
// If key is nil, uses round-robin. Otherwise uses FNV-32a hash.
func (t *Topic) PartitionFor(key []byte) int {
	if key == nil || len(key) == 0 {
		idx := atomic.AddUint64(&t.rrCounter, 1)
		return int(idx % uint64(t.numParts))
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(t.numParts))
}

// AppendToPartition writes data to a specific partition.
func (t *Topic) AppendToPartition(partitionID int, data []byte) (int64, error) {
	if partitionID < 0 || partitionID >= t.numParts {
		return -1, fmt.Errorf("partition %d out of range [0, %d)", partitionID, t.numParts)
	}
	return t.partitions[partitionID].Append(data)
}

// ReadFromPartition reads data from a specific partition at the given offset.
func (t *Topic) ReadFromPartition(partitionID int, offset int64) ([]byte, error) {
	if partitionID < 0 || partitionID >= t.numParts {
		return nil, fmt.Errorf("partition %d out of range [0, %d)", partitionID, t.numParts)
	}
	return t.partitions[partitionID].Read(offset)
}

// Append writes data using key-based routing. Convenience method.
func (t *Topic) Append(key, value []byte) (partitionID int, offset int64, err error) {
	partitionID = t.PartitionFor(key)
	offset, err = t.AppendToPartition(partitionID, value)
	return
}

// GetOrCreateConsumerGroup returns existing or creates new consumer group.
func (t *Topic) GetOrCreateConsumerGroup(groupName string) *ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	cg, ok := t.consumers[groupName]
	if !ok {
		cg = NewConsumerGroup(groupName)
		t.consumers[groupName] = cg
	}
	return cg
}

// NumPartitions returns the number of partitions.
func (t *Topic) NumPartitions() int {
	return t.numParts
}

// PartitionOffset returns the current offset for a partition.
// Returns -1 if partition doesn't exist.
func (t *Topic) PartitionOffset(partitionID int) int64 {
	if partitionID < 0 || partitionID >= t.numParts {
		return -1
	}
	return t.partitions[partitionID].CurrentOffset()
}

// PartitionTimestampAt returns the timestamp of the message at the given offset in a partition.
func (t *Topic) PartitionTimestampAt(partitionID int, offset int64) (int64, error) {
	if partitionID < 0 || partitionID >= t.numParts {
		return 0, fmt.Errorf("partition %d out of range [0, %d)", partitionID, t.numParts)
	}
	return t.partitions[partitionID].TimestampAt(offset)
}

// PartitionFindOffsetByTimestamp returns the first offset whose timestamp >= ts in a partition.
func (t *Topic) PartitionFindOffsetByTimestamp(partitionID int, ts int64) (int64, error) {
	if partitionID < 0 || partitionID >= t.numParts {
		return -1, fmt.Errorf("partition %d out of range [0, %d)", partitionID, t.numParts)
	}
	return t.partitions[partitionID].FindOffsetByTimestamp(ts)
}

// Close closes all partitions.
func (t *Topic) Close() error {
	var firstErr error
	for _, p := range t.partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// compile-time check: Segment implements Store
var _ storage.Store = (*storage.Segment)(nil)
