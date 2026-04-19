package broker

import (
	"fmt"
	"gokafk/internal/storage"
	"sync/atomic"
)

type Partition struct {
	id         int
	topicName  string
	store      storage.Store
	currOffset int64
}

func NewPartition(id int, topicName string, dataDir string) (*Partition, error) {
	dir := fmt.Sprintf("%s/%s/partition_%d", dataDir, topicName, id)
	store, err := storage.NewSegment(dir, uint16(id))
	if err != nil {
		return nil, fmt.Errorf("new partition %d: %w", id, err)
	}

	return &Partition{
		id:        id,
		topicName: topicName,
		store:     store,
	}, nil
}

func (p *Partition) ID() int {
	return p.id
}

func (p *Partition) Append(data []byte) (int64, error) {
	offset, err := p.store.Append(data)
	if err == nil {
		// Update highest offset 
		atomic.StoreInt64(&p.currOffset, offset+1)
	}
	return offset, err
}

func (p *Partition) Read(offset int64) ([]byte, error) {
	return p.store.Read(offset)
}

func (p *Partition) CurrentOffset() int64 {
	return atomic.LoadInt64(&p.currOffset)
}

func (p *Partition) Close() error {
	return p.store.Close()
}
