package broker

import (
	"net"
	"sort"
	"sync"
	"time"
)

type ConsumerGroup struct {
	mu          sync.RWMutex
	group       string
	members     map[string]*GroupMember // memberID -> member
	assignments map[string][]int        // memberID -> partition IDs
	offsets     map[int]int64           // partitionID -> committed offset
}

type GroupMember struct {
	memberID string
	conn     net.Conn
	joinedAt time.Time
}

func NewConsumerGroup(groupName string) *ConsumerGroup {
	return &ConsumerGroup{
		group:       groupName,
		members:     make(map[string]*GroupMember),
		assignments: make(map[string][]int),
		offsets:     make(map[int]int64),
	}
}

func (cg *ConsumerGroup) AddMember(conn net.Conn) string {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	memberId := conn.RemoteAddr().String()
	cg.members[memberId] = &GroupMember{
		memberID: memberId,
		conn:     conn,
		joinedAt: time.Now(),
	}
	return memberId
}

func (cg *ConsumerGroup) RemoveMember(memberID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.members, memberID)
}

func (cg *ConsumerGroup) MemberCount() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return len(cg.members)
}

func (cg *ConsumerGroup) Rebalance(numPartitions int) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// 1. Sort member IDs
	var memberIDs []string
	for id := range cg.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	if len(memberIDs) == 0 {
		cg.assignments = make(map[string][]int)
		return
	}

	// 2. Calculate distribution
	perMember := numPartitions / len(memberIDs)
	remainder := numPartitions % len(memberIDs)

	// 3. Assign partitions
	assignments := make(map[string][]int)
	currPartition := 0

	for i, memberID := range memberIDs {
		assignCount := perMember
		if i < remainder {
			assignCount++
		}

		parts := make([]int, 0, assignCount)
		for j := 0; j < assignCount; j++ {
			parts = append(parts, currPartition)
			currPartition++
		}
		assignments[memberID] = parts
	}

	cg.assignments = assignments
}

func (cg *ConsumerGroup) GetAssignments(memberID string) []int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	if parts, ok := cg.assignments[memberID]; ok {
		// return a copy to prevent mutation
		res := make([]int, len(parts))
		copy(res, parts)
		return res
	}
	return []int{}
}

func (cg *ConsumerGroup) GetAndAdvancePartitionOffset(partID int) int64 {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	offset := cg.offsets[partID]
	cg.offsets[partID] = offset + 1
	return offset
}

func (cg *ConsumerGroup) GetPartitionOffset(partID int) int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.offsets[partID]
}
