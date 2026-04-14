package broker

import (
	"log/slog"
	"sync"
)

const (
	MAX_MSG_SIZE   = 255
	QUEUE_CAPACITY = 10000
)

var underArr = make([]byte, MAX_MSG_SIZE*QUEUE_CAPACITY)
var underSize = make([]byte, MAX_MSG_SIZE*QUEUE_CAPACITY)

type Queue struct {
	mu   sync.Mutex
	head uint32
	tail uint32
}

func (q *Queue) init() {
	q.head = 0
	q.tail = 0
}

func (q *Queue) push(data []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	copy(underArr[q.tail:int(q.tail)+len(data)], data)
	underSize[q.tail] = byte(len(data))
	q.tail += MAX_MSG_SIZE
	q.tail %= MAX_MSG_SIZE * QUEUE_CAPACITY
}

func (q *Queue) pop() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	data := underArr[q.head : q.head+uint32(underSize[q.head])]
	q.head += MAX_MSG_SIZE
	q.head %= MAX_MSG_SIZE * QUEUE_CAPACITY
	return data
}

func (q *Queue) debug() {
	q.mu.Lock()
	defer q.mu.Unlock()
	slog.Info("Debug queue")
	var cur = q.head
	for {
		data := underArr[cur : cur+uint32(underSize[cur])]
		slog.Info("Message", "message", data)
		cur += MAX_MSG_SIZE
		cur %= MAX_MSG_SIZE * QUEUE_CAPACITY
		if cur == q.tail {
			break
		}
	}
}
