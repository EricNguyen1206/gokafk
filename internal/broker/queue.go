package broker

import "fmt"

const (
	MAX_MSG_SIZE   = 255
	QUEUE_CAPACITY = 10000
)

var underArr = make([]byte, MAX_MSG_SIZE*QUEUE_CAPACITY)
var underSize = make([]byte, MAX_MSG_SIZE*QUEUE_CAPACITY)

type Queue struct {
	head uint32
	tail uint32
}

func (q *Queue) init() {
	q.head = 0
	q.tail = 0
}

func (q *Queue) push(data []byte) {
	copy(underArr[q.tail:int(q.tail)+len(data)], data)
	underSize[q.tail] = byte(len(data))
	q.tail += MAX_MSG_SIZE
	q.tail %= MAX_MSG_SIZE * QUEUE_CAPACITY
}

func (q *Queue) pop() []byte {
	data := underArr[q.head : q.head+uint32(underSize[q.head])]
	q.head += MAX_MSG_SIZE
	q.head %= MAX_MSG_SIZE * QUEUE_CAPACITY
	return data
}

func (q *Queue) debug() {
	fmt.Printf("Debug queue: \n")
	var cur = q.head
	for {
		data := underArr[cur : cur+uint32(underSize[cur])]
		fmt.Printf("%s\n", data)
		cur += MAX_MSG_SIZE
		cur %= MAX_MSG_SIZE * QUEUE_CAPACITY
		if cur == q.tail {
			break
		}
	}
}
