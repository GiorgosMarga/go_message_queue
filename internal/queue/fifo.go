package queue

import (
	"sync"

	"github.com/GiorgosMarga/ibmmq/internal/message"
)

type Node struct {
	next  *Node
	value *message.Message
}

type Fifo struct {
	head  *Node
	tail  *Node
	items uint
	mtx   *sync.Mutex
}

func NewFifo() *Fifo {
	return &Fifo{
		mtx: &sync.Mutex{},
	}
}

func (f *Fifo) Enqueue(msg *message.Message) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	n := &Node{
		value: msg,
		next:  nil,
	}
	if f.tail != nil {
		f.tail.next = n
		f.tail = n
		f.items++
		return nil
	}
	f.head = n
	f.tail = n
	f.items++
	return nil
}

func (f *Fifo) Dequeue() (*message.Message, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	if f.items == 0 {
		return nil, ErrEmptyQueue
	}
	n := f.head
	f.head = f.head.next
	if f.head == nil {
		f.tail = nil
	}
	f.items -= 1
	return n.value, nil
}

func (f *Fifo) Peek() (*message.Message, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	if f.items == 0 {
		return nil, ErrEmptyQueue
	}
	return f.head.value, nil
}

func (f *Fifo) Size() int {
	return int(f.items)
}

func (f *Fifo) Ack(msgID int) error {
	return nil
}

func (f *Fifo) GetLiveMessages() []*message.Message {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	buf := make([]*message.Message, f.Size())
	i := 0
	for curr := f.head; curr != nil; curr = curr.next {
		buf[i] = curr.value
		i++
	}
	return buf
}

func (f *Fifo) Close() error {
	return nil
}
