package queue

import "github.com/GiorgosMarga/ibmmq/internal/message"

type Node struct {
	next  *Node
	value *message.Message
}

type Fifo struct {
	head  *Node
	tail  *Node
	items uint
}

func NewFifo() *Fifo {
	return &Fifo{}
}

func (f *Fifo) Enqueue(msg *message.Message) error {
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

func (f *Fifo) Close() error {
	return nil
}
