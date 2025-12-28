package queue

import "fmt"

type Node struct {
	next  *Node
	value *Message
}

type Fifo struct {
	head  *Node
	tail  *Node
	items uint
}

func New() *Fifo {
	return &Fifo{}
}

// 	// Ack confirms the processing of a message

func (f *Fifo) Enqueue(msg *Message) error {
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

func (f *Fifo) Dequeue() (*Message, error) {
	if f.items == 0 {
		return nil, fmt.Errorf("empty queue")
	}
	n := f.head
	f.head = f.head.next
	if f.head == nil {
		f.tail = nil
	}
	f.items -= 1
	return n.value, nil
}

func (f *Fifo) Peek() (*Message, error) {
	if f.items == 0 {
		return nil, fmt.Errorf("empty queue")
	}
	return f.head.value, nil
}

func (f *Fifo) Size() int {
	return int(f.items)
}

func (f *Fifo) Ack(msgID string) error {
	return nil
}

func (f *Fifo) Close() error {
	return nil
}
