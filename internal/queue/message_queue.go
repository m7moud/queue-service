package queue

import "sync"

type MessageQueue struct {
	messages []string
	mu       sync.RWMutex
	cond     *sync.Cond
}

func NewMessageQueue() *MessageQueue {
	mq := &MessageQueue{
		messages: make([]string, 0),
	}
	mq.cond = sync.NewCond(&mq.mu)
	return mq
}

func (mq *MessageQueue) Enqueue(msg string) {
	mq.mu.Lock()
	mq.messages = append(mq.messages, msg)
	mq.mu.Unlock()
	mq.cond.Signal()
}

func (mq *MessageQueue) Dequeue() (string, bool) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	for len(mq.messages) == 0 {
		return "", false
	}

	msg := mq.messages[0]
	mq.messages = mq.messages[1:]
	return msg, true
}
