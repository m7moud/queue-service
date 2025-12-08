package queue

import (
	"context"
	"time"
)

// Message represents a single message in the queue
type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	LineNum   int       `json:"line_num"`
	Timestamp time.Time `json:"timestamp"`
}

// QueueService defines the interface for queue operations
type QueueService interface {
	Push(ctx context.Context, msg *Message) error
	Pop(ctx context.Context) (*Message, error)
	Close() error
}
