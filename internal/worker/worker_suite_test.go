package worker_test

import (
	"context"
	"testing"

	"github.com/m7moud/queue-service/internal/queue"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker Suite")
}

// Mocking QueueClient for testing purposes
type MockQueueClient struct {
	pushBatchCount int
	messages       []*queue.Message
}

func (m *MockQueueClient) Push(ctx context.Context, msg *queue.Message) error {
	m.messages = append(m.messages, msg)
	return nil
}

func (m *MockQueueClient) Pop(ctx context.Context) (*queue.Message, error) {
	if len(m.messages) == 0 {
		return nil, nil
	}
	msg := m.messages[0]
	m.messages = m.messages[1:]
	return msg, nil
}

func (m *MockQueueClient) Close() error {
	return nil
}
