package worker

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/m7moud/queue-service/internal/queue"

	"github.com/sirupsen/logrus"
)

// FileReaderConfig holds configuration for the file reader
type FileReaderConfig struct {
	InputFile  string
	BatchSize  int
	BufferSize int
}

// FileReaderWorker reads from file and pushes to queue
type FileReaderWorker struct {
	config    FileReaderConfig
	queue     queue.QueueService
	logger    *logrus.Logger
	processed int64
	running   atomic.Bool
}

// NewFileReaderWorker creates a new file reader worker
func NewFileReaderWorker(addr string, config FileReaderConfig) (*FileReaderWorker, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	q, err := queue.NewQueueClient(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to queue service: %w", err)
	}

	return &FileReaderWorker{
		config:  config,
		queue:   q,
		logger:  logger,
		running: atomic.Bool{},
	}, nil
}

// Start begins reading the file and pushing to queue
func (w *FileReaderWorker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("worker already running")
	}
	defer w.running.Store(false)

	file, err := os.Open(w.config.InputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, w.config.BufferSize)
	scanner.Buffer(buf, w.config.BufferSize)

	lineNum := 0
	batch := make([]*queue.Message, 0, w.config.BatchSize)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			w.logger.Info("context cancelled, stopping reader")
			return w.flushBatch(ctx, batch)
		default:
		}

		line := scanner.Text()
		lineNum++

		msg := &queue.Message{
			ID:        fmt.Sprintf("line-%d-%d", time.Now().UnixNano(), lineNum),
			Content:   line,
			Timestamp: time.Now(),
			LineNum:   lineNum,
		}

		batch = append(batch, msg)

		// Process batch if full
		if len(batch) >= w.config.BatchSize {
			if err := w.processBatch(ctx, batch); err != nil {
				return fmt.Errorf("failed to process batch: %w", err)
			}
			batch = batch[:0]
		}

		atomic.AddInt64(&w.processed, 1)
	}

	// Process remaining lines
	if err := w.flushBatch(ctx, batch); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	w.logger.WithFields(logrus.Fields{
		"processed": atomic.LoadInt64(&w.processed),
		"file":      w.config.InputFile,
	}).Info("file reading completed")

	return nil
}

func (w *FileReaderWorker) processBatch(ctx context.Context, batch []*queue.Message) error {
	for _, msg := range batch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := w.queue.Push(ctx, msg); err != nil {
				return fmt.Errorf("failed to push message %s: %w", msg.ID, err)
			}

			w.logger.WithFields(logrus.Fields{
				"id":     msg.ID,
				"line":   msg.LineNum,
				"length": len(msg.Content),
			}).Debug("pushed message to queue")
		}
	}
	return nil
}

func (w *FileReaderWorker) flushBatch(ctx context.Context, batch []*queue.Message) error {
	if len(batch) == 0 {
		return nil
	}
	return w.processBatch(ctx, batch)
}

// GetStats returns worker statistics
func (w *FileReaderWorker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"processed":   atomic.LoadInt64(&w.processed),
		"input_file":  w.config.InputFile,
		"batch_size":  w.config.BatchSize,
		"buffer_size": w.config.BufferSize,
		"is_running":  w.running.Load(),
	}
}

// Close closes the worker and its resources
func (w *FileReaderWorker) Close() error {
	if w.queue != nil {
		return w.queue.Close()
	}
	return nil
}

// IsRunning returns true if the worker is running
func (w *FileReaderWorker) IsRunning() bool {
	return w.running.Load()
}
