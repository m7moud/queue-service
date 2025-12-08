package worker

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/m7moud/queue-service/internal/queue"

	"github.com/sirupsen/logrus"
)

// FileWriterConfig holds configuration for the file writer
type FileWriterConfig struct {
	OutputFile    string
	BatchSize     int
	FlushInterval time.Duration
	AppendMode    bool
}

// FileWriterWorker reads from queue and writes to file
type FileWriterWorker struct {
	config  FileWriterConfig
	queue   queue.QueueService
	logger  *logrus.Logger
	written int64
	running atomic.Bool
	file    *os.File
}

// NewFileWriterWorker creates a new file writer worker
func NewFileWriterWorker(q queue.QueueService, config FileWriterConfig) (*FileWriterWorker, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Open output file
	flags := os.O_CREATE | os.O_WRONLY
	if config.AppendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(config.OutputFile, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}

	return &FileWriterWorker{
		config:  config,
		queue:   q,
		logger:  logger,
		file:    file,
		running: atomic.Bool{},
	}, nil
}

// Start begins reading from queue and writing to file
func (w *FileWriterWorker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("worker already running")
	}
	defer w.running.Store(false)
	defer w.file.Close()

	batch := make([]*queue.Message, 0, w.config.BatchSize)
	flushTicker := time.NewTicker(w.config.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("context cancelled, stopping writer")
			return w.flushBatch(batch)

		case <-flushTicker.C:
			if err := w.flushBatch(batch); err != nil {
				return fmt.Errorf("failed to flush batch: %w", err)
			}
			batch = batch[:0]

		default:
			msg, err := w.queue.Pop(ctx)
			if err != nil {
				w.logger.WithError(err).Error("failed to pop from queue")
				continue
			}

			batch = append(batch, msg)

			// Flush if batch is full
			if len(batch) >= w.config.BatchSize {
				if err := w.flushBatch(batch); err != nil {
					return fmt.Errorf("failed to flush batch: %w", err)
				}
				batch = batch[:0]
			}
		}
	}
}

func (w *FileWriterWorker) flushBatch(batch []*queue.Message) error {
	if len(batch) == 0 {
		return nil
	}

	for _, msg := range batch {
		line := msg.Content + "\n"
		if _, err := w.file.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}

		atomic.AddInt64(&w.written, 1)
		w.logger.WithFields(logrus.Fields{
			"id":     msg.ID,
			"line":   msg.LineNum,
			"length": len(msg.Content),
		}).Debug("wrote message to file")
	}

	// Flush to disk
	if err := w.file.Sync(); err != nil {
		w.logger.WithError(err).Warn("failed to sync file to disk")
	}

	w.logger.WithFields(logrus.Fields{
		"batch_size": len(batch),
		"total":      atomic.LoadInt64(&w.written),
	}).Info("flushed batch to file")

	return nil
}

// GetStats returns worker statistics
func (w *FileWriterWorker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"written":        atomic.LoadInt64(&w.written),
		"output_file":    w.config.OutputFile,
		"batch_size":     w.config.BatchSize,
		"flush_interval": w.config.FlushInterval.String(),
		"append_mode":    w.config.AppendMode,
		"is_running":     w.running.Load(),
	}
}

// Close closes the worker and its resources
func (w *FileWriterWorker) Close() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	if w.queue != nil {
		return w.queue.Close()
	}
	return nil
}

// IsRunning returns true if the worker is running
func (w *FileWriterWorker) IsRunning() bool {
	return w.running.Load()
}
