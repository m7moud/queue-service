package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/m7moud/queue-service/internal/config"
	"github.com/m7moud/queue-service/internal/queue"
	"github.com/m7moud/queue-service/internal/worker"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

func main() {
	addr := ":8080"

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.Info("Starting reader/writer system...")

	queueService, err := queue.NewQueueClient(addr)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to queue service")
	}

	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	// Create workers
	reader, err := worker.NewFileReaderWorker(queueService, cfg.ReaderConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create reader worker")
	}
	defer reader.Close()

	writer, err := worker.NewFileWriterWorker(queueService, cfg.WriterConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create writer worker")
	}
	defer writer.Close()

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := reader.Start(ctx); err != nil && err != context.Canceled {
			errChan <- errors.Wrap(err, "reader worker error")
		}
	}()

	go func() {
		defer wg.Done()
		if err := writer.Start(ctx); err != nil && err != context.Canceled {
			errChan <- errors.Wrap(err, "writer worker error")
		}
	}()

	// Wait for signal or error
	select {
	case sig := <-sigChan:
		logger.WithField("signal", sig).Info("Received signal, shutting down...")
		cancel()
	case err := <-errChan:
		logger.WithError(err).Error("Worker failed, shutting down...")
		cancel()
	}

	// Wait for graceful shutdown
	wg.Wait()

	logger.Info("System shutdown completed")
}
