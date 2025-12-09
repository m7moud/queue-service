package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/m7moud/queue-service/internal/config"
	"github.com/m7moud/queue-service/internal/worker"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/sirupsen/logrus"
)

func main() {
	queueAddr := ":8080"
	if os.Getenv("QUEUE_ADDR") != "" {
		queueAddr = os.Getenv("QUEUE_ADDR")
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.Info("Starting reader/writer system...")

	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	// Create workers
	reader, err := worker.NewFileReaderWorker(queueAddr, cfg.ReaderConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create reader worker")
	}
	defer reader.Close()

	writer, err := worker.NewFileWriterWorker(queueAddr, cfg.WriterConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create writer worker")
	}
	defer writer.Close()

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := reader.Start(ctx)
		if err != nil && err != context.Canceled {
			return errors.Wrap(err, "reader worker error")
		}

		return nil
	})

	g.Go(func() error {
		err := writer.Start(ctx)
		if err != nil && err != context.Canceled {
			return errors.Wrap(err, "writer worker error")
		}

		return nil
	})

	// Wait for signal or error
	select {
	case sig := <-sigChan:
		logger.WithField("signal", sig).Info("Received signal, shutting down...")
		cancel()
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down workers...")
	}

	if err := g.Wait(); err != nil {
		logger.WithError(err).Error("Worker failed, shutting down...")
		cancel()
	} else {
		logger.Info("All workers completed successfully.")
	}

	logger.Info("System shutting down")
}
