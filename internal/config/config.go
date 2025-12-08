package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/m7moud/queue-service/internal/worker"
)

// Config holds the application configuration
type Config struct {
	ReaderConfig worker.FileReaderConfig
	WriterConfig worker.FileWriterConfig
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() (*Config, error) {
	inputFile := getEnv("INPUT_FILE", "input.txt")
	outputFile := getEnv("OUTPUT_FILE", "output.txt")

	batchSize, err := strconv.Atoi(getEnv("BATCH_SIZE", "100"))
	if err != nil {
		return nil, fmt.Errorf("invalid BATCH_SIZE: %w", err)
	}

	bufferSize, err := strconv.Atoi(getEnv("BUFFER_SIZE", "65536"))
	if err != nil {
		return nil, fmt.Errorf("invalid BUFFER_SIZE: %w", err)
	}

	flushInterval, err := time.ParseDuration(getEnv("FLUSH_INTERVAL", "5s"))
	if err != nil {
		return nil, fmt.Errorf("invalid FLUSH_INTERVAL: %w", err)
	}

	appendMode := getEnvBool("APPEND_MODE", false)

	return &Config{
		ReaderConfig: worker.FileReaderConfig{
			InputFile:  inputFile,
			BatchSize:  batchSize,
			BufferSize: bufferSize,
		},
		WriterConfig: worker.FileWriterConfig{
			OutputFile:    outputFile,
			BatchSize:     batchSize,
			FlushInterval: flushInterval,
			AppendMode:    appendMode,
		},
	}, nil
}

// LoadReaderConfig loads configuration for reader only
func LoadReaderConfig() (*worker.FileReaderConfig, error) {
	cfg, err := LoadFromEnv()
	if err != nil {
		return nil, err
	}
	return &cfg.ReaderConfig, nil
}

// LoadWriterConfig loads configuration for writer only
func LoadWriterConfig() (*worker.FileWriterConfig, error) {
	cfg, err := LoadFromEnv()
	if err != nil {
		return nil, err
	}
	return &cfg.WriterConfig, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
