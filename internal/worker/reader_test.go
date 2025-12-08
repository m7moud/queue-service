package worker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/m7moud/queue-service/internal/queue"
	"github.com/m7moud/queue-service/internal/worker"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileReaderWorker", func() {
	var (
		workerInstance *worker.FileReaderWorker
		queueClient    *queue.QueueClient
		inputFile      string
		config         worker.FileReaderConfig
		ctx            context.Context
		cancel         context.CancelFunc
	)

	BeforeEach(func() {
		// Setup a temporary file for the worker to read from
		inputFile = "test_file.txt"
		content := "line 1\nline 2\nline 3\nline 4\nline 5\n"
		err := os.WriteFile(inputFile, []byte(content), 0o644)
		Expect(err).NotTo(HaveOccurred())

		ln, err := net.Listen("tcp", ":0") // OS pick an available port
		Expect(err).NotTo(HaveOccurred())

		addr := fmt.Sprintf("localhost:%d", ln.Addr().(*net.TCPAddr).Port)

		server := queue.NewQueueServer(addr)

		// Start the server in a goroutine so it runs asynchronously
		go func() {
			err = server.Start()
			Expect(err).NotTo(HaveOccurred())
		}()

		// Mock the QueueClient (this could be a stub or a mock in a real test)
		queueClient, err = queue.NewQueueClient(addr)
		Expect(err).NotTo(HaveOccurred())

		// Create a new FileReaderWorker with test config
		config = worker.FileReaderConfig{
			InputFile:  inputFile,
			BatchSize:  2, // Test with a batch size of 2
			BufferSize: 1024,
		}

		workerInstance, err = worker.NewFileReaderWorker(queueClient, config)
		Expect(err).NotTo(HaveOccurred())

		// Create a cancellable context for stopping the worker
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		// Clean up the temporary file after each test
		err := os.Remove(inputFile)
		Expect(err).NotTo(HaveOccurred())
		cancel() // Cancel the context
	})

	Describe("Start", func() {
		It("should process all lines from the file", func() {
			// Start the worker in a separate goroutine to simulate async work
			go func() {
				err := workerInstance.Start(ctx)
				Expect(err).NotTo(HaveOccurred())
			}()

			// Give it a bit of time to process the file
			time.Sleep(2 * time.Second)

			// Check if the worker has processed the expected number of messages
			stats := workerInstance.GetStats()
			processed := stats["processed"].(int64)
			Expect(processed).To(Equal(int64(5))) // Expecting 5 lines processed

			// You can also verify logs, but for simplicity, we're focusing on functionality.
		})

		It("should handle context cancellation gracefully", func() {
			// Start the worker in a separate goroutine
			go func() {
				err := workerInstance.Start(ctx)
				Expect(err).NotTo(HaveOccurred())
			}()

			// Sleep for a while and cancel the context to simulate worker stop
			time.Sleep(1 * time.Second)
			cancel()

			// Verify that the worker was stopped gracefully
			Expect(workerInstance.IsRunning()).To(BeFalse())
		})
	})

	Describe("GetStats", func() {
		It("should return correct stats", func() {
			// Start the worker in a separate goroutine
			go func() {
				err := workerInstance.Start(ctx)
				Expect(err).NotTo(HaveOccurred())
			}()

			// Sleep while the worker processes the file
			time.Sleep(2 * time.Second)

			// Check if the stats show the correct processed count
			stats := workerInstance.GetStats()
			processed := stats["processed"].(int64)
			Expect(processed).To(Equal(int64(5))) // Expecting 5 lines processed
		})
	})
})
