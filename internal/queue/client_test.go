package queue_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/m7moud/queue-service/internal/queue"
)

var _ = Describe("QueueClient", func() {
	var (
		server *queue.QueueServer
		client *queue.QueueClient
		addr   string
		conn   net.Conn
	)

	// Setup and teardown for the tests
	BeforeEach(func() {
		ln, err := net.Listen("tcp", ":0") // OS pick an available port
		Expect(err).NotTo(HaveOccurred())

		addr = fmt.Sprintf("localhost:%d", ln.Addr().(*net.TCPAddr).Port)

		server = queue.NewQueueServer(addr)

		// Start the server in a goroutine so it runs asynchronously
		go func() {
			err = server.Start()
			Expect(err).NotTo(HaveOccurred())
		}()

		// Wait a bit for the server to start
		time.Sleep(time.Second)

		// Create a QueueClient instance
		client, err = queue.NewQueueClient(addr)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		// Close the connection after each test
		if client != nil {
			client.Close()
		}
	})

	When("creating a client with an invalid address", func() {
		It("should handle connection errors gracefully", func() {
			// Create a client with an invalid address to simulate a connection error
			_, err := queue.NewQueueClient("localhost:9999")
			Expect(err).To(HaveOccurred())
		})
	})

	// Test for Push functionality
	When("calling Push on QueueClient", func() {
		It("should successfully push a message to the queue", func() {
			// Call the Push method to add a message
			err := client.Push(context.Background(), &queue.Message{
				Content: "message1",
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify by sending a POP command and expecting the message back
			conn, err = net.Dial("tcp", addr)
			Expect(err).NotTo(HaveOccurred())
			defer conn.Close()

			writer := bufio.NewWriter(conn)
			reader := bufio.NewReader(conn)

			// Send the POP command
			writer.WriteString("POP\n")
			writer.Flush()

			// Read the response (should be "message1")
			popResponse, err := reader.ReadString('\n')
			Expect(err).NotTo(HaveOccurred())
			Expect(popResponse).To(ContainSubstring("message1"))
		})

		XIt("should return an error if the server responds with something other than OK", func() {
			// Attempt to push a message
			err := client.Push(context.Background(), &queue.Message{
				Content: "message1",
			})
			Expect(err).To(MatchError("server error: ERROR\n"))
		})
	})

	// Test for Pop functionality
	When("calling Pop on QueueClient", func() {
		It("should successfully pop a message from the queue", func() {
			// First, push a message to the queue
			err := client.Push(context.Background(), &queue.Message{
				Content: "message1",
			})
			Expect(err).NotTo(HaveOccurred())

			// Call the Pop method
			message, err := client.Pop(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(message.Content).To(Equal("message1"))
		})

		It("should return an empty string when the queue is empty", func() {
			// Call the Pop method on an empty queue
			message, err := client.Pop(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(message).To(BeNil())
		})
	})
})
