package queue_test

import (
	"bufio"
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/m7moud/queue-service/internal/queue"
)

var _ = Describe("QueueServer", func() {
	var (
		server *queue.QueueServer
		addr   string
		conn   net.Conn
		reader *bufio.Reader
		writer *bufio.Writer
	)

	// Setup and teardown for the tests
	BeforeEach(func() {
		// Prepare a new server instance before each test
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

		// Establish a new connection to the server
		conn, err = net.Dial("tcp", addr)
		Expect(err).NotTo(HaveOccurred())

		// Set up readers and writers for the connection
		reader = bufio.NewReader(conn)
		writer = bufio.NewWriter(conn)
	})

	AfterEach(func() {
		// Close the connection after each test
		if conn != nil {
			conn.Close()
		}
	})

	Describe("handling message", func() {
		// Test for the PUSH command
		When("sending a PUSH command", func() {
			It("should add a message to the queue and return OK", func() {
				// Send PUSH command with a message
				writer.WriteString("PUSH\n")
				writer.WriteString("message1\n")
				writer.Flush()

				// Read the response (should be "OK")
				response, err := reader.ReadString('\n')
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response).To(Equal("OK\n"))
			})
		})

		// Test for the POP command
		When("sending a POP command", func() {
			It("should return the message from the queue", func() {
				// First, push a message to the queue
				writer.WriteString("PUSH\n")
				writer.WriteString("message1\n")
				writer.Flush()

				// read that response is OK
				response, err := reader.ReadString('\n')
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response).To(Equal("OK\n"))

				// Then send the POP command
				writer.WriteString("POP\n")
				writer.Flush()

				// Read the response (should be "message1")
				popResponse, err := reader.ReadString('\n')
				Expect(err).ShouldNot(HaveOccurred())
				Expect(popResponse).To(Equal("message1\n"))
			})

			It("should return EMPTY when the queue is empty", func() {
				// Send POP command on an empty queue
				writer.WriteString("POP\n")
				writer.Flush()

				// Read the response (should be "EMPTY")
				emptyResponse, err := reader.ReadString('\n')
				Expect(err).ShouldNot(HaveOccurred())
				Expect(emptyResponse).To(Equal("EMPTY\n"))
			})
		})

		When("sending an unknown command", func() {
			It("should return UNKNOWN_COMMAND", func() {
				// Send an unknown command
				writer.WriteString("XXX\n")
				writer.Flush()

				// Read the response (should be "UNKNOWN_COMMAND")
				unknownResponse, err := reader.ReadString('\n')
				Expect(err).ShouldNot(HaveOccurred())
				Expect(unknownResponse).To(Equal("UNKNOWN_COMMAND\n"))
			})
		})
	})
})
