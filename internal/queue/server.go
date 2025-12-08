package queue

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

type QueueServer struct {
	queue *MessageQueue
	addr  string
}

func NewQueueServer(addr string) *QueueServer {
	return &QueueServer{
		queue: NewMessageQueue(),
		addr:  addr,
	}
}

func (qs *QueueServer) Start() error {
	ln, err := net.Listen("tcp", qs.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer ln.Close()

	log.Printf("Queue service listening on %s", qs.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go qs.handleConnection(conn)
	}
}

func (qs *QueueServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Read command
		cmd, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// Process command
		switch cmd[:len(cmd)-1] { // Remove newline
		case "PUSH":
			// Read message
			msg, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Failed to read message: %v", err)
				writer.WriteString("ERROR\n")
				writer.Flush()
				continue
			}

			qs.queue.Enqueue(msg[:len(msg)-1])
			writer.WriteString("OK\n")
			writer.Flush()

		case "POP":
			msg, ok := qs.queue.Dequeue()
			if !ok {
				writer.WriteString("EMPTY\n")
			} else {
				writer.WriteString(msg + "\n")
			}
			writer.Flush()

		default:
			writer.WriteString("UNKNOWN_COMMAND\n")
			writer.Flush()
		}
	}
}
