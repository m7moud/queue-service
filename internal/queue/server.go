package queue

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/sirupsen/logrus"
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

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.WithFields(logrus.Fields{
		"address": qs.addr,
	}).Info("Queue service listening")

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
			qs.handlePush(reader, writer)
		case "POP":
			qs.handlePop(writer)
		default:
			writer.WriteString("UNKNOWN_COMMAND\n")
			writer.Flush()
		}
	}
}

func (qs *QueueServer) handlePush(reader *bufio.Reader, writer *bufio.Writer) {
	// Read message
	msg, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read message: %v", err)
		writer.WriteString("ERROR\n")
		writer.Flush()
		return
	}

	qs.queue.Enqueue(msg[:len(msg)-1])
	writer.WriteString("OK\n")
	writer.Flush()
}

func (qs *QueueServer) handlePop(writer *bufio.Writer) {
	msg, ok := qs.queue.Dequeue()
	if !ok {
		writer.WriteString("EMPTY\n")
	} else {
		writer.WriteString(msg + "\n")
	}
	writer.Flush()
}
