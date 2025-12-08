package queue

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

type QueueClient struct {
	conn   net.Conn
	writer *bufio.Writer
	reader *bufio.Reader
}

func NewQueueClient(addr string) (*QueueClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &QueueClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

func (qc *QueueClient) Push(ctx context.Context, msg *Message) error {
	// Send PUSH command
	if _, err := qc.writer.WriteString("PUSH\n"); err != nil {
		return err
	}

	// Send message
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if _, err := qc.writer.WriteString(string(data) + "\n"); err != nil {
		return err
	}

	if err := qc.writer.Flush(); err != nil {
		return err
	}

	// Read response
	response, err := qc.reader.ReadString('\n')
	if err != nil {
		return err
	}

	if response[:len(response)-1] != "OK" {
		return fmt.Errorf("server error: %s", response)
	}

	return nil
}

func (qc *QueueClient) Pop(ctx context.Context) (*Message, error) {
	// Send POP command
	if _, err := qc.writer.WriteString("POP\n"); err != nil {
		return nil, err
	}

	if err := qc.writer.Flush(); err != nil {
		return nil, err
	}

	// Read response
	response, err := qc.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	response = strings.TrimSpace(response)
	if response == "EMPTY" || response == "OK" {
		return nil, nil // Queue is empty
	}

	fmt.Println("response", response)
	var msg Message
	if err := json.Unmarshal([]byte(response), &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &msg, nil
}

func (qc *QueueClient) Close() error {
	return qc.conn.Close()
}
