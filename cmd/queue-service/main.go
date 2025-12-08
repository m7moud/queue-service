package main

import (
	"log"
	"os"

	"github.com/m7moud/queue-service/internal/queue"
)

func main() {
	addr := ":8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	server := queue.NewQueueServer(addr)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start queue server: %v", err)
	}
}
