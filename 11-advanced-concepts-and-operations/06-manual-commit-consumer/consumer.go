package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9094", "localhost:9095", "localhost:9096"},
		Topic:          "go-app-events",
		GroupID:        "go-app-readers-manual-commit",
		CommitInterval: 0, // 👈 disable auto-commit
	})

	defer r.Close()

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Fetch error: %v", err)
			continue
		}

		fmt.Printf("Processing message: %s = %s\n", string(m.Key), string(m.Value))

		// 🎯 simulate business logic
		time.Sleep(2 * time.Second)
		businessLogicSucceded := true

		if businessLogicSucceded {
			// 🎯 commit manually after processing
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Commit failed: %v", err)
			} else {
				fmt.Println("🎯 Offset committed")
			}
		} else {
			log.Println("Business logic failed")
		}
	}
}
