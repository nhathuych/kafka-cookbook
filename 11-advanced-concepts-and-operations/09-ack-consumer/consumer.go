package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := "localhost:9092"
	topic := "acks-demo-topic"
	groupID := "acks-demo-group"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{broker},
		Topic:           topic,
		GroupID:         groupID,
		CommitInterval:  0, // 🔥 Full manual commit control
		StartOffset:     kafka.FirstOffset,
		ReadLagInterval: -1,
	})

	fmt.Println("🚀 Kafka consumer started...")

	ctx := context.Background()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("❌ Failed to fetch message: %v", err)
		}

		// Simulate processing
		// fmt.Printf("📥 Received: %s = %s (offset=%d)\n", string(m.Key), string(m.Value), m.Offset)
		fmt.Printf("📥 Received: %s (offset=%d)\n", string(m.Value), m.Offset)

		// Fake a condition where we don't commit sometimes
		if string(m.Value) == "fail-me" {
			fmt.Println("⚠️ Simulating failure. Not committing offset.")
			continue
		}

		time.Sleep(time.Second) // simulate message/task processing

		// 🎯 Commit = acknowledgment
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatalf("❌ Failed to commit offset: %v", err)
		} else {
			fmt.Printf("🎯 Offset %d committed\n", m.Offset)
		}
	}
}
