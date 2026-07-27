package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// === CONFIGURATION ===
	brokers := []string{"localhost:9092"}
	topic := "go-app-events"
	groupID := "demo-consumer-group"

	// === ASK USER HOW TO START ===
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose how the consumer should start:")
	fmt.Println("NOTE: If this group has already committed offsets, option 1 will not re-read old messages unless you change the group ID. Kafka will ignore your selection and start from last stored offset")
	fmt.Println("1. From beginning of topic")
	fmt.Println("2. From last committed offset")
	fmt.Print("Enter choice (1 or 2): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	// === CONSUMER SETUP ===
	var startOffset int64 = kafka.LastOffset // default
	if choice == "1" {
		startOffset = kafka.FirstOffset
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         brokers,
		Topic:           topic,
		GroupID:         groupID,
		StartOffset:     startOffset,
		CommitInterval:  0, // manual commit
		ReadLagInterval: -1,
	})
	defer r.Close()

	fmt.Println("🚀 Consumer started...")
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("❌ Error reading message: %v", err)
		}

		fmt.Printf("📥 Message received: %s = %s (offset=%d)\n", string(msg.Key), string(msg.Value), msg.Offset)

		// Simulate processing
		time.Sleep(500 * time.Millisecond)

		// Manually commit offset
		if err := r.CommitMessages(context.Background(), msg); err != nil {
			log.Fatalf("❌ Failed to commit offset: %v", err)
		}
		fmt.Printf("🎯 Offset committed: %d\n", msg.Offset)
	}
}
