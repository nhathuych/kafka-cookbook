package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "user-profiles-segio"
	groupID := "user-profiles-group-segio"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second, // Batch commit processed offsets to Kafka every second.
	})

	defer func() {
		err := reader.Close()
		if err != nil {
			log.Fatal("failed to close reader", err)
		}
	}()

	fmt.Println("Consumer started. Waiting for messages...")

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message:", err)
			continue
		}

		fmt.Printf(
			"Received message: partition=%d offset=%d key=%s value='%s'\n",
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
		)
		time.Sleep(500 * time.Millisecond)
	}
}
