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

	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9094"),
		Topic: topic,
		// Idempotence config
		RequiredAcks: kafka.RequireAll, // Enables idempotent writes
		MaxAttempts:  10,
	}

	defer func() {
		err := writer.Close()
		if err != nil {
			log.Fatal("failed to close writer", err)
		}
	}()

	fmt.Println("Producer started. Sending messages to topic: ", topic)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		message := fmt.Sprintf("Message #%d for user", i)
		err := writer.WriteMessages(ctx, kafka.Message{
			Value: []byte(message),
		})
		if err != nil {
			log.Fatalf("failed to write message %d: %v", i, err)
		}

		fmt.Printf("Sent message: %s\n", message)

		time.Sleep(500 * time.Microsecond)
	}

	fmt.Println("Finished sending messages!")
}
