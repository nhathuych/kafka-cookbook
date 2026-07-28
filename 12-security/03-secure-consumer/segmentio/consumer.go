package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
	// --- 1. Set up the SASL/SCRAM Authentication Mechanism ---
	mechanism, err := scram.Mechanism(scram.SHA256, "app", "supersecret")
	if err != nil {
		log.Fatalf("failed to create scram mechanism: %v", err)
	}

	// --- 2. Configure the Reader with a Custom Dialer ---
	// The Dialer is what establishes the connection to the brokers.
	// We provide it with our SASL mechanism to handle authentication.
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9097"},
		Topic:   "secure-app-segmentio-topic",
		GroupID: "secure-consumer-segmentio-group",
		Dialer:  dialer, // Use our custom, secure dialer
	})
	defer reader.Close()

	log.Println("Secure consumer (segmentio) started. Waiting for messages...")

	ctx := context.Background()
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("could not read message: %v", err)
			break
		}

		fmt.Printf("Received message: Key: %s | Value: %s\n",
			string(message.Key), string(message.Value))
	}
}
