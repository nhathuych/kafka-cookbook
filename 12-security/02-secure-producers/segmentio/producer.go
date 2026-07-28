package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
	// --- 1. Set up the SASL/SCRAM Authentication Mechanism ---
	// This is the main difference. We create a mechanism object that holds
	// our authentication details.
	mechanism, err := scram.Mechanism(scram.SHA256, "app", "supersecret")
	if err != nil {
		log.Fatalf("failed to create scram mechanism: %v", err)
	}

	// --- 2. Configure the Writer with a Custom Dialer ---
	// The kafka.Writer needs to be told how to establish a secure connection.
	// We do this by creating a custom Dialer that uses a Transport configured
	// with our SASL mechanism.
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9097"),
		Topic: "secure-app-segmentio-topic",
		// The Transport is the key piece that handles the secure handshake.
		Transport: &kafka.Transport{
			SASL: mechanism,
		},
	}
	defer writer.Close()

	fmt.Println("Secure producer (segmentio) started. Sending message...")

	// --- 3. Send a Message ---
	// The rest of the logic is very similar to the unsecured producer.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messageValue := fmt.Sprintf("Hello from a secure Segment.io client! #%d", rand.IntN(100000))

	err = writer.WriteMessages(ctx,
		kafka.Message{
			Value: []byte(messageValue),
		},
	)

	if err != nil {
		log.Fatalf("failed to write message: %v", err)
	}

	fmt.Println("Message sent successfully!")
}
