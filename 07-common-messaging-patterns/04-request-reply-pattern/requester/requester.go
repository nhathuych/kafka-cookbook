package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	producer := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092", "localhost:9094", "localhost:9095"),
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal("failed to close producer", err)
		}
	}()

	// Prepare the Request
	requestTopic := "fraud-check-requests"
	replyTopic := "payment-service-replies"
	correlationID := fmt.Sprintf("request-%d", time.Now().UnixNano())

	requestMessage := kafka.Message{
		Topic: requestTopic,
		Value: []byte(messageValue()),
		Headers: []kafka.Header{
			{Key: "correlation-id", Value: []byte(correlationID)},
			{Key: "reply-to-topic", Value: []byte(replyTopic)},
		},
	}

	fmt.Println("Sending request with correlation ID:", correlationID)
	err := producer.WriteMessages(ctx, requestMessage)
	if err != nil {
		log.Fatal("failed to send request:", err)
	}

	// Wait for reply
	// Create a new reader to listen on the reply topic
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:   replyTopic,
		GroupID: fmt.Sprintf("payment-service-group-%s", correlationID),
	})

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal("failed to close consumer", err)
		}
	}()

	fmt.Println("Waiting for reply...")

	//Loop with a timeout for the read operation
	for {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		msg, err := consumer.ReadMessage(readCtx)
		if err != nil {
			fmt.Println("failed to receive reply:", err)
			break
		}

		// Check if the correlation ID matches
		for _, h := range msg.Headers {
			if h.Key == "correlation-id" && string(h.Value) == correlationID {
				fmt.Println("Received matching reply!\n-> Response:", string(msg.Value))
				return
			}
		}
	}
}

func messageValue() string {
	userID := rand.Intn(900) + 100
	amount := 10.0 + rand.Float64()*(1000.0-10.0)
	return fmt.Sprintf("Check transaction for user #%d, amount $%.2f", userID, amount)
}
