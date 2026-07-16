package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type order struct {
	OrderID string
	Items   int
}

const (
	requestTopic = "orders"
	dlqTopic     = "orders-dlq"
	groupID      = "order-processing-group"
	brokerURL    = "localhost:9092"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerURL},
		Topic:   requestTopic,
		GroupID: groupID,
	})
	defer reader.Close()

	// Dead Letter Queue setup (within the consumer app)
	dlqWriter := kafka.Writer{
		Addr:  kafka.TCP(brokerURL),
		Topic: dlqTopic,
	}
	defer dlqWriter.Close()

	log.Println("Starting consumer for topic:", requestTopic)

	ctx := context.Background()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context canceled")
				break
			}
			log.Println("Error fetching message:", err)
			continue
		}

		log.Printf("Received message at offset %d: %s", msg.Offset, string(msg.Value))

		var processingErr error
		var order order

		err = json.Unmarshal(msg.Value, &order)
		if err != nil {
			processingErr = fmt.Errorf("malformed message: %v", err)
		} else {
			log.Printf("Processing order %s with %d items...\n", order.OrderID, order.Items)

			if order.OrderID == "FAIL-ME-ORDER" {
				processingErr = fmt.Errorf("simulated processing error for order ID: %v", order.OrderID)
			} else {
				// simulate some task done
				time.Sleep(500 * time.Millisecond)
				processingErr = nil
			}
		}

		// DLQ Handling
		if processingErr != nil {
			log.Printf("ERROR: Failed to process order %s: %v. Sending to DLQ\n", string(msg.Key), processingErr)

			dlqPayload := map[string]any{
				"original_message":   string(msg.Value),
				"error":              processingErr.Error(),
				"timestamp":          time.Now().Format(time.RFC3339),
				"original_topic":     msg.Topic,
				"original_offset":    msg.Offset,
				"original_partition": msg.Partition,
			}
			dlqValue, err := json.Marshal(dlqPayload)
			if err != nil {
				panic(err)
			}

			err = dlqWriter.WriteMessages(ctx, kafka.Message{
				Key:   msg.Key,
				Value: dlqValue,
			})
			if err != nil {
				panic(err)
			}

			log.Println("Successfully sent message to DLQ!")
		} else {
			log.Println("Successfully processed order:", string(msg.Key))
		}

		// Offset committing
		// This is crucial after successful processing or DLQ forwarding.
		// Prevents the same message from being processed again.
		if err = reader.CommitMessages(ctx, msg); err != nil {
			panic(err)
		}

		log.Println("Committed the offset:", msg.Offset)
	}
}
