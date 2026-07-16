package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	requestTopic = "orders"
	dlqTopic     = "orders-dlq"
	brokerURL    = "localhost:9092"
)

type OrderMSG struct {
	OrderID string
	Items   int
}

func main() {
	writer := kafka.Writer{
		Addr:  kafka.TCP(brokerURL),
		Topic: requestTopic,
	}
	defer writer.Close()

	messages := []OrderMSG{
		{"order-001", 3},
		{"order-002", 2},
		{"FAIL-ME-ORDER", 1}, // This will be the "bad message"
		{"order-001", 4},
		{"order-001", 5},
	}

	ctx := context.Background()
	for _, msg := range messages {
		payload, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(msg.OrderID),
			Value: payload,
		})
		if err != nil {
			panic(err)
		}

		log.Println("Produced message:", msg.OrderID)
		time.Sleep(time.Second)
	}
	log.Println("Finished producing messages")
}
