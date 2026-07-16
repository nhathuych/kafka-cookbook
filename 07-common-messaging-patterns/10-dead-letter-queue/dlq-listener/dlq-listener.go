package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	dlqTopic   = "orders-dlq"
	dlqGroupID = "dlq-handler-group"
	brokerURL  = "localhost:9092"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerURL},
		Topic:   dlqTopic,
		GroupID: dlqGroupID,
	})
	defer reader.Close()

	log.Println("Starting DLQ monitor for topic:", dlqTopic)

	ctx := context.Background()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			panic(err)
		}

		log.Printf("DLQ received [%s] offset %d: %s\n", string(msg.Key), msg.Offset, string(msg.Value))
		reader.CommitMessages(ctx, msg)
	}
}
