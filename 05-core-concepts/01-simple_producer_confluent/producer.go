package main

import (
	"fmt"
	"math/rand/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Create a new producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9095",
		// Idempotence config
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"retries":                               10,
		"max.in.flight.requests.per.connection": 5,
	})

	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Topic to produce messages to
	topic := "user-profiles-conf"
	message := fmt.Sprintf("Hello from Go #%d", rand.IntN(100000))

	// Asynchronously produce a message
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, nil)

	if err != nil {
		panic(err)
	}

	fmt.Println("Message sent successfully!")
}
