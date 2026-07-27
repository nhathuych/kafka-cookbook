package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	topic := "go-app-events"

	// --- 1. Create a new producer instance ---
	// The ConfigMap contains the producer's configuration. The most important
	// setting is "bootstrap.servers", which points to our Kafka cluster.
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9094,localhost:9095,localhost:9096",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	log.Println("Producer created successfully.")

	// --- 2. Set up a goroutine for handling delivery reports ---
	// We create a separate goroutine to read from the delivery channel.
	// This is the "asynchronous" part. Our main function can continue sending
	// messages without blocking, while this goroutine handles the confirmations.
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message was successfully delivered
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %s\n", ev.TopicPartition)
				}
			case kafka.Error:
				// A general producer error
				fmt.Printf("Producer error: %v\n", ev)
			}
		}
	}()

	// --- 3. Produce messages ---
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := fmt.Sprintf("Hello from Go! Message #%d", i)

		// The Produce call sends the message. It's non-blocking.
		// It places the message in a buffer and returns immediately.
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
		}, nil) // We use nil for the delivery channel because we are handling events globally.

		if err != nil {
			log.Printf("Failed to produce message: %v\n", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// --- 4. Wait for all messages to be delivered ---
	// The Flush call will block until all buffered messages are sent and their
	// delivery reports have been received.
	outstandingMessages := p.Flush(15 * 1000)
	if outstandingMessages > 0 {
		fmt.Printf("\nWARNING: %d outstanding messages were not delivered.\n", outstandingMessages)
	}

	fmt.Println("\nAll messages sent.")
}
