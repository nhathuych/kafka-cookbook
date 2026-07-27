package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	topic := "go-app-events"

	// --- 1. Create a new consumer instance ---
	// The ConfigMap is similar to the producer, but with two crucial additions:
	// "group.id" identifies this consumer as part of a team.
	// "auto.offset.reset" tells it where to start reading if it's a new group.
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9094,localhost:9095,localhost:9096",
		"group.id":          "go-app-readers",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer c.Close()

	log.Println("Consumer created successfully.")

	// --- 2. Subscribe to the topic ---
	// This tells the consumer which "magazine" it's interested in.
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(err)
	}

	log.Printf("Subscribed to topic: %s\n", topic)

	// --- Graceful Shutdown Setup ---
	// This allows us to stop the consumer cleanly with Ctrl+C
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// --- 3. The Poll Loop ---
	// This is the heart of the consumer. It's an infinite loop that
	// continuously asks Kafka for new messages.
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			// The ReadMessage call will block for up to the specified timeout
			// waiting for a new message.
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// This is the most common error: a timeout. It's not a real
				// problem, it just means no new messages arrived in the last 100ms.
				// We just continue our loop and try again.
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				fmt.Printf("Consumer error: %v (%v)\n", err, ev)
				continue
			}

			// We received a message!
			fmt.Printf("Received message on %s: Key: %s | Value: %s\n",
				ev.TopicPartition, string(ev.Key), string(ev.Value))
		}
	}
	log.Println("Closing consumer.")
}
