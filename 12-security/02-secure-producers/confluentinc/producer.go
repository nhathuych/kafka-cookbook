package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9097", // Pointing to the secure port
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanisms":   "SCRAM-SHA-256",
		"sasl.username":     "app", // Using the correct username
		"sasl.password":     "supersecret",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	topic := "secure-app-confluent-topic"
	deliveryChan := make(chan kafka.Event)

	message := fmt.Sprintf("Hello from a secure Confluent client! #%d", rand.IntN(100000))

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Message delivered to topic %s\n", *m.TopicPartition.Topic)
	}
	close(deliveryChan)
}
