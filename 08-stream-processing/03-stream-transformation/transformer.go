package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Input Event represents the structure of the raw incoming messages
type InputEvent struct {
	Type    string `json:"type"`
	UserID  string `json:"user_id"`
	Payload string `json:"payload"`
}

// Output Event represents the structure of the clean transformed outgoing messages
type OutputEvent struct {
	UserID       string `json:"user_id"`
	Action       string `json:"action"`
	Timestamp    int64  `json:"timestamp"`
	OriginalData string `json:"original_data"`
}

func main() {
	ctx := context.Background()
	rawTopic := "raw-user-events"
	processedTopic := "processed-user-events"

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   rawTopic,
		GroupID: "user-event-transformer-group",
	})
	defer consumer.Close()

	// Producer setup (Transformer)
	producer := kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
	}
	defer producer.Close()

	log.Println("Starting stream transformer...")

	for {
		// Read the raw message
		inMsg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message")
			break
		}

		// Transformation 1: Peek/ForEach (The Inspector)
		// Log every received message for auditing/logging/debugging
		log.Printf("PEEK: Received raw message with key=%s, value=%s\n", string(inMsg.Key), string(inMsg.Value))

		// Transformation 2: Filter (The Bouncer)
		var inputEvent InputEvent
		err = json.Unmarshal(inMsg.Value, &inputEvent)
		if err != nil {
			log.Printf("FILTER: Discarding malformed JSON message: %s. Error: %v\n", string(inMsg.Value), err)
			continue
		}

		if inputEvent.Type != "login" {
			log.Println("FILTER: Discarding message of type:", inputEvent.Type)
			continue
		}

		// Transformation 3: Map (The Translator)
		outputEvent := OutputEvent{
			UserID:       inputEvent.UserID,
			Action:       strings.ToUpper(inputEvent.Type),
			Timestamp:    time.Now().Unix(),
			OriginalData: inputEvent.Payload,
		}

		outValue, err := json.Marshal(outputEvent)
		if err != nil {
			log.Println("Error marshaling output event:", err)
			continue
		}

		outMsg := kafka.Message{
			Topic: processedTopic,
			Key:   []byte(outputEvent.UserID),
			Value: outValue,
		}

		// Produce the transformed message
		if err = producer.WriteMessages(ctx, outMsg); err != nil {
			log.Println("Failed to write transformed message:", err)
		} else {
			log.Println("MAP: Successfully produce transformed message for key:", string(outMsg.Key))
		}
	}
}
