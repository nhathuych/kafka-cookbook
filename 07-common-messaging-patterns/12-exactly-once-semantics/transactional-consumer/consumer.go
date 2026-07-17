package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	topic := "transactions-demo"
	consumerGroup := "transactions-demo-group"

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), // Only fetch messages from successfully committed transactions
		// kgo.RequireStableFetchOffsets(),              // Enabled by default
	)
	if err != nil {
		log.Fatal("Error creating Kafka consumer:", err)
	}

	defer consumer.Close()

	fmt.Println("Transaction-safe consumer started...")

	for {
		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			if ftp.Err != nil {
				fmt.Println("Error in partition", ftp.Err)
				return
			}

			for _, rec := range ftp.Records {
				fmt.Printf("Committed message with key=%s value=%s offset=%d\n", string(rec.Key), string(rec.Value), rec.Offset)
			}
		})

		time.Sleep(500 * time.Millisecond)
	}
}
