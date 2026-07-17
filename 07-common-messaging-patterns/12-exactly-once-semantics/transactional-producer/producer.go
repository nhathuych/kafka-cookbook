package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	topic := "transactions-demo"
	producerID := strconv.FormatInt(int64(os.Getpid()), 10)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.TransactionalID(producerID),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalln("Error creating Kafka client:", err)
	}

	defer client.Close()

	// Begin Transaction
	if err = client.BeginTransaction(); err != nil {
		log.Fatalln("Couldn't start transaction:", err)
	}

	fmt.Println("Transaction started!")

	messages := []string{"Order Placed", "Payment Success", "fail now", "Inventory Updated"}
	// messages := []string{"Order Placed", "Payment Success", "Inventory Updated"}

	for _, msg := range messages {
		// Simulate failure
		if msg == "fail now" {
			fmt.Println("Rolling back transaction...")

			rollback(ctx, client)
			os.Exit(1)
		}

		record := kgo.StringRecord(msg)

		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			log.Println("Failed to send message, aborting transaction...")

			rollback(ctx, client)
			return
		}

		fmt.Println("Message sent:", msg)
		time.Sleep(time.Second)
	}

	if err = client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		log.Fatal("Commit failed:", err)
	}

	fmt.Println("Transaction committed successfully")
}

func rollback(ctx context.Context, client *kgo.Client) {
	// This only happens if ctx is canceled
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Println("error aborting buffered records:", err)
		return
	}

	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Println("error rolling back transaction:", err)
		return
	}

	fmt.Println("Transaction rolled back")
}
