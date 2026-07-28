package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// --- 1. Configure the Transactional Producer ---
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		// This is the mandatory ID for transactional producers.
		"transactional.id": "my-interactive-transactional-producer",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// Initialize the producer for transactions. This must be called once.
	// We use a longer context here as it's a one-time setup.
	initCtx, cancelInit := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelInit()
	err = p.InitTransactions(initCtx)
	if err != nil {
		log.Fatalf("Failed to initialize transactions: %s", err)
	}

	// --- User Instructions ---
	fmt.Println("🚀 Kafka Transactional Producer Started")
	fmt.Println("Enter an Order ID and a Payment Amount to process a transaction.")
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("To trigger a FAILED transaction (rollback), do one of the following:")
	fmt.Println("  - Enter 'fail-me' as the Order ID.")
	fmt.Println("  - Enter a payment amount that is zero or less (e.g., 0, -10).")
	fmt.Println("Any other combination will result in a SUCCESSFUL transaction (commit).")
	fmt.Println("Press Ctrl+C to exit.")
	fmt.Println("-----------------------------------------------------------------")

	reader := bufio.NewReader(os.Stdin)
	orderTopic := "order-events-txn"
	paymentTopic := "payment-events-txn"
	deliveryChan := make(chan kafka.Event, 2)

	// --- 2. Start the Infinite Processing Loop ---
	for {
		// Create a context with a timeout for each transaction.
		txCtx, cancelTx := context.WithTimeout(context.Background(), 30*time.Second)

		// Begin a new transaction for this iteration.
		err = p.BeginTransaction()
		if err != nil {
			log.Printf("Error beginning transaction: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			cancelTx() // Clean up the context for this failed iteration
			continue
		}

		// --- Get User Input ---
		fmt.Print("Enter Order ID: ")
		orderID, _ := reader.ReadString('\n')
		orderID = strings.TrimSpace(orderID)

		fmt.Print("Enter Payment Amount: ")
		amountStr, _ := reader.ReadString('\n')
		amountStr = strings.TrimSpace(amountStr)
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			log.Println("Invalid amount entered. Aborting transaction.")
			p.AbortTransaction(txCtx)
			cancelTx()
			continue
		}

		// --- 3. Business Logic: Check all failure conditions first ---
		if orderID == "fail-me" || amount <= 0 {
			// --- 4a. Abort Path ---
			log.Printf("FAILURE CONDITION MET (OrderID: '%s', Amount: %.2f). Aborting transaction...", orderID, amount)

			// NOTE: We don't produce any messages in this case before aborting.
			// If we had, they would be rolled back.
			err = p.AbortTransaction(txCtx)
			if err != nil {
				log.Fatalf("FATAL: Failed to abort transaction: %s", err)
			}
			log.Println("Transaction aborted successfully. No messages were sent.")
			fmt.Println("-----------------------------------------------------------------")
		} else {
			// --- 4b. Success Path ---
			log.Printf("SUCCESS CONDITIONS MET (OrderID: '%s', Amount: %.2f). Processing...", orderID, amount)

			// Produce order message
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &orderTopic, Partition: kafka.PartitionAny},
				Value:          fmt.Appendf(nil, "Order '%s' created", orderID),
			}, deliveryChan)

			// Produce payment message
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &paymentTopic, Partition: kafka.PartitionAny},
				Value:          fmt.Appendf(nil, "Payment of $%.2f processed for Order '%s'", amount, orderID),
			}, deliveryChan)

			// Wait for both delivery reports
			wasSuccessful := true
			for i := 0; i < 2; i++ {
				e := <-deliveryChan
				m := e.(*kafka.Message)

				if m.TopicPartition.Error != nil {
					log.Printf("Delivery to broker failed: %v. Aborting transaction.", m.TopicPartition.Error)
					p.AbortTransaction(txCtx)
					wasSuccessful = false
					break // Exit the delivery report loop
				}
				log.Printf("Message successfully produced to topic %s [partition %d]", *m.TopicPartition.Topic, m.TopicPartition.Partition)
			}

			// Only commit if both messages were successfully produced to the broker's buffer.
			if wasSuccessful {
				log.Println("Both messages produced to internal buffer. Committing transaction...")
				err = p.CommitTransaction(txCtx)
				if err != nil {
					log.Fatalf("FATAL: Failed to commit transaction: %s", err)
				}
				log.Println("Transaction committed successfully! Messages are now visible to consumers.")
				fmt.Println("-----------------------------------------------------------------")
			}
		}
		// Clean up the context for this iteration before the next loop.
		cancelTx()
	}
}
