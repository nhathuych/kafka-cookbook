# Create topic for incoming raw orders
# Source topic for Resilient Processor with Retry and DLQ Pattern
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic incoming-orders \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Create topic for Dead-Letter Queue
# Output topic for Resilient Processor / DLQ Pattern
# Contains poison pill messages that failed after max retries (messages containing "fail")
# Stores original failed message plus header 'error-reason' for debugging
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic incoming-orders-dlq \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Output topic for Resilient Processor / DLQ Pattern
# Contains messages routed to DLQ after 3 retry attempts with 2s backoff
# Output Consumer: Consumes and displays DLQ messages with error-reason header in real-time
# Reads failed records from 'incoming-orders-dlq' topic from the beginning for monitoring
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic incoming-orders-dlq \
  --from-beginning \
  --bootstrap-server kafka-1:29092

# Source topic for Resilient Processor (acts as input stream with validation)
# Raw order events are consumed by 'resilient-processor-group' (Go consumer)
# Messages containing "fail" simulate poison pill and trigger retry loop -> DLQ routing
# Input Producer: Produces test orders (both valid and poison pill messages) for resilient processing
# Sends records to 'incoming-orders' topic for resilient-processor-group
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic incoming-orders \
  --bootstrap-server kafka-1:29092
