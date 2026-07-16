# Main topic: producers publish order events for asynchronous processing by consumers.
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic orders \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092

# Dead-letter topic: stores messages that could not be processed successfully for later analysis or retry.
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic orders-dlq \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092
