# Create a new Kafka topic with a replication factor of 3 across the cluster
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic photo-processing-queue \
  --partitions 4 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Start a Kafka consumer in the photo-processors-group consumer group and read all messages from the beginning of the topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic photo-processing-queue \
  --group photo-processors-group

# Start a Kafka producer on broker kafka-3 to publish messages to the topic
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-producer.sh \
  --topic photo-processing-queue \
  --bootstrap-server kafka-3:29092 \
  --property "parse.key=true" \
  --property "key.separator=:"
