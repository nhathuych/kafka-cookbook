# Create a topic with 3 partitions for testing
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092

# Console consumer to verify messages with full metadata
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true

# Producer WITHOUT key - RoundRobin demo - Figure 01
# Distributes messages evenly across partitions when no key is provided
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic order-events \
  --bootstrap-server kafka-1:29092 \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner

# Producer WITH key - Key-based partitioning demo - Figure 02
# Ensures all events from the same user go to the same partition, preserving per-user order
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic order-events \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --property "parse.key=true" \
  --property "key.separator=:"
