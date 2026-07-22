# Create topic for user profile Avro events
# Source topic for Avro Serialization with Schema Registry Pattern (Blueprint Library)
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic user-profile-avro-events \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
